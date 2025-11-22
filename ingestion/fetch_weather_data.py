import logging
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests

BRONZE_ROOT = Path("storage/bronze/weather")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("weather")

HEADERS = {
    "User-Agent": "GridCARE Weather Agent (contact@example.com)",
    "Accept": "application/geo+json",
}

ISO_COORDS = {
    "CAISO": (34.0522, -118.2437),
    "ISONE": (42.3601,  -71.0589),
    "NYISO": (40.7128,  -74.0060),
    "SPP":   (35.4676,  -97.5164),
    "MISO":  (41.8781,  -87.6298),
}

# ---------- time helpers ----------
def _to_utc(series: pd.Series) -> pd.Series:
    """Return tz-aware UTC datetimes from any input."""
    return pd.to_datetime(series, errors="coerce", utc=True)

def _utc_naive_string(series: pd.Series) -> pd.Series:
    """Return tz-naive strings in UTC for Spark ingestion."""
    t_utc = _to_utc(series)
    return t_utc.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")

# ---------- io helpers ----------
def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _save_parquet(df: pd.DataFrame, iso: str) -> None:
    if df.empty:
        LOG.warning(f"{iso}: no rows to write")
        return

    ts_utc = _to_utc(df["timestamp"])
    day_str = ts_utc.dt.date.min().isoformat()

    out_dir = BRONZE_ROOT / iso / day_str
    _ensure_dir(out_dir)
    out_file = out_dir / f"{iso}_weather.parquet"

    out = pd.DataFrame(
        {
            "timestamp": _utc_naive_string(df["timestamp"]),
            "temperature": pd.to_numeric(df["temperature"], errors="coerce"),
            "wind_speed": pd.to_numeric(df["wind_speed"], errors="coerce"),
            "humidity": pd.to_numeric(df["humidity"], errors="coerce"),
        }
    ).dropna(subset=["timestamp"])

    out.to_parquet(out_file, index=False)
    LOG.info(f"{iso}: wrote {len(out)} rows -> {out_file}")

# ---------- NOAA calls ----------
def _station_ids_for_point(lat: float, lon: float, max_n: int = 6) -> List[str]:
    url = f"https://api.weather.gov/points/{lat},{lon}/stations"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    feats = r.json().get("features", [])
    ids: List[str] = []
    for f in feats[:max_n]:
        sid = f.get("properties", {}).get("stationIdentifier")
        if sid:
            ids.append(sid)
    return ids

def _collect_recent_observations(station_id: str, limit: int = 400) -> pd.DataFrame:
    url = f"https://api.weather.gov/stations/{station_id}/observations"
    r = requests.get(url, headers=HEADERS, params={"limit": limit}, timeout=30)
    r.raise_for_status()
    feats = r.json().get("features", [])
    rows: List[Dict[str, Any]] = []
    for f in feats:
        p = f.get("properties", {})
        rows.append(
            {
                "timestamp": p.get("timestamp"),
                "temperature": (p.get("temperature", {}) or {}).get("value"),
                "wind_speed": (p.get("windSpeed", {}) or {}).get("value"),
                "humidity": (p.get("relativeHumidity", {}) or {}).get("value"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["timestamp", "temperature", "wind_speed", "humidity"])

    df = pd.DataFrame(rows)
    df["timestamp"] = _to_utc(df["timestamp"])
    for c in ["temperature", "wind_speed", "humidity"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    return df

# ---------- driver ----------
def fetch_iso_weather(iso: str, lat: float, lon: float) -> None:
    try:
        LOG.info(f"{iso}: resolving stations near {lat},{lon}")
        stations = _station_ids_for_point(lat, lon, max_n=6)
        if not stations:
            LOG.warning(f"{iso}: no stations found near point")
            return

        dfs: List[pd.DataFrame] = []
        for sid in stations:
            try:
                LOG.info(f"{iso}: fetching observations from {sid}")
                df = _collect_recent_observations(sid, limit=400)
                if not df.empty:
                    dfs.append(df)
            except requests.HTTPError as he:
                LOG.warning(f"{iso}: station {sid} http {he}")
            except Exception as e:
                LOG.warning(f"{iso}: station {sid} error {e}")

        if not dfs:
            LOG.warning(f"{iso}: observations empty from all stations")
            return

        all_obs = pd.concat(dfs, ignore_index=True).sort_values("timestamp").drop_duplicates(subset=["timestamp"])

        # Keep last 48 hours without localizing an already tz-aware object
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=48)
        all_obs = all_obs[all_obs["timestamp"] >= cutoff]

        _save_parquet(all_obs, iso)

    except requests.HTTPError as he:
        LOG.error(f"{iso}: weather ingestion http error {he}")
    except Exception as e:
        LOG.error(f"{iso}: weather ingestion error {e}")

def main():
    for iso, (lat, lon) in ISO_COORDS.items():
        fetch_iso_weather(iso, lat, lon)
    LOG.info("Weather ingestion completed.")

if __name__ == "__main__":
    main()
