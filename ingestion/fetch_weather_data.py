import logging
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests

BRONZE_ROOT = Path("storage/bronze/weather")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("weather")

# Track success/failure
SUCCESS_COUNT = 0
FAILURE_COUNT = 0

HEADERS = {
    "User-Agent": "GridCARE Weather Agent (contact@example.com)",
    "Accept": "application/geo+json",
}

ISO_COORDS = {
    "CAISO": (34.0522, -118.2437),   # Los Angeles
    "ISONE": (42.3601, -71.0589),     # Boston
    "NYISO": (40.7128, -74.0060),     # New York
    "SPP": (35.4676, -97.5164),       # Oklahoma City
    "MISO": (41.8781, -87.6298),      # Chicago
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
    global SUCCESS_COUNT
    
    if df.empty:
        LOG.warning(f"{iso}: no rows to write")
        return

    ts_utc = _to_utc(df["timestamp"])
    day_str = ts_utc.dt.date.min().isoformat()

    out_dir = BRONZE_ROOT / iso / day_str
    _ensure_dir(out_dir)
    out_file = out_dir / f"{iso}_weather.parquet"

    out = pd.DataFrame({
        "timestamp": _utc_naive_string(df["timestamp"]),
        "temperature": pd.to_numeric(df["temperature"], errors="coerce"),
        "wind_speed": pd.to_numeric(df["wind_speed"], errors="coerce"),
        "humidity": pd.to_numeric(df["humidity"], errors="coerce"),
    }).dropna(subset=["timestamp"])

    if out.empty:
        LOG.warning(f"{iso}: No valid data after cleaning")
        return

    out.to_parquet(out_file, index=False)
    SUCCESS_COUNT += 1
    LOG.info(f"✅ {iso}: Saved {len(out)} rows → {out_file}")

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
        rows.append({
            "timestamp": p.get("timestamp"),
            "temperature": (p.get("temperature", {}) or {}).get("value"),
            "wind_speed": (p.get("windSpeed", {}) or {}).get("value"),
            "humidity": (p.get("relativeHumidity", {}) or {}).get("value"),
        })

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
    global FAILURE_COUNT
    
    try:
        LOG.info(f"{iso}: Resolving stations near {lat},{lon}")
        stations = _station_ids_for_point(lat, lon, max_n=6)
        
        if not stations:
            FAILURE_COUNT += 1
            LOG.warning(f"❌ {iso}: No stations found near point")
            return

        dfs: List[pd.DataFrame] = []
        for sid in stations:
            try:
                LOG.info(f"{iso}: Fetching observations from {sid}")
                df = _collect_recent_observations(sid, limit=400)
                if not df.empty:
                    dfs.append(df)
            except requests.HTTPError as he:
                LOG.warning(f"{iso}: Station {sid} HTTP {he.response.status_code}")
            except Exception as e:
                LOG.warning(f"{iso}: Station {sid} error - {e}")

        if not dfs:
            FAILURE_COUNT += 1
            LOG.warning(f"❌ {iso}: All stations returned empty data")
            return

        all_obs = pd.concat(dfs, ignore_index=True).sort_values("timestamp").drop_duplicates(subset=["timestamp"])

        # Keep last 48 hours
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=48)
        all_obs = all_obs[all_obs["timestamp"] >= cutoff]

        _save_parquet(all_obs, iso)

    except requests.HTTPError as he:
        FAILURE_COUNT += 1
        LOG.error(f"❌ {iso}: HTTP error {he.response.status_code}")
    except Exception as e:
        FAILURE_COUNT += 1
        LOG.error(f"❌ {iso}: Weather ingestion error - {e}")

def main():
    LOG.info("=" * 60)
    LOG.info("Starting Weather Ingestion")
    LOG.info("=" * 60)
    
    # Ensure storage directory exists
    _ensure_dir(BRONZE_ROOT)
    
    for iso, (lat, lon) in ISO_COORDS.items():
        fetch_iso_weather(iso, lat, lon)
    
    LOG.info("=" * 60)
    LOG.info(f"Weather Ingestion Complete: {SUCCESS_COUNT} succeeded, {FAILURE_COUNT} failed")
    LOG.info("=" * 60)
    
    # Don't exit with error even if some failed (weather is supplementary)
    if SUCCESS_COUNT == 0:
        LOG.warning("⚠️  ALL weather ingestions failed! Weather data will be unavailable.")
    
    exit(0)

if __name__ == "__main__":
    main()