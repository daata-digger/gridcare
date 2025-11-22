# ingestion/fetch_grid_data.py

import io
import logging
import warnings
from pathlib import Path
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

# GridStatus is optional; import inside try so failures do not break ingestion
try:
    from gridstatus import CAISO, MISO, NYISO, SPP, ISONE
    _HAS_GRIDSTATUS = True
except Exception:
    _HAS_GRIDSTATUS = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BRONZE_ROOT = Path("storage/bronze/grid/load")


# ------------------------------
# Utilities
# ------------------------------
def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _to_spark_ts_mixed(s: pd.Series) -> pd.Series:
    """
    Convert many possible timestamp formats to UTC naive strings 'YYYY-MM-DD HH:MM:SS'.
    Use only when the timezone is either embedded in the string (eg RFC3339 with offset),
    or when the value is a pure epoch number. Do NOT use for known local times.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        dt = pd.to_datetime(s, utc=True, errors="coerce", format=None)

    if dt.isna().all():
        num = pd.to_numeric(s, errors="coerce")
        finite = num[np.isfinite(num)]
        if not finite.empty:
            med = finite.median()
            if 30000 < med < 60000:  # Excel serial date
                dt = pd.to_datetime(num, unit="D", origin="1899-12-30", utc=True, errors="coerce")
            elif med > 1e11:        # epoch ms
                dt = pd.to_datetime(num, unit="ms", utc=True, errors="coerce")
            elif med > 1e9:         # epoch s
                dt = pd.to_datetime(num, unit="s", utc=True, errors="coerce")

    return dt.dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")


def _local_to_utc_str(series: pd.Series, tz_name: str) -> pd.Series:
    """
    Convert a local time string series to UTC naive strings 'YYYY-MM-DD HH:MM:SS'.
    Use when the source publishes local clock times (CAISO, NYISO PAL).
    """
    t_local = pd.to_datetime(series, errors="coerce")
    # handle nonexistent/ambiguous for DST edges
    t_local = t_local.dt.tz_localize(
        tz_name,
        nonexistent="shift_forward",
        ambiguous="NaT",
    )
    t_utc = t_local.dt.tz_convert("UTC")
    return t_utc.dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")


def _save(df: pd.DataFrame, iso: str) -> None:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = BRONZE_ROOT / iso / today_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{iso}_load.parquet"

    out = pd.DataFrame({
        "timestamp": pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S"),
        "load": pd.to_numeric(df["load"], errors="coerce").astype("float64"),
    }).dropna(subset=["timestamp", "load"])

    out.to_parquet(out_file, engine="pyarrow", index=False)
    logging.info(f"Saved {iso} load → {out_file}")


# ------------------------------
# CAISO
# ------------------------------
def fetch_caiso():
    logging.info("Fetching CAISO load")
    # Try GridStatus first
    if _HAS_GRIDSTATUS:
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            df = CAISO().get_load(date=today)
            cols = {c.lower(): c for c in df.columns}
            # Prefer time_utc if present; otherwise 'time' is local Pacific
            if "time_utc" in cols:
                ts = _to_spark_ts_mixed(df[cols["time_utc"]])
            else:
                time_col = cols.get("time") or list(df.columns)[0]
                ts = _local_to_utc_str(df[time_col], "America/Los_Angeles")
            load_col = cols.get("load") or "Load"
            out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(df[load_col], errors="coerce")})
            _save(out, "CAISO")
            return
        except Exception as e:
            logging.warning(f"CAISO via gridstatus failed: {e}. Trying CSV fallback.")

    # Public CSV fallback (times are Pacific local)
    try:
        url = "https://www.caiso.com/outlook/current/demand.csv"
        raw = pd.read_csv(url)
        tcol = next((c for c in raw.columns if c.lower().startswith("time")), raw.columns[0])
        lcol = next((c for c in raw.columns if "demand" in c.lower() or "load" in c.lower()), raw.columns[-1])
        ts = _local_to_utc_str(raw[tcol], "America/Los_Angeles")
        out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(raw[lcol], errors="coerce")})
        _save(out, "CAISO")
    except Exception as e:
        logging.error(f"CAISO failed entirely: {e}")


# ------------------------------
# MISO
# ------------------------------
def fetch_miso():
    logging.info("Fetching MISO load")
    # Use CSV endpoint because it exposes INTERVAL_START_GMT
    try:
        base = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"
        params = {"messageType": "gettotalload", "returnType": "csv"}
        resp = requests.get(base, params=params, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))

        cols = {c.lower(): c for c in df.columns}
        ts_col = cols.get("interval_start_gmt") or "INTERVAL_START_GMT"
        val_col = cols.get("value") or "VALUE"
        if ts_col not in df.columns or val_col not in df.columns:
            raise ValueError("MISO CSV missing INTERVAL_START_GMT or VALUE")

        ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        ts_str = ts.dt.tz_convert("UTC").dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        out = pd.DataFrame({"timestamp": ts_str, "load": pd.to_numeric(df[val_col], errors="coerce")})
        _save(out, "MISO")
    except Exception as e:
        # Optional GridStatus fallback
        if _HAS_GRIDSTATUS:
            try:
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                gdf = MISO().get_load(date=today)
                cols = {c.lower(): c for c in gdf.columns}
                tcol = cols.get("time_utc") or cols.get("time") or list(gdf.columns)[0]
                if tcol == cols.get("time"):
                    ts = _to_spark_ts_mixed(gdf[tcol])  # GridStatus often gives UTC string already
                else:
                    ts = _to_spark_ts_mixed(gdf[tcol])
                lcol = cols.get("load") or "Load"
                out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(gdf[lcol], errors="coerce")})
                _save(out, "MISO")
                return
            except Exception as ge:
                logging.error(f"MISO failed via CSV and gridstatus: {ge}")
        else:
            logging.error(f"MISO CSV failed: {e}")


# ------------------------------
# NYISO
# ------------------------------
def _nyiso_pal_url(dt_et: datetime) -> str:
    return f"http://mis.nyiso.com/public/csv/pal/{dt_et.strftime('%Y%m%d')}pal.csv"


def fetch_nyiso():
    logging.info("Fetching NYISO load")
    # Try GridStatus first
    if _HAS_GRIDSTATUS:
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            df = NYISO().get_load(date=today)
            cols = {c.lower(): c for c in df.columns}
            if "time_utc" in cols:
                ts = _to_spark_ts_mixed(df[cols["time_utc"]])
            else:
                time_col = cols.get("time") or list(df.columns)[0]
                # Some GridStatus versions return ET in 'time'
                ts = _local_to_utc_str(df[time_col], "America/New_York")
            load_col = cols.get("load") or "Load"
            out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(df[load_col], errors="coerce")})
            _save(out, "NYISO")
            return
        except Exception as e:
            logging.warning(f"NYISO via gridstatus failed: {e}. Trying PAL CSV fallback.")

    # PAL CSV fallback (times are Eastern local)
    try:
        try:
            from zoneinfo import ZoneInfo
            et_now = datetime.now(ZoneInfo("America/New_York"))
        except Exception:
            et_now = datetime.now()
        for dt_try in (et_now, et_now - timedelta(days=1)):
            url = _nyiso_pal_url(dt_try)
            try:
                raw = pd.read_csv(url)
                tcol = "Time Stamp" if "Time Stamp" in raw.columns else raw.columns[0]
                lcol = "Load" if "Load" in raw.columns else raw.columns[-1]
                ts = _local_to_utc_str(raw[tcol], "America/New_York")
                out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(raw[lcol], errors="coerce")})
                _save(out, "NYISO")
                return
            except Exception:
                continue
        raise RuntimeError("PAL CSV not available for today or yesterday in ET")
    except Exception as e:
        logging.error(f"NYISO failed entirely: {e}")


# ------------------------------
# SPP
# ------------------------------
def _first_tabular_list(obj):
    """Find the first list of dicts in a nested JSON."""
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj
        for item in obj:
            found = _first_tabular_list(item)
            if found is not None:
                return found
    elif isinstance(obj, dict):
        for k in ["Data", "data", "Rows", "rows", "LoadInfo", "response", "Response", "series", "forecast", "actual"]:
            if k in obj:
                found = _first_tabular_list(obj[k])
                if found is not None:
                    return found
        for v in obj.values():
            found = _first_tabular_list(v)
            if found is not None:
                return found
    return None


def fetch_spp():
    logging.info("Fetching SPP load")

    # Try GridStatus first
    if _HAS_GRIDSTATUS:
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            df = SPP().get_load(date=today)
            cols = {c.lower(): c for c in df.columns}
            tcol = cols.get("time_utc") or cols.get("time") or list(df.columns)[0]
            ts = _to_spark_ts_mixed(df[tcol])
            lcol = cols.get("load") or "Load"
            out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(df[lcol], errors="coerce")})
            _save(out, "SPP")
            return
        except Exception as e:
            logging.warning(f"SPP via gridstatus failed: {e}. Trying public JSON.")

    # Public JSON fallback
    endpoints = [
        "https://portal.spp.org/chart-api/load-forecast/asChart",
        "https://portal.spp.org/chart-api/load-actuals/asChart",
    ]
    headers = {"User-Agent": "GridCARE/1.0 (+https://example.com)"}

    def _pick(cols, candidates, default=None):
        low = {str(c).lower(): c for c in cols}
        for cand in candidates:
            if cand in low:
                return low[cand]
        return default or list(cols)[-1]

    for url in endpoints:
        try:
            r = requests.get(url, headers=headers, timeout=25)
            r.raise_for_status()
            try:
                j = r.json()
            except Exception:
                import json as _json
                j = _json.loads(r.text)

            records = None
            if isinstance(j, dict):
                resp = j.get("response")
                if isinstance(resp, dict):
                    series = resp.get("series")
                    if isinstance(series, list) and series:
                        frames = []
                        for ser in series:
                            data = ser.get("data", [])
                            if isinstance(data, list) and data:
                                if isinstance(data[0], list) and len(data[0]) >= 2:
                                    frames.append(pd.DataFrame(data, columns=["x", "y"]))
                                else:
                                    frames.append(pd.DataFrame(data))
                        if frames:
                            import pandas as _pd
                            records = _pd.concat(frames, ignore_index=True).to_dict("records")

            if records is None:
                records = _first_tabular_list(j)
            if not records:
                raise ValueError("No tabular series found in SPP JSON")

            raw = pd.DataFrame(records)
            tcol = _pick(raw.columns, ["x", "begin", "interval", "time", "timestamp", "datetime", "start"])
            vcol = _pick(raw.columns, ["y", "value", "load", "mw", "mwh"], default=list(raw.columns)[-1])

            out = pd.DataFrame({
                "timestamp": _to_spark_ts_mixed(raw[tcol]),
                "load": pd.to_numeric(raw[vcol], errors="coerce"),
            })
            _save(out, "SPP")
            return
        except Exception as e:
            logging.warning(f"SPP fallback {url} failed: {e}")

    logging.error("SPP failed entirely after all fallbacks")


# ------------------------------
# ISONE
# ------------------------------
def fetch_isone():
    logging.info("Fetching ISONE load")
    if not _HAS_GRIDSTATUS:
        logging.error("ISONE requires gridstatus. Skipping.")
        return
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        df = ISONE().get_load(date=today)
        cols = {c.lower(): c for c in df.columns}
        tcol = cols.get("time_utc") or cols.get("time") or list(df.columns)[0]
        ts = _to_spark_ts_mixed(df[tcol])
        lcol = cols.get("load") or "Load"
        out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(df[lcol], errors="coerce")})
        _save(out, "ISONE")
    except Exception as e:
        logging.error(f"ISONE failed: {e}")


# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    fetch_caiso()
    fetch_miso()
    fetch_nyiso()
    fetch_spp()
    fetch_isone()
    logging.info("Grid load ingestion completed.")
