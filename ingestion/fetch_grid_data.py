import io
import logging
import warnings
from pathlib import Path
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

try:
    from gridstatus import CAISO, MISO, NYISO, SPP, ISONE
    _HAS_GRIDSTATUS = True
except Exception:
    _HAS_GRIDSTATUS = False
    logging.warning("GridStatus library not available. Using fallback APIs only.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BRONZE_ROOT = Path("storage/bronze/grid/load")
SUCCESS_COUNT = 0
FAILURE_COUNT = 0

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _to_spark_ts_mixed(s: pd.Series) -> pd.Series:
    """Convert timestamps to UTC naive strings 'YYYY-MM-DD HH:MM:SS'."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        dt = pd.to_datetime(s, utc=True, errors="coerce", format=None)

    if dt.isna().all():
        num = pd.to_numeric(s, errors="coerce")
        finite = num[np.isfinite(num)]
        if not finite.empty:
            med = finite.median()
            if 30000 < med < 60000:
                dt = pd.to_datetime(num, unit="D", origin="1899-12-30", utc=True, errors="coerce")
            elif med > 1e11:
                dt = pd.to_datetime(num, unit="ms", utc=True, errors="coerce")
            elif med > 1e9:
                dt = pd.to_datetime(num, unit="s", utc=True, errors="coerce")

    return dt.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")

def _local_to_utc_str(series: pd.Series, tz_name: str) -> pd.Series:
    """Convert local time strings to UTC naive strings."""
    t_local = pd.to_datetime(series, errors="coerce")
    
    # ✅ FIX: Check if already tz-aware
    if t_local.dt.tz is not None:
        t_utc = t_local.dt.tz_convert("UTC")
    else:
        t_local = t_local.dt.tz_localize(
            tz_name,
            nonexistent="shift_forward",
            ambiguous="NaT",
        )
        t_utc = t_local.dt.tz_convert("UTC")
    
    return t_utc.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")

def _save(df: pd.DataFrame, iso: str) -> None:
    global SUCCESS_COUNT
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = BRONZE_ROOT / iso / today_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{iso}_load.parquet"

    out = pd.DataFrame({
        "timestamp": pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S"),
        "load": pd.to_numeric(df["load"], errors="coerce").astype("float64"),
    }).dropna(subset=["timestamp", "load"])

    if out.empty:
        logging.warning(f"{iso}: No valid data to save after cleaning")
        return

    out.to_parquet(out_file, engine="pyarrow", index=False)
    SUCCESS_COUNT += 1
    logging.info(f"✅ {iso}: Saved {len(out)} rows → {out_file}")

# ------------------------------
# CAISO - FIXED
# ------------------------------
def fetch_caiso():
    global FAILURE_COUNT
    logging.info("Fetching CAISO load")
    
    if _HAS_GRIDSTATUS:
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            df = CAISO().get_load(date=today)
            cols = {c.lower(): c for c in df.columns}
            
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

    # ✅ FIX: Remove timeout parameter
    try:
        url = "https://www.caiso.com/outlook/current/demand.csv"
        raw = pd.read_csv(url)  # ✅ No timeout parameter
        tcol = next((c for c in raw.columns if c.lower().startswith("time")), raw.columns[0])
        lcol = next((c for c in raw.columns if "demand" in c.lower() or "load" in c.lower()), raw.columns[-1])
        ts = _local_to_utc_str(raw[tcol], "America/Los_Angeles")
        out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(raw[lcol], errors="coerce")})
        _save(out, "CAISO")
    except Exception as e:
        FAILURE_COUNT += 1
        logging.error(f"❌ CAISO failed entirely: {e}")

# ------------------------------
# MISO - FIXED
# ------------------------------
def fetch_miso():
    global FAILURE_COUNT
    logging.info("Fetching MISO load")
    
    try:
        base = "https://api.misoenergy.org/MISORTWDDataBroker/DataBrokerServices.asmx"
        params = {"messageType": "gettotalload", "returnType": "csv"}
        resp = requests.get(base, params=params, timeout=30)
        resp.raise_for_status()
        
        df = pd.read_csv(io.StringIO(resp.text))
        
        # ✅ FIX: Flexible column detection
        ts_col = None
        val_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'interval' in col_lower and ('start' in col_lower or 'gmt' in col_lower or 'time' in col_lower):
                ts_col = col
            if 'value' in col_lower or 'load' in col_lower or 'mw' in col_lower:
                val_col = col
        
        # Fallback to first two columns
        if not ts_col:
            ts_col = df.columns[0]
        if not val_col:
            val_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        logging.info(f"MISO: Using columns {ts_col} (time), {val_col} (value)")

        ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        ts_str = ts.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        out = pd.DataFrame({"timestamp": ts_str, "load": pd.to_numeric(df[val_col], errors="coerce")})
        _save(out, "MISO")
    except Exception as e:
        FAILURE_COUNT += 1
        logging.error(f"❌ MISO failed: {e}")

# ------------------------------
# NYISO - FIXED
# ------------------------------
def fetch_nyiso():
    global FAILURE_COUNT
    logging.info("Fetching NYISO load")
    
    if _HAS_GRIDSTATUS:
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            df = NYISO().get_load(date=today)
            cols = {c.lower(): c for c in df.columns}
            
            if "time_utc" in cols:
                ts = _to_spark_ts_mixed(df[cols["time_utc"]])
            else:
                time_col = cols.get("time") or list(df.columns)[0]
                ts = _local_to_utc_str(df[time_col], "America/New_York")  # ✅ Fixed function handles tz-aware
            
            load_col = cols.get("load") or "Load"
            out = pd.DataFrame({"timestamp": ts, "load": pd.to_numeric(df[load_col], errors="coerce")})
            _save(out, "NYISO")
            return
        except Exception as e:
            logging.warning(f"NYISO via gridstatus failed: {e}")

    # PAL CSV fallback
    try:
        from zoneinfo import ZoneInfo
        et_now = datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        et_now = datetime.now()
    
    for dt_try in (et_now, et_now - timedelta(days=1)):
        url = f"http://mis.nyiso.com/public/csv/pal/{dt_try.strftime('%Y%m%d')}pal.csv"
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
    
    FAILURE_COUNT += 1
    logging.error("❌ NYISO failed entirely")

# ------------------------------
# SPP
# ------------------------------
def fetch_spp():
    global FAILURE_COUNT
    logging.info("Fetching SPP load")

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
            logging.warning(f"SPP via gridstatus failed: {e}")

    FAILURE_COUNT += 1
    logging.error("❌ SPP failed")

# ------------------------------
# ISONE
# ------------------------------
def fetch_isone():
    global FAILURE_COUNT
    logging.info("Fetching ISONE load")
    
    if not _HAS_GRIDSTATUS:
        FAILURE_COUNT += 1
        logging.error("❌ ISONE requires gridstatus. Skipping.")
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
        FAILURE_COUNT += 1
        logging.error(f"❌ ISONE failed: {e}")

# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Starting Grid Load Ingestion")
    logging.info("=" * 60)
    
    _ensure_dir(BRONZE_ROOT)
    
    fetch_caiso()
    fetch_miso()
    fetch_nyiso()
    fetch_spp()
    fetch_isone()
    
    logging.info("=" * 60)
    logging.info(f"Grid Load Ingestion Complete: {SUCCESS_COUNT} succeeded, {FAILURE_COUNT} failed")
    logging.info("=" * 60)
    
    if SUCCESS_COUNT == 0:
        logging.error("⚠️  ALL ISO ingestions failed!")
        exit(1)
    
    exit(0)