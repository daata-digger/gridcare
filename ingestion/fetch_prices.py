"""
Price Data Ingestion
Fetches LMP (Locational Marginal Price) data from ISO APIs
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

try:
    from gridstatus import CAISO, MISO, NYISO, SPP, ISONE
    _HAS_GRIDSTATUS = True
except Exception:
    _HAS_GRIDSTATUS = False
    logging.warning("GridStatus library not available.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BRONZE_ROOT = Path("storage/bronze/prices")
SUCCESS_COUNT = 0
FAILURE_COUNT = 0

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _save(df: pd.DataFrame, iso: str) -> None:
    global SUCCESS_COUNT
    
    if df.empty:
        logging.warning(f"{iso}: No price data to save")
        return
    
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = BRONZE_ROOT / iso / today_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{iso}_prices.parquet"
    
    # Ensure proper schema
    out = pd.DataFrame({
        "timestamp": pd.to_datetime(df["timestamp"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S"),
        "location": df.get("location", "SYSTEM").astype(str),
        "lmp": pd.to_numeric(df["lmp"], errors="coerce"),
        "energy": pd.to_numeric(df.get("energy", df["lmp"]), errors="coerce"),
        "congestion": pd.to_numeric(df.get("congestion", 0), errors="coerce"),
        "loss": pd.to_numeric(df.get("loss", 0), errors="coerce"),
    }).dropna(subset=["timestamp", "lmp"])
    
    if out.empty:
        logging.warning(f"{iso}: No valid price data after cleaning")
        return
    
    out.to_parquet(out_file, engine="pyarrow", index=False)
    SUCCESS_COUNT += 1
    logging.info(f"✅ {iso}: Saved {len(out)} price records → {out_file}")


# ------------------------------
# CAISO Prices
# ------------------------------
def fetch_caiso_prices():
    global FAILURE_COUNT
    logging.info("Fetching CAISO prices")
    
    if not _HAS_GRIDSTATUS:
        FAILURE_COUNT += 1
        logging.error("❌ CAISO prices require gridstatus library")
        return
    
    try:
        # Use yesterday's date (today's data not available yet)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # CAISO uses "RTM" for real-time market
        df = CAISO().get_lmp(date=yesterday, market="RTM")
        
        if df.empty:
            FAILURE_COUNT += 1
            logging.warning("❌ CAISO: No price data returned")
            return
        
        # Rename columns to standard schema
        df_clean = pd.DataFrame({
            "timestamp": df.get("Time") or df.get("time") or df.index,
            "location": df.get("Location") or df.get("location", "SYSTEM"),
            "lmp": df.get("LMP") or df.get("lmp") or df.get("price"),
            "energy": df.get("Energy") or df.get("energy", None),
            "congestion": df.get("Congestion") or df.get("congestion", 0),
            "loss": df.get("Loss") or df.get("loss", 0),
        })
        
        _save(df_clean, "CAISO")
        
    except Exception as e:
        FAILURE_COUNT += 1
        logging.error(f"❌ CAISO prices failed: {e}")


# ------------------------------
# MISO Prices
# ------------------------------
def fetch_miso_prices():
    global FAILURE_COUNT
    logging.info("Fetching MISO prices")
    
    if not _HAS_GRIDSTATUS:
        FAILURE_COUNT += 1
        logging.error("❌ MISO prices require gridstatus library")
        return
    
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        try:
            df = MISO().get_lmp(date=today, market="REAL_TIME_5_MIN")
        except:
            try:
                df = MISO().get_lmp(date=today, market="RT_LMP")
            except:
                df = MISO().get_lmp(date=today)  # Use default
        
        if df.empty:
            FAILURE_COUNT += 1
            logging.warning("❌ MISO: No price data returned")
            return
        
        df_clean = pd.DataFrame({
            "timestamp": df.get("Time") or df.get("interval_start") or df.index,
            "location": df.get("Location") or df.get("location", "HUB"),
            "lmp": df.get("LMP") or df.get("lmp"),
            "energy": df.get("Energy") or df.get("energy", None),
            "congestion": df.get("Congestion") or df.get("congestion", 0),
            "loss": df.get("Loss") or df.get("loss", 0),
        })
        
        _save(df_clean, "MISO")
        
    except Exception as e:
        FAILURE_COUNT += 1
        logging.error(f"❌ MISO prices failed: {e}")


# ------------------------------
# NYISO Prices
# ------------------------------
def fetch_nyiso_prices():
    global FAILURE_COUNT
    logging.info("Fetching NYISO prices")
    
    if not _HAS_GRIDSTATUS:
        FAILURE_COUNT += 1
        logging.error("❌ NYISO prices require gridstatus library")
        return
    
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        try:
            df = NYISO().get_lmp(date=today, market="REAL_TIME_5_MIN")
        except:
            try:
                df = NYISO().get_lmp(date=today, market="REAL_TIME_HOURLY")
            except:
                df = NYISO().get_lmp(date=today)  # Use default
        
        if df.empty:
            FAILURE_COUNT += 1
            logging.warning("❌ NYISO: No price data returned")
            return
        
        df_clean = pd.DataFrame({
            "timestamp": df.get("Time") or df.get("time") or df.index,
            "location": df.get("Location") or df.get("location", "NYISO"),
            "lmp": df.get("LMP") or df.get("lmp"),
            "energy": df.get("Energy") or df.get("energy", None),
            "congestion": df.get("Congestion") or df.get("congestion", 0),
            "loss": df.get("Loss") or df.get("loss", 0),
        })
        
        _save(df_clean, "NYISO")
        
    except Exception as e:
        FAILURE_COUNT += 1
        logging.error(f"❌ NYISO prices failed: {e}")


# ------------------------------
# SPP Prices
# ------------------------------
def fetch_spp_prices():
    global FAILURE_COUNT
    logging.info("Fetching SPP prices")
    
    # SPP doesn't have LMP API in gridstatus, skip for now
    FAILURE_COUNT += 1
    logging.warning("⚠️  SPP: LMP data not available via gridstatus (not supported)")
    return


# ------------------------------
# ISONE Prices
# ------------------------------
def fetch_isone_prices():
    global FAILURE_COUNT
    logging.info("Fetching ISONE prices")
    
    if not _HAS_GRIDSTATUS:
        FAILURE_COUNT += 1
        logging.error("❌ ISONE prices require gridstatus library")
        return
    
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        try:
            df = ISONE().get_lmp(date=today, market="REAL_TIME_5_MIN")
        except:
            try:
                df = ISONE().get_lmp(date=today, market="REAL_TIME_HOURLY")
            except:
                df = ISONE().get_lmp(date=today)  # Use default
        
        if df.empty:
            FAILURE_COUNT += 1
            logging.warning("❌ ISONE: No price data returned")
            return
        
        df_clean = pd.DataFrame({
            "timestamp": df.get("Time") or df.get("time") or df.index,
            "location": df.get("Location") or df.get("location", "HUB"),
            "lmp": df.get("LMP") or df.get("lmp"),
            "energy": df.get("Energy") or df.get("energy", None),
            "congestion": df.get("Congestion") or df.get("congestion", 0),
            "loss": df.get("Loss") or df.get("loss", 0),
        })
        
        _save(df_clean, "ISONE")
        
    except Exception as e:
        FAILURE_COUNT += 1
        logging.error(f"❌ ISONE prices failed: {e}")


# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    logging.info("=" * 60)
    logging.info("Starting Price Data Ingestion")
    logging.info("=" * 60)
    
    _ensure_dir(BRONZE_ROOT)
    
    fetch_caiso_prices()
    fetch_miso_prices()
    fetch_nyiso_prices()
    fetch_spp_prices()
    fetch_isone_prices()
    
    logging.info("=" * 60)
    logging.info(f"Price Ingestion Complete: {SUCCESS_COUNT} succeeded, {FAILURE_COUNT} failed")
    logging.info("=" * 60)
    
    if SUCCESS_COUNT == 0:
        logging.error("⚠️  ALL price ingestions failed!")
        exit(1)
    
    exit(0)