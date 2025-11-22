import os
import sys
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Folders to sanitize
ROOTS = [
    Path("storage/bronze/grid/load"),
    Path("storage/bronze/weather"),
]

TS_COL_CANDIDATES = {"timestamp", "time", "time_utc", "datetime", "begin", "interval", "x"}

def sanitize_file(pq_path: Path) -> None:
    try:
        table = pq.read_table(pq_path)
    except Exception as e:
        logging.warning(f"[SKIP] {pq_path} read failed: {e}")
        return

    df = table.to_pandas(types_mapper=None)  # keep as pandas dtypes

    # detect timestamp-like cols
    cols = set(c.lower() for c in df.columns)
    ts_cols = [c for c in df.columns if c.lower() in TS_COL_CANDIDATES]
    if not ts_cols:
        # try any pandas datetime dtype
        ts_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    # normalize timestamps to string
    for c in ts_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
        df[c] = df[c].astype("string")  # ensure Arrow writes as plain string

    # normalize load if present
    load_like = [c for c in df.columns if c.lower() in ("load", "value", "mw", "mwh", "y")]
    if load_like:
        # choose first and standardize column name to 'load' if needed
        first = load_like[0]
        if first != "load":
            if "load" in df.columns and first != "load":
                # already exists, just coerce
                df["load"] = pd.to_numeric(df["load"], errors="coerce")
            else:
                df.rename(columns={first: "load"}, inplace=True)
        df["load"] = pd.to_numeric(df["load"], errors="coerce")

    tmp = pq_path.with_suffix(".parquet.tmp")
    try:
        table_out = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(
            table_out,
            tmp,
            compression="snappy",
            coerce_timestamps="ms",  # extra safety even though we wrote strings
            use_deprecated_int96_timestamps=False,
        )
        os.replace(tmp, pq_path)  # atomic replace
        logging.info(f"[OK]  sanitized {pq_path}")
    except Exception as e:
        # cleanup tmp on failure
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        logging.error(f"[FAIL] {pq_path}: {e}")

def walk_and_sanitize(root: Path) -> None:
    if not root.exists():
        return
    for p in root.rglob("*.parquet"):
        sanitize_file(p)

if __name__ == "__main__":
    for r in ROOTS:
        logging.info(f"Scanning {r}")
        walk_and_sanitize(r)
    logging.info("Sanitization complete.")
