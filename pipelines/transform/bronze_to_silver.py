import logging
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BRONZE_GRID_ROOT = Path("storage/bronze/grid/load")
BRONZE_WEATHER_ROOT = Path("storage/bronze/weather")
SILVER_GRID = Path("storage/silver/grid_clean")
SILVER_WEATHER = Path("storage/silver/weather_clean")
SILVER_ENRICHED = Path("storage/silver/grid_weather_enriched")

# Create output directories
SILVER_GRID.mkdir(parents=True, exist_ok=True)
SILVER_WEATHER.mkdir(parents=True, exist_ok=True)
SILVER_ENRICHED.mkdir(parents=True, exist_ok=True)

QUALITY_METRICS = {
    "grid_rows_read": 0,
    "grid_rows_cleaned": 0,
    "grid_rows_dropped": 0,
    "weather_rows_read": 0,
    "weather_rows_cleaned": 0,
    "weather_rows_dropped": 0,
    "enriched_rows": 0
}


def load_grid_bronze():
    """Load and clean Bronze grid data using Pandas."""
    logging.info("=" * 60)
    logging.info("STEP 1: Loading Bronze Grid Data")
    logging.info("=" * 60)
    
    # Find all parquet files
    parquet_files = list(BRONZE_GRID_ROOT.rglob("*.parquet"))
    
    if not parquet_files:
        logging.error("❌ No Bronze grid data found!")
        return None
    
    logging.info(f"📁 Found {len(parquet_files)} parquet files")
    
    # Load all files
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        # Extract ISO from path
        iso = file.parent.name
        df['iso_code'] = iso
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    QUALITY_METRICS["grid_rows_read"] = len(df)
    logging.info(f"📥 Read {QUALITY_METRICS['grid_rows_read']:,} raw grid rows")
    
    # Clean data
    # Try to find timestamp column
    ts_cols = ['timestamp', 'Time', 'Interval End', 'Interval Start', 'time', 'time_utc']
    ts_col = next((c for c in ts_cols if c in df.columns), None)
    
    # Try to find load column
    load_cols = ['load', 'Load', 'value', 'MW', 'MWh']
    load_col = next((c for c in load_cols if c in df.columns), None)
    
    if not ts_col or not load_col:
        logging.error("❌ Missing required columns")
        return None
    
    # Rename columns
    df = df.rename(columns={ts_col: 'timestamp_utc', load_col: 'load_mw'})
    
    # Convert types
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], errors='coerce')
    df['load_mw'] = pd.to_numeric(df['load_mw'], errors='coerce')
    
    # Filter invalid data
    df = df[
        df['iso_code'].notna() &
        (df['iso_code'] != '') &
        df['timestamp_utc'].notna() &
        df['load_mw'].notna() &
        (df['load_mw'] > 0) &
        (df['load_mw'] < 1000000)
    ]
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['iso_code', 'timestamp_utc'])
    
    QUALITY_METRICS["grid_rows_cleaned"] = len(df)
    QUALITY_METRICS["grid_rows_dropped"] = QUALITY_METRICS["grid_rows_read"] - QUALITY_METRICS["grid_rows_cleaned"]
    
    logging.info(f"✅ Cleaned {QUALITY_METRICS['grid_rows_cleaned']:,} grid rows")
    logging.info(f"🗑️ Dropped {QUALITY_METRICS['grid_rows_dropped']:,} invalid rows ({QUALITY_METRICS['grid_rows_dropped']/QUALITY_METRICS['grid_rows_read']*100:.1f}%)")
    
    # Save partitioned by ISO
    for iso in df['iso_code'].unique():
        iso_df = df[df['iso_code'] == iso][['iso_code', 'timestamp_utc', 'load_mw']]
        output_path = SILVER_GRID / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        iso_df.to_parquet(output_path / "data.parquet", index=False)
    
    logging.info(f"💾 Wrote Silver grid → {SILVER_GRID}")
    
    # Show summary
    logging.info("\n📊 Grid Data Summary by ISO:")
    summary = df.groupby('iso_code').agg({
        'timestamp_utc': ['min', 'max', 'count'],
        'load_mw': 'mean'
    }).round(2)
    print(summary)
    
    return df


def load_weather_bronze():
    """Load and clean Bronze weather data using Pandas."""
    logging.info("=" * 60)
    logging.info("STEP 2: Loading Bronze Weather Data")
    logging.info("=" * 60)
    
    parquet_files = list(BRONZE_WEATHER_ROOT.rglob("*.parquet"))
    
    if not parquet_files:
        logging.warning("⚠️ No Bronze weather data found!")
        return None
    
    logging.info(f"📁 Found {len(parquet_files)} parquet files")
    
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        iso = file.parent.name
        df['iso_code'] = iso
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    QUALITY_METRICS["weather_rows_read"] = len(df)
    logging.info(f"📥 Read {QUALITY_METRICS['weather_rows_read']:,} raw weather rows")
    
    # Clean data
    df['timestamp_utc'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['temperature'] = pd.to_numeric(df.get('temperature'), errors='coerce')
    df['wind_speed'] = pd.to_numeric(df.get('wind_speed'), errors='coerce')
    df['humidity'] = pd.to_numeric(df.get('humidity'), errors='coerce')
    
    # Filter valid data
    df = df[df['timestamp_utc'].notna()]
    df = df[(df['temperature'].isna()) | ((df['temperature'] >= -50) & (df['temperature'] <= 60))]
    df = df[(df['humidity'].isna()) | ((df['humidity'] >= 0) & (df['humidity'] <= 100))]
    df = df[(df['wind_speed'].isna()) | ((df['wind_speed'] >= 0) & (df['wind_speed'] <= 200))]
    
    df = df[['iso_code', 'timestamp_utc', 'temperature', 'wind_speed', 'humidity']]
    
    QUALITY_METRICS["weather_rows_cleaned"] = len(df)
    QUALITY_METRICS["weather_rows_dropped"] = QUALITY_METRICS["weather_rows_read"] - QUALITY_METRICS["weather_rows_cleaned"]
    
    logging.info(f"✅ Cleaned {QUALITY_METRICS['weather_rows_cleaned']:,} weather rows")
    logging.info(f"🗑️ Dropped {QUALITY_METRICS['weather_rows_dropped']:,} invalid rows")
    
    # Save partitioned by ISO
    for iso in df['iso_code'].unique():
        iso_df = df[df['iso_code'] == iso]
        output_path = SILVER_WEATHER / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        iso_df.to_parquet(output_path / "data.parquet", index=False)
    
    logging.info(f"💾 Wrote Silver weather → {SILVER_WEATHER}")
    
    return df


def enrich_grid_with_weather(grid_df, weather_df):
    """Join grid and weather data."""
    logging.info("=" * 60)
    logging.info("STEP 3: Enriching Grid Data with Weather")
    logging.info("=" * 60)
    
    if grid_df is None:
        logging.error("❌ No grid data to enrich")
        return
    
    if weather_df is None:
        logging.warning("⚠️ No weather data - creating enriched table without weather")
        enriched = grid_df.copy()
        enriched['hour'] = enriched['timestamp_utc'].dt.floor('H')
        enriched['temperature'] = None
        enriched['wind_speed'] = None
        enriched['humidity'] = None
        enriched['solar_irradiance_proxy'] = None
    else:
        # Merge on ISO and nearest timestamp (within 60 min)
        enriched = pd.merge_asof(
            grid_df.sort_values('timestamp_utc'),
            weather_df.sort_values('timestamp_utc'),
            on='timestamp_utc',
            by='iso_code',
            tolerance=pd.Timedelta('60min'),
            direction='nearest'
        )
        enriched['hour'] = enriched['timestamp_utc'].dt.floor('H')
        enriched['solar_irradiance_proxy'] = None
    
    QUALITY_METRICS["enriched_rows"] = len(enriched)
    logging.info(f"✅ Created {QUALITY_METRICS['enriched_rows']:,} enriched rows")
    
    if weather_df is not None:
        with_weather = enriched['temperature'].notna().sum()
        join_rate = (with_weather / QUALITY_METRICS['enriched_rows'] * 100) if QUALITY_METRICS['enriched_rows'] > 0 else 0
        logging.info(f"🌤️ Weather join rate: {join_rate:.1f}% ({with_weather:,} rows have weather data)")
    
    # Save partitioned by ISO
    for iso in enriched['iso_code'].unique():
        iso_df = enriched[enriched['iso_code'] == iso]
        output_path = SILVER_ENRICHED / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        iso_df.to_parquet(output_path / "data.parquet", index=False)
    
    logging.info(f"💾 Wrote Silver enriched → {SILVER_ENRICHED}")


def print_quality_report():
    """Print final quality report."""
    logging.info("\n" + "=" * 60)
    logging.info("📋 DATA QUALITY REPORT")
    logging.info("=" * 60)
    
    logging.info("\n📹 Grid Data:")
    logging.info(f"  Raw rows:     {QUALITY_METRICS['grid_rows_read']:>10,}")
    logging.info(f"  Cleaned rows: {QUALITY_METRICS['grid_rows_cleaned']:>10,}")
    logging.info(f"  Dropped rows: {QUALITY_METRICS['grid_rows_dropped']:>10,}")
    if QUALITY_METRICS['grid_rows_read'] > 0:
        logging.info(f"  Quality rate: {QUALITY_METRICS['grid_rows_cleaned']/QUALITY_METRICS['grid_rows_read']*100:>9.1f}%")
    
    logging.info("\n📹 Weather Data:")
    logging.info(f"  Raw rows:     {QUALITY_METRICS['weather_rows_read']:>10,}")
    logging.info(f"  Cleaned rows: {QUALITY_METRICS['weather_rows_cleaned']:>10,}")
    logging.info(f"  Dropped rows: {QUALITY_METRICS['weather_rows_dropped']:>10,}")
    if QUALITY_METRICS['weather_rows_read'] > 0:
        logging.info(f"  Quality rate: {QUALITY_METRICS['weather_rows_cleaned']/QUALITY_METRICS['weather_rows_read']*100:>9.1f}%")
    
    logging.info(f"\n📹 Enriched rows: {QUALITY_METRICS['enriched_rows']:>10,}")
    logging.info("=" * 60)


def main():
    start_time = datetime.now(timezone.utc)
    logging.info("=" * 60)
    logging.info("🚀 Bronze → Silver Transformation Started (Pandas)")
    logging.info(f"⏰ Start time: {start_time.isoformat()}")
    logging.info("=" * 60)
    
    try:
        grid_df = load_grid_bronze()
        weather_df = load_weather_bronze()
        enrich_grid_with_weather(grid_df, weather_df)
        print_quality_report()
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        logging.info("\n" + "=" * 60)
        logging.info(f"✅ Transformation Complete in {duration:.1f}s")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Transformation failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()