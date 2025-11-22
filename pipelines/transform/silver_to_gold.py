import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Paths
SILVER_ENRICHED = Path("storage/silver/grid_weather_enriched")
GOLD_ROOT = Path("storage/gold")
GOLD_HOURLY = GOLD_ROOT / "hourly_metrics"
GOLD_DAILY = GOLD_ROOT / "daily_metrics"
GOLD_ISO_SUMMARY = GOLD_ROOT / "iso_summary"

# Create output directories
GOLD_HOURLY.mkdir(parents=True, exist_ok=True)
GOLD_DAILY.mkdir(parents=True, exist_ok=True)
GOLD_ISO_SUMMARY.mkdir(parents=True, exist_ok=True)


def load_silver_enriched():
    """Load Silver enriched data."""
    logging.info("\n📥 Loading Silver enriched data...")
    
    parquet_files = list(SILVER_ENRICHED.rglob("*.parquet"))
    
    if not parquet_files:
        logging.error("❌ No Silver enriched data found!")
        return None
    
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    logging.info(f"✅ Loaded {len(df):,} enriched rows")
    
    # Ensure datetime types
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['hour'] = pd.to_datetime(df['hour'])
    
    return df


def create_hourly_metrics(df):
    """Create hourly aggregated metrics."""
    logging.info("=" * 60)
    logging.info("STEP 1: Creating Hourly Metrics")
    logging.info("=" * 60)
    
    # Group by ISO and hour
    hourly = df.groupby(['iso_code', 'hour']).agg({
        'load_mw': ['mean', 'min', 'max', 'std', 'count'],
        'temperature': ['mean', 'min', 'max'],
        'wind_speed': ['mean', 'max'],
        'humidity': 'mean'
    }).reset_index()
    
    # Flatten column names
    hourly.columns = [
        'iso_code', 'hour',
        'avg_load_mw', 'min_load_mw', 'max_load_mw', 'std_load_mw', 'data_points',
        'avg_temperature', 'min_temperature', 'max_temperature',
        'avg_wind_speed', 'max_wind_speed',
        'avg_humidity'
    ]
    
    # Calculate load variability (coefficient of variation)
    hourly['load_variability'] = (hourly['std_load_mw'] / hourly['avg_load_mw'] * 100).round(2)
    
    # Add time features
    hourly['hour_of_day'] = hourly['hour'].dt.hour
    hourly['day_of_week'] = hourly['hour'].dt.dayofweek
    hourly['is_weekend'] = hourly['day_of_week'].isin([5, 6]).astype(int)
    
    # Round numeric columns
    numeric_cols = hourly.select_dtypes(include=[np.number]).columns
    hourly[numeric_cols] = hourly[numeric_cols].round(2)
    
    logging.info(f"✅ Created {len(hourly):,} hourly metric rows")
    
    # Save partitioned by ISO
    for iso in hourly['iso_code'].unique():
        iso_df = hourly[hourly['iso_code'] == iso]
        output_path = GOLD_HOURLY / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        iso_df.to_parquet(output_path / "data.parquet", index=False)
    
    logging.info(f"💾 Wrote Gold hourly metrics → {GOLD_HOURLY}")
    
    # Show sample
    logging.info("\n📊 Sample Hourly Metrics:")
    print(hourly.head(10).to_string(index=False))
    
    return hourly


def create_daily_metrics(df):
    """Create daily aggregated metrics."""
    logging.info("=" * 60)
    logging.info("STEP 2: Creating Daily Metrics")
    logging.info("=" * 60)
    
    # Add date column
    df['date'] = df['timestamp_utc'].dt.date
    
    # Group by ISO and date
    daily = df.groupby(['iso_code', 'date']).agg({
        'load_mw': ['mean', 'min', 'max', 'std', 'sum', 'count'],
        'temperature': ['mean', 'min', 'max'],
        'wind_speed': ['mean', 'max'],
        'humidity': 'mean'
    }).reset_index()
    
    # Flatten column names
    daily.columns = [
        'iso_code', 'date',
        'avg_load_mw', 'min_load_mw', 'max_load_mw', 'std_load_mw', 'total_load_mwh', 'data_points',
        'avg_temperature', 'min_temperature', 'max_temperature',
        'avg_wind_speed', 'max_wind_speed',
        'avg_humidity'
    ]
    
    # Calculate peak-to-average ratio
    daily['peak_to_avg_ratio'] = (daily['max_load_mw'] / daily['avg_load_mw']).round(2)
    
    # Calculate load factor (how consistently load is at peak)
    daily['load_factor'] = (daily['avg_load_mw'] / daily['max_load_mw'] * 100).round(2)
    
    # Add day of week
    daily['date'] = pd.to_datetime(daily['date'])
    daily['day_of_week'] = daily['date'].dt.dayofweek
    daily['is_weekend'] = daily['day_of_week'].isin([5, 6]).astype(int)
    daily['day_name'] = daily['date'].dt.day_name()
    
    # Round numeric columns
    numeric_cols = daily.select_dtypes(include=[np.number]).columns
    daily[numeric_cols] = daily[numeric_cols].round(2)
    
    logging.info(f"✅ Created {len(daily):,} daily metric rows")
    
    # Save partitioned by ISO
    for iso in daily['iso_code'].unique():
        iso_df = daily[daily['iso_code'] == iso]
        output_path = GOLD_DAILY / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        iso_df.to_parquet(output_path / "data.parquet", index=False)
    
    logging.info(f"💾 Wrote Gold daily metrics → {GOLD_DAILY}")
    
    # Show sample
    logging.info("\n📊 Sample Daily Metrics:")
    print(daily.head(10).to_string(index=False))
    
    return daily


def create_iso_summary(df):
    """Create ISO-level summary statistics."""
    logging.info("=" * 60)
    logging.info("STEP 3: Creating ISO Summary")
    logging.info("=" * 60)
    
    # Overall statistics by ISO
    summary = df.groupby('iso_code').agg({
        'timestamp_utc': ['min', 'max', 'count'],
        'load_mw': ['mean', 'min', 'max', 'std'],
        'temperature': ['mean', 'min', 'max'],
        'wind_speed': 'mean',
        'humidity': 'mean'
    }).reset_index()
    
    # Flatten columns
    summary.columns = [
        'iso_code',
        'data_start', 'data_end', 'total_observations',
        'avg_load_mw', 'min_load_mw', 'max_load_mw', 'std_load_mw',
        'avg_temperature', 'min_temperature', 'max_temperature',
        'avg_wind_speed', 'avg_humidity'
    ]
    
    # Calculate data quality metrics
    summary['data_completeness'] = (
        df.groupby('iso_code')['temperature'].apply(lambda x: x.notna().sum()) / 
        df.groupby('iso_code').size() * 100
    ).round(2).values
    
    # Calculate date range in days
    summary['data_span_days'] = (
        (summary['data_end'] - summary['data_start']).dt.total_seconds() / 86400
    ).round(1)
    
    # Add capacity factor estimate (if we had capacity data)
    summary['estimated_capacity_mw'] = (summary['max_load_mw'] * 1.1).round(0)
    summary['avg_capacity_factor'] = (
        summary['avg_load_mw'] / summary['estimated_capacity_mw'] * 100
    ).round(2)
    
    # Round numeric columns
    numeric_cols = summary.select_dtypes(include=[np.number]).columns
    summary[numeric_cols] = summary[numeric_cols].round(2)
    
    logging.info(f"✅ Created ISO summary for {len(summary)} ISOs")
    
    # Save
    summary.to_parquet(GOLD_ISO_SUMMARY / "iso_summary.parquet", index=False)
    logging.info(f"💾 Wrote Gold ISO summary → {GOLD_ISO_SUMMARY}")
    
    # Show full summary
    logging.info("\n📊 ISO Summary Statistics:")
    print(summary.to_string(index=False))
    
    return summary


def generate_insights(hourly, daily, summary):
    """Generate insights from the data."""
    logging.info("=" * 60)
    logging.info("STEP 4: Generating Insights")
    logging.info("=" * 60)
    
    insights = []
    
    # 1. Peak demand patterns
    logging.info("\n🔍 Peak Demand Patterns:")
    for iso in hourly['iso_code'].unique():
        iso_hourly = hourly[hourly['iso_code'] == iso]
        peak_hour = iso_hourly.loc[iso_hourly['avg_load_mw'].idxmax()]
        insights.append(f"  • {iso}: Peak load at hour {int(peak_hour['hour_of_day'])}:00 ({peak_hour['avg_load_mw']:.0f} MW)")
        logging.info(insights[-1])
    
    # 2. Weekend vs Weekday comparison
    logging.info("\n🔍 Weekend vs Weekday Load:")
    for iso in daily['iso_code'].unique():
        iso_daily = daily[daily['iso_code'] == iso]
        weekday_avg = iso_daily[iso_daily['is_weekend'] == 0]['avg_load_mw'].mean()
        weekend_avg = iso_daily[iso_daily['is_weekend'] == 1]['avg_load_mw'].mean()
        if not pd.isna(weekday_avg) and not pd.isna(weekend_avg):
            diff_pct = ((weekday_avg - weekend_avg) / weekend_avg * 100)
            insights.append(f"  • {iso}: Weekday load {diff_pct:+.1f}% vs weekend")
            logging.info(insights[-1])
    
    # 3. Temperature correlation
    logging.info("\n🔍 Weather Impact (Temperature Correlation):")
    for iso in hourly['iso_code'].unique():
        iso_hourly = hourly[hourly['iso_code'] == iso].dropna(subset=['avg_temperature', 'avg_load_mw'])
        if len(iso_hourly) > 10:
            corr = iso_hourly['avg_temperature'].corr(iso_hourly['avg_load_mw'])
            insights.append(f"  • {iso}: Temperature correlation = {corr:.3f}")
            logging.info(insights[-1])
    
    # 4. Load variability
    logging.info("\n🔍 Load Variability:")
    for iso in summary['iso_code'].unique():
        iso_summary = summary[summary['iso_code'] == iso].iloc[0]
        cv = (iso_summary['std_load_mw'] / iso_summary['avg_load_mw'] * 100)
        insights.append(f"  • {iso}: Coefficient of variation = {cv:.1f}%")
        logging.info(insights[-1])
    
    # Save insights
    insights_df = pd.DataFrame({'insight': insights})
    insights_df.to_parquet(GOLD_ROOT / "insights.parquet", index=False)
    
    # Also save as text file
    with open(GOLD_ROOT / "insights.txt", 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("GRIDCARE ENERGY DATA INSIGHTS\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
        for insight in insights:
            f.write(insight + "\n")
    
    logging.info(f"\n💾 Wrote insights → {GOLD_ROOT}/insights.txt")


def print_summary_report():
    """Print final summary report."""
    logging.info("\n" + "=" * 60)
    logging.info("📊 GOLD LAYER SUMMARY")
    logging.info("=" * 60)
    
    # Count files
    hourly_files = len(list(GOLD_HOURLY.rglob("*.parquet")))
    daily_files = len(list(GOLD_DAILY.rglob("*.parquet")))
    summary_files = len(list(GOLD_ISO_SUMMARY.rglob("*.parquet")))
    
    logging.info(f"\n📁 Gold Layer Assets Created:")
    logging.info(f"  • Hourly metrics: {hourly_files} partition(s)")
    logging.info(f"  • Daily metrics: {daily_files} partition(s)")
    logging.info(f"  • ISO summaries: {summary_files} file(s)")
    logging.info(f"  • Insights: 1 file")
    
    logging.info(f"\n📂 Gold Layer Location: {GOLD_ROOT.absolute()}")
    logging.info("=" * 60)


def main():
    start_time = datetime.now(timezone.utc)
    logging.info("=" * 60)
    logging.info("🚀 Silver → Gold Transformation Started (Pandas)")
    logging.info(f"⏰ Start time: {start_time.isoformat()}")
    logging.info("=" * 60)
    
    try:
        # Load Silver data
        enriched = load_silver_enriched()
        
        if enriched is None or len(enriched) == 0:
            logging.error("❌ No data to process")
            return
        
        # Create Gold layer aggregations
        hourly = create_hourly_metrics(enriched)
        daily = create_daily_metrics(enriched)
        summary = create_iso_summary(enriched)
        
        # Generate insights
        generate_insights(hourly, daily, summary)
        
        # Print summary
        print_summary_report()
        
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