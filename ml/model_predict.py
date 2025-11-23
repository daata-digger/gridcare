import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Paths
SILVER_ENRICHED = Path("storage/silver/grid_weather_enriched")
GOLD_HOURLY = Path("storage/gold/hourly_metrics")
PREDICTIONS_OUTPUT = Path("storage/ml/predictions")
MODEL_METADATA = Path("storage/ml/model_metadata.json")

# Create output directory
PREDICTIONS_OUTPUT.mkdir(parents=True, exist_ok=True)


def load_recent_data(hours_back=168):
    """Load recent data from Silver enriched layer."""
    logging.info(f"📥 Loading recent {hours_back} hours of data...")
    
    parquet_files = list(SILVER_ENRICHED.rglob("*.parquet"))
    if not parquet_files:
        logging.error("❌ No enriched data found!")
        return None
    
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    
    # Filter to recent data (make cutoff tz-naive to match data)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    cutoff_naive = cutoff.replace(tzinfo=None)  # ✅ Remove timezone for comparison
    df = df[df['timestamp_utc'] >= cutoff_naive]
    
    logging.info(f"✅ Loaded {len(df):,} rows from last {hours_back} hours")
    return df


def create_features_for_forecast(last_timestamp, iso_code, hours_ahead=24):
    """Create feature dataframe for future timestamps."""
    # Generate future timestamps
    future_times = [last_timestamp + timedelta(hours=i) for i in range(1, hours_ahead + 1)]
    
    features = []
    for ts in future_times:
        features.append({
            'iso_code': iso_code,
            'timestamp_utc': ts,
            'hour_of_day': ts.hour,
            'day_of_week': ts.weekday(),
            'is_weekend': 1 if ts.weekday() >= 5 else 0,
        })
    
    return pd.DataFrame(features)


def simple_forecast_model(historical_df, iso_code, hours_ahead=24):
    """
    Simple time-series forecasting using historical patterns.
    Uses:
    - Same hour/day-of-week average from past 4 weeks
    - Exponential smoothing for recent trend
    - 95% confidence intervals
    """
    iso_data = historical_df[historical_df['iso_code'] == iso_code].copy()
    
    if len(iso_data) < 24:
        logging.warning(f"⚠️  {iso_code}: Not enough data for forecast")
        return None
    
    # Get last known timestamp
    last_timestamp = iso_data['timestamp_utc'].max()
    
    # Create features for future
    future = create_features_for_forecast(last_timestamp, iso_code, hours_ahead)
    
    # Historical patterns by hour of day and day of week
    iso_data['hour_of_day'] = iso_data['timestamp_utc'].dt.hour
    iso_data['day_of_week'] = iso_data['timestamp_utc'].dt.dayofweek
    
    # Calculate averages and std for each hour/day combination
    patterns = iso_data.groupby(['hour_of_day', 'day_of_week']).agg({
        'load_mw': ['mean', 'std', 'count']
    }).reset_index()
    
    patterns.columns = ['hour_of_day', 'day_of_week', 'avg_load', 'std_load', 'count']
    
    # Join patterns with future timestamps
    forecast = future.merge(
        patterns,
        on=['hour_of_day', 'day_of_week'],
        how='left'
    )
    
    # Recent trend (last 24 hours vs 24 hours before that)
    recent_24h = iso_data[iso_data['timestamp_utc'] >= (last_timestamp - timedelta(hours=24))]
    older_24h = iso_data[
        (iso_data['timestamp_utc'] >= (last_timestamp - timedelta(hours=48))) &
        (iso_data['timestamp_utc'] < (last_timestamp - timedelta(hours=24)))
    ]
    
    if len(recent_24h) > 0 and len(older_24h) > 0:
        recent_avg = recent_24h['load_mw'].mean()
        older_avg = older_24h['load_mw'].mean()
        trend_factor = recent_avg / older_avg if older_avg > 0 else 1.0
    else:
        trend_factor = 1.0
    
    # Apply trend to forecast
    forecast['forecasted_load_mw'] = forecast['avg_load'] * trend_factor
    
    # Calculate confidence intervals (95%)
    forecast['ci_lower'] = forecast['forecasted_load_mw'] - 1.96 * forecast['std_load']
    forecast['ci_upper'] = forecast['forecasted_load_mw'] + 1.96 * forecast['std_load']
    
    # Fill missing values with overall ISO average
    overall_avg = iso_data['load_mw'].mean()
    forecast['forecasted_load_mw'].fillna(overall_avg, inplace=True)
    forecast['ci_lower'].fillna(overall_avg * 0.9, inplace=True)
    forecast['ci_upper'].fillna(overall_avg * 1.1, inplace=True)
    
    # Add metadata
    forecast['forecast_generated_at'] = datetime.now(timezone.utc)
    forecast['model_version'] = 'simple_v1'
    forecast['confidence'] = 0.95
    
    # Select final columns
    result = forecast[[
        'iso_code', 'timestamp_utc', 'forecasted_load_mw',
        'ci_lower', 'ci_upper', 'forecast_generated_at',
        'model_version', 'confidence'
    ]].copy()
    
    return result


def run_forecasts(historical_df, hours_ahead=24):
    """Generate forecasts for all ISOs."""
    logging.info("=" * 60)
    logging.info(f"🔮 Generating {hours_ahead}-hour forecasts for all ISOs")
    logging.info("=" * 60)
    
    all_forecasts = []
    isos = historical_df['iso_code'].unique()
    
    for iso in isos:
        logging.info(f"\n📊 Forecasting for {iso}...")
        forecast = simple_forecast_model(historical_df, iso, hours_ahead)
        
        if forecast is not None:
            all_forecasts.append(forecast)
            logging.info(f"✅ {iso}: Generated {len(forecast)} hourly forecasts")
            
            # Show first few predictions
            sample = forecast.head(3)[['timestamp_utc', 'forecasted_load_mw', 'ci_lower', 'ci_upper']]
            print(sample.to_string(index=False))
        else:
            logging.warning(f"⚠️  {iso}: Forecast failed")
    
    if not all_forecasts:
        logging.error("❌ No forecasts generated!")
        return None
    
    return pd.concat(all_forecasts, ignore_index=True)


def save_forecasts(forecasts_df):
    """Save forecasts as parquet files partitioned by ISO."""
    logging.info("\n💾 Saving forecasts...")
    
    # Save by ISO
    for iso in forecasts_df['iso_code'].unique():
        iso_forecast = forecasts_df[forecasts_df['iso_code'] == iso]
        output_path = PREDICTIONS_OUTPUT / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"forecast_{timestamp_str}.parquet"
        
        iso_forecast.to_parquet(output_file, index=False)
        logging.info(f"✅ {iso}: Saved forecast → {output_file}")
    
    # Save consolidated latest forecast
    latest_file = PREDICTIONS_OUTPUT / "latest_forecast.parquet"
    forecasts_df.to_parquet(latest_file, index=False)
    logging.info(f"📁 Saved consolidated forecast → {latest_file}")


def print_summary(forecasts_df):
    """Print forecast summary."""
    logging.info("\n" + "=" * 60)
    logging.info("📊 FORECAST SUMMARY")
    logging.info("=" * 60)
    
    summary = forecasts_df.groupby('iso_code').agg({
        'forecasted_load_mw': ['mean', 'min', 'max'],
        'timestamp_utc': ['min', 'max']
    }).round(2)
    
    print(summary)
    
    logging.info(f"\n📈 Total forecasts generated: {len(forecasts_df):,}")
    logging.info(f"🔮 Forecast horizon: {forecasts_df['timestamp_utc'].max() - forecasts_df['timestamp_utc'].min()}")
    logging.info("=" * 60)


def main():
    start_time = datetime.now(timezone.utc)
    logging.info("=" * 60)
    logging.info("🚀 ML Load Forecasting Started")
    logging.info(f"⏰ Start time: {start_time.isoformat()}")
    logging.info("=" * 60)
    
    try:
        # Load recent historical data
        historical_df = load_recent_data(hours_back=168)  # 7 days
        
        if historical_df is None or len(historical_df) == 0:
            logging.error("❌ No historical data available")
            return
        
        # Generate forecasts
        forecasts = run_forecasts(historical_df, hours_ahead=24)
        
        if forecasts is None:
            logging.error("❌ Forecast generation failed")
            return
        
        # Save results
        save_forecasts(forecasts)
        
        # Print summary
        print_summary(forecasts)
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        logging.info("\n" + "=" * 60)
        logging.info(f"✅ Forecasting Complete in {duration:.1f}s")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Forecasting failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()