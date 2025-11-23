"""
Anomaly Detection for Grid Load
Detects unusual load patterns that may indicate:
- Equipment failures
- Data quality issues
- Unexpected demand spikes
- Grid emergencies
"""

import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Paths
SILVER_ENRICHED = Path("storage/silver/grid_weather_enriched")
GOLD_HOURLY = Path("storage/gold/hourly_metrics")
ANOMALIES_OUTPUT = Path("storage/ml/anomalies")

# Create output directory
ANOMALIES_OUTPUT.mkdir(parents=True, exist_ok=True)


def load_recent_data(hours_back=168):
    """Load recent enriched data for anomaly detection."""
    logging.info(f"📥 Loading last {hours_back} hours of data...")
    
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
    
    # Filter to recent data
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    cutoff_naive = cutoff.replace(tzinfo=None)
    df = df[df['timestamp_utc'] >= cutoff_naive]
    
    logging.info(f"✅ Loaded {len(df):,} rows")
    return df


def detect_anomalies_iso(iso_data, iso_code):
    """
    Detect anomalies for a single ISO using multiple methods:
    1. Statistical outliers (Z-score)
    2. Isolation Forest (ML-based)
    3. Rate of change anomalies
    """
    if len(iso_data) < 50:
        logging.warning(f"⚠️  {iso_code}: Not enough data ({len(iso_data)} rows)")
        return None
    
    logging.info(f"\n🔍 Analyzing {iso_code}...")
    
    # Add time-based features
    iso_data = iso_data.copy()
    iso_data['hour_of_day'] = iso_data['timestamp_utc'].dt.hour
    iso_data['day_of_week'] = iso_data['timestamp_utc'].dt.dayofweek
    iso_data['is_weekend'] = (iso_data['day_of_week'] >= 5).astype(int)
    
    # Sort by time
    iso_data = iso_data.sort_values('timestamp_utc').reset_index(drop=True)
    
    # Calculate rate of change
    iso_data['load_change'] = iso_data['load_mw'].diff()
    iso_data['load_change_pct'] = iso_data['load_mw'].pct_change() * 100
    
    # Method 1: Statistical Outliers (Z-score > 3)
    load_mean = iso_data['load_mw'].mean()
    load_std = iso_data['load_mw'].std()
    iso_data['z_score'] = np.abs((iso_data['load_mw'] - load_mean) / load_std)
    iso_data['is_statistical_outlier'] = iso_data['z_score'] > 3
    
    # Method 2: Isolation Forest
    features_for_if = iso_data[['load_mw', 'hour_of_day', 'day_of_week']].copy()
    features_for_if = features_for_if.fillna(features_for_if.mean())
    
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features_for_if)
    
    iso_forest = IsolationForest(
        contamination=0.05,  # Expect 5% anomalies
        random_state=42,
        n_estimators=100
    )
    
    iso_data['isolation_score'] = iso_forest.fit_predict(features_scaled)
    iso_data['is_isolation_anomaly'] = iso_data['isolation_score'] == -1
    
    # Method 3: Rapid Change Detection (>20% change in 1 hour)
    iso_data['is_rapid_change'] = np.abs(iso_data['load_change_pct']) > 20
    
    # Method 4: Historical Pattern Deviation
    # Compare current load to typical load at same hour/day
    historical_pattern = iso_data.groupby(['hour_of_day', 'day_of_week'])['load_mw'].agg(['mean', 'std']).reset_index()
    historical_pattern.columns = ['hour_of_day', 'day_of_week', 'expected_load', 'expected_std']
    
    iso_data = iso_data.merge(historical_pattern, on=['hour_of_day', 'day_of_week'], how='left')
    iso_data['deviation_from_expected'] = np.abs(iso_data['load_mw'] - iso_data['expected_load'])
    iso_data['is_pattern_anomaly'] = iso_data['deviation_from_expected'] > (2 * iso_data['expected_std'])
    
    # Combine: Flag as anomaly if detected by 2+ methods
    iso_data['anomaly_count'] = (
        iso_data['is_statistical_outlier'].astype(int) +
        iso_data['is_isolation_anomaly'].astype(int) +
        iso_data['is_rapid_change'].astype(int) +
        iso_data['is_pattern_anomaly'].astype(int)
    )
    
    iso_data['is_anomaly'] = iso_data['anomaly_count'] >= 2
    
    # Anomaly severity (1-5 scale)
    iso_data['severity'] = np.minimum(iso_data['anomaly_count'], 5)
    
    # Categorize anomaly type
    def categorize_anomaly(row):
        if not row['is_anomaly']:
            return None
        if row['is_rapid_change']:
            return 'rapid_change'
        if row['is_statistical_outlier']:
            return 'statistical_outlier'
        if row['is_pattern_anomaly']:
            return 'pattern_deviation'
        return 'general_anomaly'
    
    iso_data['anomaly_type'] = iso_data.apply(categorize_anomaly, axis=1)
    
    # Filter to actual anomalies
    anomalies = iso_data[iso_data['is_anomaly']].copy()
    
    if len(anomalies) == 0:
        logging.info(f"✅ {iso_code}: No anomalies detected (clean data)")
        return None
    
    # Select relevant columns
    result = anomalies[[
        'iso_code', 'timestamp_utc', 'load_mw', 'temperature',
        'anomaly_type', 'severity', 'z_score', 'load_change_pct',
        'deviation_from_expected', 'expected_load'
    ]].copy()
    
    result['detected_at'] = datetime.now(timezone.utc)
    
    logging.info(f"⚠️  {iso_code}: Found {len(result)} anomalies")
    
    # Show top 3 most severe
    if len(result) > 0:
        top_anomalies = result.nlargest(3, 'severity')
        logging.info(f"\n📊 Top anomalies for {iso_code}:")
        for _, row in top_anomalies.iterrows():
            logging.info(
                f"  • {row['timestamp_utc']}: {row['load_mw']:.0f} MW "
                f"(severity {row['severity']}, type: {row['anomaly_type']})"
            )
    
    return result


def run_anomaly_detection(df):
    """Run anomaly detection for all ISOs."""
    logging.info("=" * 60)
    logging.info("🚨 Running Anomaly Detection")
    logging.info("=" * 60)
    
    all_anomalies = []
    isos = df['iso_code'].unique()
    
    for iso in isos:
        iso_data = df[df['iso_code'] == iso].copy()
        anomalies = detect_anomalies_iso(iso_data, iso)
        
        if anomalies is not None:
            all_anomalies.append(anomalies)
    
    if not all_anomalies:
        logging.info("\n✅ No anomalies detected across all ISOs")
        return None
    
    return pd.concat(all_anomalies, ignore_index=True)


def save_anomalies(anomalies_df):
    """Save anomalies to storage."""
    if anomalies_df is None or len(anomalies_df) == 0:
        logging.info("ℹ️  No anomalies to save")
        return
    
    logging.info("\n💾 Saving anomalies...")
    
    # Save by ISO
    for iso in anomalies_df['iso_code'].unique():
        iso_anomalies = anomalies_df[anomalies_df['iso_code'] == iso]
        output_path = ANOMALIES_OUTPUT / f"iso_code={iso}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"anomalies_{timestamp_str}.parquet"
        
        iso_anomalies.to_parquet(output_file, index=False)
        logging.info(f"✅ {iso}: Saved {len(iso_anomalies)} anomalies → {output_file}")
    
    # Save consolidated latest
    latest_file = ANOMALIES_OUTPUT / "latest_anomalies.parquet"
    anomalies_df.to_parquet(latest_file, index=False)
    logging.info(f"📁 Saved consolidated anomalies → {latest_file}")
    
    # Generate alert summary
    generate_alert_summary(anomalies_df)


def generate_alert_summary(anomalies_df):
    """Generate human-readable alert summary."""
    summary_file = ANOMALIES_OUTPUT / "alert_summary.txt"
    
    with open(summary_file, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("GRIDCARE ANOMALY ALERT SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Total Anomalies: {len(anomalies_df)}\n\n")
        
        # Group by ISO
        for iso in anomalies_df['iso_code'].unique():
            iso_anomalies = anomalies_df[anomalies_df['iso_code'] == iso]
            f.write(f"\n{iso}:\n")
            f.write(f"  Anomalies: {len(iso_anomalies)}\n")
            
            # Count by severity
            severity_counts = iso_anomalies['severity'].value_counts().sort_index(ascending=False)
            for sev, count in severity_counts.items():
                f.write(f"    Severity {sev}: {count}\n")
            
            # Most recent critical anomaly
            critical = iso_anomalies[iso_anomalies['severity'] >= 3].sort_values('timestamp_utc', ascending=False)
            if len(critical) > 0:
                recent = critical.iloc[0]
                f.write(f"  Most recent critical:\n")
                f.write(f"    Time: {recent['timestamp_utc']}\n")
                f.write(f"    Load: {recent['load_mw']:.0f} MW\n")
                f.write(f"    Type: {recent['anomaly_type']}\n")
    
    logging.info(f"📝 Alert summary → {summary_file}")


def print_summary(anomalies_df):
    """Print anomaly detection summary."""
    if anomalies_df is None or len(anomalies_df) == 0:
        logging.info("\n" + "=" * 60)
        logging.info("✅ ANOMALY DETECTION COMPLETE: No anomalies found")
        logging.info("=" * 60)
        return
    
    logging.info("\n" + "=" * 60)
    logging.info("📊 ANOMALY DETECTION SUMMARY")
    logging.info("=" * 60)
    
    summary = anomalies_df.groupby(['iso_code', 'anomaly_type']).size().unstack(fill_value=0)
    print(summary)
    
    logging.info(f"\n⚠️  Total anomalies: {len(anomalies_df)}")
    logging.info(f"🔥 Critical (severity ≥3): {len(anomalies_df[anomalies_df['severity'] >= 3])}")
    logging.info("=" * 60)


def main():
    start_time = datetime.now(timezone.utc)
    logging.info("=" * 60)
    logging.info("🚨 Anomaly Detection Started")
    logging.info(f"⏰ Start time: {start_time.isoformat()}")
    logging.info("=" * 60)
    
    try:
        # Load recent data (7 days for better historical context)
        df = load_recent_data(hours_back=168)
        
        if df is None or len(df) == 0:
            logging.error("❌ No data available for anomaly detection")
            return
        
        # Run anomaly detection
        anomalies = run_anomaly_detection(df)
        
        # Save results
        save_anomalies(anomalies)
        
        # Print summary
        print_summary(anomalies)
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        logging.info("\n" + "=" * 60)
        logging.info(f"✅ Anomaly Detection Complete in {duration:.1f}s")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Anomaly detection failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()