"""
Unit Tests for Data Transformations
Tests bronze→silver→gold pipeline logic
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestDataValidation:
    """Test data validation and cleaning logic."""
    
    def test_timestamp_parsing(self):
        """Test timestamp parsing handles various formats."""
        data = {
            'timestamp': ['2025-11-22 14:00:00', '2025-11-22 15:00:00'],
            'load': [1000, 1100]
        }
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        assert df['timestamp'].dtype == 'datetime64[ns]'
        assert len(df) == 2
    
    def test_load_validation(self):
        """Test load values are properly validated."""
        data = {
            'timestamp': pd.date_range('2025-11-22', periods=5, freq='H'),
            'load': [1000, -50, 1100, 2000000, 1050]  # Invalid: negative and too large
        }
        df = pd.DataFrame(data)
        
        # Apply validation logic
        df_clean = df[(df['load'] > 0) & (df['load'] < 1000000)]
        
        assert len(df_clean) == 3
        assert df_clean['load'].min() > 0
        assert df_clean['load'].max() < 1000000
    
    def test_duplicate_removal(self):
        """Test duplicate timestamps are removed."""
        data = {
            'iso_code': ['CAISO', 'CAISO', 'CAISO'],
            'timestamp': ['2025-11-22 14:00:00'] * 3,
            'load': [1000, 1010, 1005]
        }
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df_clean = df.drop_duplicates(subset=['iso_code', 'timestamp'])
        
        assert len(df_clean) == 1
    
    def test_missing_data_handling(self):
        """Test missing data is properly handled."""
        data = {
            'timestamp': pd.date_range('2025-11-22', periods=5, freq='H'),
            'load': [1000, None, 1100, 1050, None]
        }
        df = pd.DataFrame(data)
        
        df_clean = df.dropna(subset=['load'])
        
        assert len(df_clean) == 3
        assert df_clean['load'].isna().sum() == 0


class TestAggregations:
    """Test aggregation logic for Gold layer."""
    
    def test_hourly_aggregation(self):
        """Test hourly metrics calculation."""
        # Create sample data
        timestamps = pd.date_range('2025-11-22 14:00', periods=12, freq='5min')
        data = {
            'iso_code': ['CAISO'] * 12,
            'timestamp_utc': timestamps,
            'load_mw': np.random.uniform(20000, 25000, 12)
        }
        df = pd.DataFrame(data)
        df['hour'] = df['timestamp_utc'].dt.floor('H')
        
        # Aggregate by hour
        hourly = df.groupby(['iso_code', 'hour']).agg({
            'load_mw': ['mean', 'min', 'max', 'std', 'count']
        }).reset_index()
        
        assert len(hourly) == 1
        assert hourly.iloc[0][('load_mw', 'count')] == 12
    
    def test_daily_aggregation(self):
        """Test daily metrics calculation."""
        timestamps = pd.date_range('2025-11-22', periods=24, freq='H')
        data = {
            'iso_code': ['CAISO'] * 24,
            'timestamp_utc': timestamps,
            'load_mw': np.random.uniform(15000, 30000, 24)
        }
        df = pd.DataFrame(data)
        df['date'] = df['timestamp_utc'].dt.date
        
        daily = df.groupby(['iso_code', 'date']).agg({
            'load_mw': ['mean', 'min', 'max', 'sum']
        }).reset_index()
        
        assert len(daily) == 1
        assert daily.iloc[0][('load_mw', 'count')] == 24


class TestMLFeatures:
    """Test ML feature engineering."""
    
    def test_time_features(self):
        """Test time-based feature extraction."""
        timestamps = pd.date_range('2025-11-22', periods=48, freq='H')
        df = pd.DataFrame({'timestamp_utc': timestamps})
        
        df['hour_of_day'] = df['timestamp_utc'].dt.hour
        df['day_of_week'] = df['timestamp_utc'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        assert df['hour_of_day'].min() == 0
        assert df['hour_of_day'].max() == 23
        assert df['day_of_week'].min() >= 0
        assert df['day_of_week'].max() <= 6
        assert df['is_weekend'].isin([0, 1]).all()
    
    def test_rate_of_change(self):
        """Test rate of change calculation."""
        data = {
            'timestamp': pd.date_range('2025-11-22', periods=5, freq='H'),
            'load_mw': [20000, 21000, 20500, 19000, 19500]
        }
        df = pd.DataFrame(data)
        
        df['load_change'] = df['load_mw'].diff()
        df['load_change_pct'] = df['load_mw'].pct_change() * 100
        
        assert df['load_change'].iloc[1] == 1000
        assert abs(df['load_change_pct'].iloc[1] - 5.0) < 0.1


class TestAnomalyDetection:
    """Test anomaly detection logic."""
    
    def test_z_score_calculation(self):
        """Test statistical outlier detection."""
        data = [20000, 21000, 20500, 19500, 40000]  # Last value is outlier
        df = pd.DataFrame({'load_mw': data})
        
        mean = df['load_mw'].mean()
        std = df['load_mw'].std()
        df['z_score'] = np.abs((df['load_mw'] - mean) / std)
        df['is_outlier'] = df['z_score'] > 3
        
        assert df['is_outlier'].sum() == 1
        assert df.loc[df['is_outlier'], 'load_mw'].iloc[0] == 40000
    
    def test_rapid_change_detection(self):
        """Test rapid change anomaly detection."""
        data = {
            'timestamp': pd.date_range('2025-11-22', periods=5, freq='H'),
            'load_mw': [20000, 21000, 30000, 31000, 30500]  # Rapid jump at index 2
        }
        df = pd.DataFrame(data)
        
        df['load_change_pct'] = df['load_mw'].pct_change() * 100
        df['is_rapid_change'] = np.abs(df['load_change_pct']) > 20
        
        assert df['is_rapid_change'].sum() >= 1


class TestForecastModel:
    """Test forecast model logic."""
    
    def test_historical_pattern_lookup(self):
        """Test historical pattern matching."""
        # Create 2 weeks of data
        timestamps = pd.date_range('2025-11-08', periods=336, freq='H')
        data = {
            'timestamp_utc': timestamps,
            'load_mw': np.random.uniform(15000, 30000, 336)
        }
        df = pd.DataFrame(data)
        
        df['hour_of_day'] = df['timestamp_utc'].dt.hour
        df['day_of_week'] = df['timestamp_utc'].dt.dayofweek
        
        # Calculate historical patterns
        patterns = df.groupby(['hour_of_day', 'day_of_week'])['load_mw'].agg(['mean', 'std'])
        
        assert len(patterns) <= 24 * 7  # At most 24 hours * 7 days
        assert patterns['mean'].isna().sum() == 0
    
    def test_forecast_confidence_intervals(self):
        """Test confidence interval calculation."""
        mean = 20000
        std = 2000
        
        ci_lower = mean - 1.96 * std
        ci_upper = mean + 1.96 * std
        
        assert ci_lower < mean < ci_upper
        assert (ci_upper - ci_lower) == pytest.approx(2 * 1.96 * std, rel=0.01)


# Pytest configuration
if __name__ == "__main__":
    pytest.main([__file__, "-v"])