"""
Forecast API Endpoint
Serves ML-generated load forecasts
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import pandas as pd

router = APIRouter(prefix="/forecast", tags=["forecast"])

PREDICTIONS_PATH = Path("storage/ml/predictions/latest_forecast.parquet")


def load_latest_forecasts() -> pd.DataFrame:
    """Load the most recent forecast file."""
    if not PREDICTIONS_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail="No forecasts available. Run model_predict.py first."
        )
    
    try:
        df = pd.read_parquet(PREDICTIONS_PATH)
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df['forecast_generated_at'] = pd.to_datetime(df['forecast_generated_at'])
        return df
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error loading forecasts: {str(e)}"
        )


@router.get("/", response_model=Dict[str, Any])
def get_forecast_info():
    """Get forecast service status and metadata."""
    try:
        df = load_latest_forecasts()
        
        return {
            "status": "operational",
            "forecasts_available": len(df),
            "isos": sorted(df['iso_code'].unique().tolist()),
            "forecast_generated_at": df['forecast_generated_at'].iloc[0].isoformat(),
            "forecast_horizon_hours": int((df['timestamp_utc'].max() - df['timestamp_utc'].min()).total_seconds() / 3600),
            "model_version": df['model_version'].iloc[0],
            "confidence_level": float(df['confidence'].iloc[0]),
        }
    except HTTPException:
        return {
            "status": "no_data",
            "message": "No forecasts available. Please run the forecasting pipeline first.",
            "forecasts_available": 0
        }


@router.get("/{iso_code}", response_model=List[Dict[str, Any]])
def get_forecast_by_iso(
    iso_code: str = Query(..., description="ISO code (e.g., CAISO, ISONE, NYISO, MISO, SPP)"),
    hours: Optional[int] = Query(24, ge=1, le=168, description="Number of hours to forecast (1-168)")
):
    """
    Get load forecast for a specific ISO.
    
    Returns predictions for the next N hours including:
    - Forecasted load (MW)
    - Confidence intervals (95%)
    - Timestamp
    - Model metadata
    """
    df = load_latest_forecasts()
    
    iso_upper = iso_code.upper()
    iso_data = df[df['iso_code'] == iso_upper].copy()
    
    if len(iso_data) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No forecast data available for ISO: {iso_code}. Available ISOs: {df['iso_code'].unique().tolist()}"
        )
    
    # Limit to requested hours
    iso_data = iso_data.head(hours)
    
    # Convert to JSON-friendly format
    result = []
    for _, row in iso_data.iterrows():
        result.append({
            "iso_code": row['iso_code'],
            "timestamp_utc": row['timestamp_utc'].isoformat(),
            "forecasted_load_mw": round(float(row['forecasted_load_mw']), 2),
            "confidence_interval": {
                "lower": round(float(row['ci_lower']), 2),
                "upper": round(float(row['ci_upper']), 2),
            },
            "model_version": row['model_version'],
            "forecast_generated_at": row['forecast_generated_at'].isoformat(),
        })
    
    return result


@router.get("/compare/all", response_model=Dict[str, Any])
def compare_all_isos(
    hours: int = Query(1, ge=1, le=24, description="Hours ahead to compare")
):
    """
    Compare forecast across all ISOs for the next N hours.
    Useful for dashboard visualizations.
    """
    df = load_latest_forecasts()
    
    # Group by hour offset
    df['hours_ahead'] = ((df['timestamp_utc'] - df['timestamp_utc'].min()).dt.total_seconds() / 3600).astype(int) + 1
    
    # Filter to requested hours
    df_filtered = df[df['hours_ahead'] <= hours]
    
    # Pivot for comparison
    comparison = []
    for hours_ahead in range(1, hours + 1):
        hour_data = df_filtered[df_filtered['hours_ahead'] == hours_ahead]
        
        if len(hour_data) == 0:
            continue
        
        entry = {
            "hours_ahead": hours_ahead,
            "timestamp_utc": hour_data['timestamp_utc'].iloc[0].isoformat(),
            "forecasts": {}
        }
        
        for _, row in hour_data.iterrows():
            entry["forecasts"][row['iso_code']] = {
                "load_mw": round(float(row['forecasted_load_mw']), 2),
                "ci_lower": round(float(row['ci_lower']), 2),
                "ci_upper": round(float(row['ci_upper']), 2),
            }
        
        comparison.append(entry)
    
    return {
        "forecast_generated_at": df['forecast_generated_at'].iloc[0].isoformat(),
        "hours_requested": hours,
        "comparison": comparison
    }


@router.get("/peak/{iso_code}", response_model=Dict[str, Any])
def get_peak_forecast(iso_code: str):
    """
    Get the predicted peak load and when it will occur for a specific ISO.
    """
    df = load_latest_forecasts()
    
    iso_upper = iso_code.upper()
    iso_data = df[df['iso_code'] == iso_upper]
    
    if len(iso_data) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No forecast data available for ISO: {iso_code}"
        )
    
    # Find peak
    peak_idx = iso_data['forecasted_load_mw'].idxmax()
    peak_row = iso_data.loc[peak_idx]
    
    return {
        "iso_code": iso_upper,
        "peak_load_mw": round(float(peak_row['forecasted_load_mw']), 2),
        "peak_time_utc": peak_row['timestamp_utc'].isoformat(),
        "hours_until_peak": int((peak_row['timestamp_utc'] - datetime.now()).total_seconds() / 3600),
        "confidence_interval": {
            "lower": round(float(peak_row['ci_lower']), 2),
            "upper": round(float(peak_row['ci_upper']), 2),
        },
        "forecast_generated_at": peak_row['forecast_generated_at'].isoformat(),
    }