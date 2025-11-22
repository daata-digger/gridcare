from fastapi import APIRouter

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/")
def get_forecast():
    # Placeholder, will later be wired to ML model + Postgres table
    return {
        "message": "Forecast service online",
        "sample": {
            "prediction_timestamp": "2025-01-01T00:00:00Z",
            "forecasted_load": 12345.6,
            "confidence_interval_low": 11000.0,
            "confidence_interval_high": 13500.0,
        },
    }
