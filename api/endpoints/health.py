"""
Health Check Endpoints
Provides system health status for monitoring and orchestration
"""

from fastapi import APIRouter, Request, HTTPException
from typing import Dict, Any
from datetime import datetime, timezone
from pathlib import Path
import psutil
import os

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/")
async def health_check() -> Dict[str, str]:
    """
    Basic health check - returns OK if service is running.
    Used by load balancers and Docker health checks.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "gridcare-api"
    }


@router.get("/ready")
async def readiness_check(request: Request) -> Dict[str, Any]:
    """
    Readiness check - returns OK if service can handle requests.
    Checks database connectivity and data availability.
    """
    checks = {
        "database": "unknown",
        "data": "unknown",
        "ml_models": "unknown"
    }
    
    # Check database connection
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {str(e)}"
    
    # Check if data exists
    try:
        silver_path = Path("storage/silver/grid_weather_enriched")
        has_data = silver_path.exists() and any(silver_path.rglob("*.parquet"))
        checks["data"] = "ok" if has_data else "no_data"
    except Exception as e:
        checks["data"] = f"error: {str(e)}"
    
    # Check if ML models/predictions exist
    try:
        predictions_path = Path("storage/ml/predictions/latest_forecast.parquet")
        checks["ml_models"] = "ok" if predictions_path.exists() else "no_predictions"
    except Exception as e:
        checks["ml_models"] = f"error: {str(e)}"
    
    # Overall status
    all_ok = all(v == "ok" for v in checks.values())
    status = "ready" if all_ok else "not_ready"
    
    return {
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": checks
    }


@router.get("/live")
async def liveness_check() -> Dict[str, str]:
    """
    Liveness check - returns OK if service should continue running.
    Used by Kubernetes to determine if pod should be restarted.
    """
    return {
        "status": "alive",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@router.get("/metrics/system")
async def system_metrics() -> Dict[str, Any]:
    """
    System resource metrics.
    Returns CPU, memory, and disk usage.
    """
    try:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu": {
                "percent": psutil.cpu_percent(interval=1),
                "count": psutil.cpu_count()
            },
            "memory": {
                "total": psutil.virtual_memory().total,
                "available": psutil.virtual_memory().available,
                "percent": psutil.virtual_memory().percent
            },
            "disk": {
                "total": psutil.disk_usage('/').total,
                "used": psutil.disk_usage('/').used,
                "free": psutil.disk_usage('/').free,
                "percent": psutil.disk_usage('/').percent
            },
            "process": {
                "pid": os.getpid(),
                "memory_mb": psutil.Process().memory_info().rss / 1024 / 1024,
                "threads": psutil.Process().num_threads()
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get system metrics: {str(e)}")


@router.get("/status")
async def detailed_status(request: Request) -> Dict[str, Any]:
    """
    Detailed service status including data freshness and pipeline health.
    """
    status = {
        "service": "gridcare-api",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": None,  # TODO: Track startup time
        "components": {}
    }
    
    # Check database
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        status["components"]["database"] = {"status": "healthy"}
    except Exception as e:
        status["components"]["database"] = {"status": "unhealthy", "error": str(e)}
    
    # Check data freshness
    try:
        silver_path = Path("storage/silver/grid_weather_enriched")
        if silver_path.exists():
            parquet_files = list(silver_path.rglob("*.parquet"))
            if parquet_files:
                latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
                age_hours = (datetime.now().timestamp() - latest_file.stat().st_mtime) / 3600
                status["components"]["data"] = {
                    "status": "healthy" if age_hours < 2 else "stale",
                    "latest_update_hours_ago": round(age_hours, 1),
                    "total_files": len(parquet_files)
                }
            else:
                status["components"]["data"] = {"status": "no_data"}
        else:
            status["components"]["data"] = {"status": "not_found"}
    except Exception as e:
        status["components"]["data"] = {"status": "error", "error": str(e)}
    
    # Check ML predictions
    try:
        predictions_path = Path("storage/ml/predictions/latest_forecast.parquet")
        if predictions_path.exists():
            age_hours = (datetime.now().timestamp() - predictions_path.stat().st_mtime) / 3600
            status["components"]["ml_predictions"] = {
                "status": "healthy" if age_hours < 24 else "stale",
                "latest_update_hours_ago": round(age_hours, 1)
            }
        else:
            status["components"]["ml_predictions"] = {"status": "not_found"}
    except Exception as e:
        status["components"]["ml_predictions"] = {"status": "error", "error": str(e)}
    
    # Check anomalies
    try:
        anomalies_path = Path("storage/ml/anomalies/latest_anomalies.parquet")
        if anomalies_path.exists():
            import pandas as pd
            df = pd.read_parquet(anomalies_path)
            critical_count = len(df[df['severity'] >= 3])
            status["components"]["anomaly_detection"] = {
                "status": "healthy",
                "total_anomalies": len(df),
                "critical_anomalies": critical_count
            }
        else:
            status["components"]["anomaly_detection"] = {"status": "not_run"}
    except Exception as e:
        status["components"]["anomaly_detection"] = {"status": "error", "error": str(e)}
    
    # Overall health
    component_statuses = [c.get("status") for c in status["components"].values()]
    if all(s == "healthy" for s in component_statuses):
        status["overall_status"] = "healthy"
    elif any(s == "unhealthy" for s in component_statuses):
        status["overall_status"] = "degraded"
    else:
        status["overall_status"] = "operational"
    
    return status