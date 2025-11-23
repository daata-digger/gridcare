import os
import asyncpg
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from middleware.metrics import PrometheusMiddleware, metrics_endpoint
from endpoints import grid, summary, forecast, health

def _env(*keys: str, default=None):
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            return v
    return default

PG_HOST = _env("PG_HOST", "POSTGRES_HOST", default="db")
PG_PORT = int(_env("PG_PORT", "POSTGRES_PORT", default="5432"))
PG_DB   = _env("PG_DB", "POSTGRES_DB", default="postgres")
PG_USER = _env("PG_USER", "POSTGRES_USER", default="postgres")
PG_PASS = _env("PG_PASS", "POSTGRES_PASSWORD", default="postgres")

app = FastAPI(
    title="GridCARE Energy API",
    version="1.0.0",
    description="Real-time grid monitoring, forecasting, and anomaly detection",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus Metrics Middleware
app.add_middleware(PrometheusMiddleware)

# Register routers
app.include_router(grid.router, tags=["grid"])
app.include_router(summary.router, tags=["summary"])
app.include_router(forecast.router, tags=["forecast"])
app.include_router(health.router, tags=["health"])

# Metrics endpoint
app.add_route("/metrics", metrics_endpoint)

@app.on_event("startup")
async def startup():
    """Initialize database connection pool on startup."""
    app.state.pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB,
        min_size=1, max_size=10, command_timeout=30,
    )
    print(f"✅ Database pool created: {PG_HOST}:{PG_PORT}/{PG_DB}")

@app.on_event("shutdown")
async def shutdown():
    """Close database connection pool on shutdown."""
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()
        print("✅ Database pool closed")

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "GridCARE Energy Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics"
    }

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)