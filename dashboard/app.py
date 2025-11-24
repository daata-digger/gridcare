import os
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from datetime import datetime, timedelta
import random
import json
import time

# ============================================
# DEMO CONFIGURATION - FIXED DATE
# ============================================
# All timestamps will show November 21, 2025
DEMO_DATE = datetime(2025, 11, 21, 14, 30, 0)
START_TIME = time.time()

def get_demo_time():
    """
    Return demo timestamp that advances in real-time from Nov 21, 2025.
    This ensures all API responses show the demo date, not the actual date.
    """
    elapsed = time.time() - START_TIME
    return DEMO_DATE + timedelta(seconds=elapsed)
# ============================================

app = FastAPI(title="GridCARE Live - Advanced Energy Grid Monitoring v2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics endpoint
@app.get("/metrics")
async def metrics():
    """Prometheus-compatible metrics endpoint"""
    metrics_data = f"""# HELP grid_total_load_mw Total grid load in megawatts
# TYPE grid_total_load_mw gauge
grid_total_load_mw {random.randint(80000, 95000)}

# HELP grid_renewable_generation_mw Renewable energy generation in megawatts
# TYPE grid_renewable_generation_mw gauge
grid_renewable_generation_mw {random.randint(15000, 25000)}

# HELP grid_avg_lmp_price Average LMP price in dollars per MWh
# TYPE grid_avg_lmp_price gauge
grid_avg_lmp_price {random.uniform(35.0, 65.0):.2f}

# HELP grid_carbon_intensity Carbon intensity in lbs CO2 per MWh
# TYPE grid_carbon_intensity gauge
grid_carbon_intensity {random.uniform(800, 900):.2f}

# HELP grid_api_requests_total Total API requests
# TYPE grid_api_requests_total counter
grid_api_requests_total {random.randint(10000, 50000)}

# HELP grid_response_time_seconds API response time in seconds
# TYPE grid_response_time_seconds histogram
grid_response_time_seconds_bucket{{le="0.1"}} {random.randint(1000, 5000)}
grid_response_time_seconds_bucket{{le="0.5"}} {random.randint(5000, 10000)}
grid_response_time_seconds_bucket{{le="1.0"}} {random.randint(10000, 15000)}
grid_response_time_seconds_sum {random.uniform(100, 500):.2f}
grid_response_time_seconds_count {random.randint(15000, 20000)}

# HELP iso_status ISO operational status (1=online, 0=offline)
# TYPE iso_status gauge
iso_status{{iso="CAISO"}} 1
iso_status{{iso="ISONE"}} 1
iso_status{{iso="NYISO"}} 1
iso_status{{iso="MISO"}} 1
iso_status{{iso="SPP"}} 1

# HELP fuel_generation_percentage Fuel mix generation percentage
# TYPE fuel_generation_percentage gauge
fuel_generation_percentage{{type="natural_gas"}} 42.5
fuel_generation_percentage{{type="nuclear"}} 18.2
fuel_generation_percentage{{type="coal"}} 15.8
fuel_generation_percentage{{type="wind"}} 12.3
fuel_generation_percentage{{type="solar"}} 8.1
fuel_generation_percentage{{type="hydro"}} 2.5
fuel_generation_percentage{{type="other"}} 0.6
"""
    return Response(content=metrics_data, media_type="text/plain")

# Serve the main dashboard HTML
@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        with open("index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="""
        <!DOCTYPE html>
        <html>
        <head><title>GridCARE Live</title></head>
        <body style="font-family: sans-serif; padding: 40px; background: #0a0e1a; color: #fff;">
            <h1>⚠️ GridCARE Live Dashboard</h1>
            <p>The index.html file was not found.</p>
            <p><strong>API is running!</strong> Check <a href="/docs" style="color: #3b82f6;">/docs</a></p>
        </body>
        </html>
        """)

@app.get("/health")
async def health():
    current_time = get_demo_time()
    uptime_seconds = int(time.time() - START_TIME)
    return {
        "status": "ok",
        "api": "running",
        "timestamp": current_time.isoformat(),
        "version": "2.0",
        "uptime_seconds": uptime_seconds
    }

@app.get("/api/grid/summary")
async def grid_summary():
    """Get grid summary with mock data"""
    current_time = get_demo_time()
    total_load = random.randint(80000, 95000)
    renewables = total_load * random.uniform(0.18, 0.25)
    
    return {
        "total_load_mw": total_load,
        "renewables_mw": renewables,
        "avg_price": random.uniform(35.0, 65.0),
        "iso_count": 5,
        "timestamp": current_time.isoformat(),
        "status": "success"
    }

@app.get("/api/grid/hourly")
async def grid_hourly(iso: str = "CAISO", limit: int = 24):
    """Get hourly load data"""
    current_time = get_demo_time()
    hourly_data = []
    base_load = random.randint(15000, 25000)
    
    for i in range(limit):
        hour_time = current_time - timedelta(hours=limit - i - 1)
        load_variation = random.uniform(0.85, 1.15)
        hourly_data.append({
            "hour": hour_time.isoformat(),
            "avg_load_mw": base_load * load_variation,
            "iso": iso
        })
    
    return hourly_data

@app.get("/api/grid/fuel-mix")
async def fuel_mix(iso: str = "ALL"):
    """Get fuel mix breakdown"""
    current_time = get_demo_time()
    fuel_types = {
        "Natural Gas": 42.5,
        "Nuclear": 18.2,
        "Coal": 15.8,
        "Wind": 12.3,
        "Solar": 8.1,
        "Hydro": 2.5,
        "Other": 0.6
    }
    return {"iso": iso, "fuel_mix": fuel_types, "timestamp": current_time.isoformat()}

@app.get("/api/grid/forecast")
async def load_forecast(iso: str = "ALL", hours: int = 24):
    """Get load forecast"""
    current_time = get_demo_time()
    forecast_data = []
    base_load = random.randint(82000, 92000)
    
    for i in range(hours):
        timestamp = current_time + timedelta(hours=i)
        hour_variation = 1.0 + (0.1 * random.random() - 0.05)
        forecasted = base_load * hour_variation
        
        forecast_data.append({
            "timestamp": timestamp.isoformat(),
            "forecasted_load_mw": forecasted,
            "confidence_upper": forecasted * 1.05,
            "confidence_lower": forecasted * 0.95
        })
    
    return {"iso": iso, "forecast": forecast_data}

@app.get("/api/grid/interchange")
async def grid_interchange():
    """Get inter-ISO power interchange"""
    current_time = get_demo_time()
    exchanges = [
        {"from_iso": "CAISO", "to_iso": "WECC", "mw": random.randint(1000, 1500), "direction": "export"},
        {"from_iso": "MISO", "to_iso": "PJM", "mw": random.randint(700, 1000), "direction": "export"},
        {"from_iso": "NYISO", "to_iso": "ISONE", "mw": random.randint(250, 400), "direction": "import"},
        {"from_iso": "SPP", "to_iso": "MISO", "mw": random.randint(400, 700), "direction": "export"},
    ]
    return {"exchanges": exchanges, "timestamp": current_time.isoformat()}

@app.get("/api/grid/alerts")
async def grid_alerts():
    """Get current grid alerts"""
    current_time = get_demo_time()
    return {"active_alerts": [], "alert_count": 0, "last_check": current_time.isoformat()}

@app.get("/api/grid/emissions")
async def carbon_emissions(iso: str = "ALL"):
    """Get carbon emissions intensity"""
    current_time = get_demo_time()
    base_intensity = random.uniform(800, 900)
    return {
        "iso": iso,
        "carbon_intensity": base_intensity,
        "unit": "lbs CO2/MWh",
        "timestamp": current_time.isoformat(),
        "24h_average": base_intensity * 1.05,
        "trend": "decreasing" if random.random() > 0.5 else "increasing"
    }

# NEW ENDPOINTS FOR ENHANCED FEATURES

@app.get("/api/ml/load-prediction")
async def ml_load_prediction(iso: str = "ALL", horizon: int = 168):
    """ML-based load prediction (7 days default)"""
    current_time = get_demo_time()
    predictions = []
    base_load = random.randint(80000, 95000)
    
    for i in range(horizon):
        timestamp = current_time + timedelta(hours=i)
        # Simulate ML prediction with daily pattern
        hour_of_day = timestamp.hour
        day_pattern = 1.0 + 0.2 * (hour_of_day - 12) / 12
        prediction = base_load * day_pattern * random.uniform(0.95, 1.05)
        
        predictions.append({
            "timestamp": timestamp.isoformat(),
            "predicted_load_mw": prediction,
            "confidence": random.uniform(0.85, 0.98),
            "model": "LSTM-v2",
            "features_used": ["historical_load", "weather", "day_of_week", "hour"]
        })
    
    return {
        "iso": iso,
        "predictions": predictions,
        "model_accuracy": 0.94,
        "last_trained": (current_time - timedelta(days=1)).isoformat()
    }

@app.get("/api/ml/anomaly-detection")
async def anomaly_detection():
    """Real-time anomaly detection results"""
    current_time = get_demo_time()
    anomalies = []
    
    # Simulate occasional anomalies
    if random.random() > 0.7:
        anomalies.append({
            "timestamp": current_time.isoformat(),
            "iso": random.choice(["CAISO", "ISONE", "NYISO", "MISO", "SPP"]),
            "metric": random.choice(["load_spike", "price_spike", "frequency_deviation"]),
            "severity": random.choice(["low", "medium", "high"]),
            "value": random.uniform(1000, 5000),
            "threshold": random.uniform(800, 4000),
            "confidence": random.uniform(0.8, 0.99)
        })
    
    return {
        "anomalies": anomalies,
        "total_detected": len(anomalies),
        "model": "Isolation Forest + LSTM Autoencoder",
        "timestamp": current_time.isoformat()
    }

@app.get("/api/ml/renewable-forecast")
async def renewable_forecast(hours: int = 48):
    """ML forecast for renewable generation"""
    current_time = get_demo_time()
    forecasts = []
    
    for i in range(hours):
        timestamp = current_time + timedelta(hours=i)
        hour = timestamp.hour
        
        # Solar peaks during day
        solar = max(0, 5000 * (1 - abs(hour - 12) / 12) * random.uniform(0.8, 1.2))
        
        # Wind more variable
        wind = random.uniform(3000, 8000)
        
        forecasts.append({
            "timestamp": timestamp.isoformat(),
            "solar_mw": solar,
            "wind_mw": wind,
            "total_renewable_mw": solar + wind,
            "confidence": random.uniform(0.75, 0.95)
        })
    
    return {
        "forecasts": forecasts,
        "model": "GRU + Weather Integration",
        "features": ["solar_irradiance", "wind_speed", "temperature", "cloud_cover"]
    }

@app.get("/api/grid/detailed-metrics")
async def detailed_metrics():
    """Comprehensive detailed metrics for all ISOs"""
    current_time = get_demo_time()
    isos = ["CAISO", "ISONE", "NYISO", "MISO", "SPP"]
    detailed_data = []
    
    for iso in isos:
        base_load = random.randint(15000, 25000)
        detailed_data.append({
            "iso": iso,
            "timestamp": current_time.isoformat(),
            "load": {
                "current_mw": base_load,
                "forecast_1h": base_load * random.uniform(0.98, 1.02),
                "forecast_24h": base_load * random.uniform(0.95, 1.05),
                "peak_today": base_load * random.uniform(1.1, 1.3),
                "min_today": base_load * random.uniform(0.7, 0.9)
            },
            "pricing": {
                "avg_lmp": random.uniform(30, 70),
                "min_lmp": random.uniform(20, 40),
                "max_lmp": random.uniform(60, 120),
                "hub_price": random.uniform(35, 65),
                "congestion_cost": random.uniform(0, 15)
            },
            "generation": {
                "total_mw": base_load * random.uniform(1.05, 1.15),
                "available_capacity_mw": base_load * random.uniform(1.2, 1.5),
                "reserve_margin_pct": random.uniform(10, 25),
                "renewable_mw": base_load * random.uniform(0.15, 0.30),
                "renewable_pct": random.uniform(15, 30)
            },
            "reliability": {
                "frequency_hz": random.uniform(59.98, 60.02),
                "voltage_stability": random.uniform(0.95, 1.05),
                "n1_contingency_status": "secure",
                "reserve_shortfall_mw": 0
            },
            "transmission": {
                "total_flows_mw": random.randint(5000, 15000),
                "congested_lines": random.randint(0, 3),
                "outages": random.randint(0, 2),
                "interface_limits_hit": random.randint(0, 1)
            }
        })
    
    return {"data": detailed_data, "timestamp": current_time.isoformat()}

@app.get("/api/grid/capacity-factors")
async def capacity_factors():
    """Real-time capacity factors for different generation types"""
    current_time = get_demo_time()
    return {
        "timestamp": current_time.isoformat(),
        "capacity_factors": {
            "nuclear": random.uniform(0.90, 0.95),
            "coal": random.uniform(0.40, 0.60),
            "natural_gas": random.uniform(0.50, 0.70),
            "wind": random.uniform(0.25, 0.45),
            "solar": random.uniform(0.15, 0.30),
            "hydro": random.uniform(0.35, 0.55),
            "battery": random.uniform(0.10, 0.25)
        },
        "utilization_rates": {
            "nuclear": random.uniform(0.92, 0.98),
            "coal": random.uniform(0.45, 0.65),
            "natural_gas_combined_cycle": random.uniform(0.55, 0.75),
            "natural_gas_peaker": random.uniform(0.10, 0.30),
            "wind": random.uniform(0.30, 0.50),
            "solar": random.uniform(0.20, 0.35)
        }
    }

@app.get("/api/grid/market-prices")
async def market_prices():
    """Detailed market pricing across nodes and zones"""
    current_time = get_demo_time()
    prices = []
    
    zones = ["North", "South", "East", "West", "Central"]
    isos = ["CAISO", "ISONE", "NYISO", "MISO", "SPP"]
    
    for iso in isos:
        for zone in zones[:random.randint(3, 5)]:
            prices.append({
                "iso": iso,
                "zone": zone,
                "timestamp": current_time.isoformat(),
                "lmp": random.uniform(30, 80),
                "energy_component": random.uniform(25, 65),
                "congestion_component": random.uniform(-5, 15),
                "loss_component": random.uniform(0, 5),
                "node_count": random.randint(50, 500)
            })
    
    return {"prices": prices, "timestamp": current_time.isoformat()}

@app.get("/api/monitoring/system-health")
async def system_health():
    """System monitoring and health metrics"""
    current_time = get_demo_time()
    actual_uptime = int(time.time() - START_TIME)
    
    return {
        "timestamp": current_time.isoformat(),
        "api_health": {
            "status": "healthy",
            "uptime_seconds": actual_uptime,
            "uptime_display": f"{actual_uptime // 3600}h {(actual_uptime % 3600) // 60}m",
            "requests_per_second": random.uniform(10, 50),
            "avg_response_time_ms": random.uniform(50, 200),
            "error_rate": random.uniform(0, 0.02)
        },
        "database_health": {
            "status": "healthy",
            "connections_active": random.randint(5, 20),
            "connections_max": 100,
            "query_avg_time_ms": random.uniform(10, 50),
            "slow_queries": random.randint(0, 2)
        },
        "cache_health": {
            "status": "healthy",
            "hit_rate": random.uniform(0.85, 0.98),
            "memory_used_mb": random.randint(100, 500),
            "memory_max_mb": 1024,
            "evictions_per_min": random.uniform(0, 5)
        },
        "data_freshness": {
            "last_grid_update": (current_time - timedelta(seconds=random.randint(10, 60))).isoformat(),
            "last_price_update": (current_time - timedelta(seconds=random.randint(10, 60))).isoformat(),
            "last_forecast_update": (current_time - timedelta(minutes=random.randint(5, 30))).isoformat()
        }
    }

@app.get("/api/pipeline/status")
async def pipeline_status():
    """Data pipeline status - Bronze → Silver → Gold"""
    current_time = get_demo_time()
    actual_uptime = int(time.time() - START_TIME)
    
    return {
        "timestamp": current_time.isoformat(),
        "pipeline": {
            "bronze": {
                "status": "active",
                "tier": "bronze",
                "description": "Raw data ingestion from ISO APIs",
                "throughput_records_per_sec": 2845,
                "total_records_today": random.randint(50000, 150000),
                "latency_ms": 23,
                "error_rate": 0.002,
                "sources_active": 5,
                "sources_total": 5,
                "uptime_seconds": actual_uptime,
                "last_update": (current_time - timedelta(seconds=random.randint(1, 10))).isoformat()
            },
            "silver": {
                "status": "active",
                "tier": "silver",
                "description": "Data validation, cleaning & transformation",
                "throughput_records_per_sec": 2789,
                "total_records_today": random.randint(45000, 140000),
                "latency_ms": 45,
                "error_rate": 0.015,
                "validation_rules_passed": 98.5,
                "records_cleaned": 156,
                "uptime_seconds": actual_uptime,
                "last_update": (current_time - timedelta(seconds=random.randint(1, 10))).isoformat()
            },
            "gold": {
                "status": "active",
                "tier": "gold",
                "description": "ML predictions & advanced analytics",
                "throughput_records_per_sec": 2734,
                "total_records_today": random.randint(40000, 130000),
                "latency_ms": 67,
                "error_rate": 0.003,
                "ml_models_active": 4,
                "predictions_generated": 1234,
                "avg_model_accuracy": 92.8,
                "uptime_seconds": actual_uptime,
                "last_update": (current_time - timedelta(seconds=random.randint(1, 10))).isoformat()
            }
        },
        "overall": {
            "status": "healthy",
            "end_to_end_latency_ms": 135,
            "data_quality_score": random.uniform(0.94, 0.99),
            "uptime_percentage": 99.87
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)