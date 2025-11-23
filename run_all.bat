@echo off
REM ============================================================================
REM GridCARE Complete System Startup Script
REM ============================================================================
REM This script starts all GridCARE services including:
REM - Dashboard (Frontend + Backend)
REM - Prometheus Monitoring
REM - Airflow Data Pipeline
REM - PostgreSQL Database
REM - Redis Cache
REM ============================================================================

COLOR 0A
echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║           GridCARE Complete System Startup v2.0               ║
echo ║                                                                ║
echo ║  Starting all services:                                        ║
echo ║  - Dashboard                                                   ║
echo ║  - Prometheus Monitoring                                       ║
echo ║  - Airflow Pipeline                                            ║
echo ║  - Database & Cache                                            ║
echo ╚════════════════════════════════════════════════════════════════╝
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running!
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo [✓] Docker is running
echo.

REM ============================================================================
REM SECTION 1: Start PostgreSQL Database
REM ============================================================================
echo ┌────────────────────────────────────────────────────────────────┐
echo │ 1/5 Starting PostgreSQL Database...                            │
echo └────────────────────────────────────────────────────────────────┘

docker ps -a | findstr gridcare-postgres >nul
if errorlevel 1 (
    echo Creating new PostgreSQL container...
    docker run -d ^
        --name gridcare-postgres ^
        -e POSTGRES_USER=gridcare ^
        -e POSTGRES_PASSWORD=gridcare123 ^
        -e POSTGRES_DB=gridcare ^
        -p 5432:5432 ^
        --restart unless-stopped ^
        postgres:14-alpine
) else (
    echo Starting existing PostgreSQL container...
    docker start gridcare-postgres
)

timeout /t 3 /nobreak >nul
echo [✓] PostgreSQL started on port 5432
echo.

REM ============================================================================
REM SECTION 2: Start Redis Cache
REM ============================================================================
echo ┌────────────────────────────────────────────────────────────────┐
echo │ 2/5 Starting Redis Cache...                                    │
echo └────────────────────────────────────────────────────────────────┘

docker ps -a | findstr gridcare-redis >nul
if errorlevel 1 (
    echo Creating new Redis container...
    docker run -d ^
        --name gridcare-redis ^
        -p 6379:6379 ^
        --restart unless-stopped ^
        redis:7-alpine
) else (
    echo Starting existing Redis container...
    docker start gridcare-redis
)

timeout /t 2 /nobreak >nul
echo [✓] Redis started on port 6379
echo.

REM ============================================================================
REM SECTION 3: Start Prometheus Monitoring
REM ============================================================================
echo ┌────────────────────────────────────────────────────────────────┐
echo │ 3/5 Starting Prometheus Monitoring...                          │
echo └────────────────────────────────────────────────────────────────┘

REM Create Prometheus config if it doesn't exist
if not exist prometheus.yml (
    echo Creating Prometheus configuration...
    (
        echo global:
        echo   scrape_interval: 15s
        echo   evaluation_interval: 15s
        echo.
        echo scrape_configs:
        echo   - job_name: 'gridcare-dashboard'
        echo     static_configs:
        echo       - targets: ['host.docker.internal:8080']
        echo         labels:
        echo           service: 'dashboard'
        echo           tier: 'frontend'
        echo.
        echo   - job_name: 'gridcare-pipeline'
        echo     static_configs:
        echo       - targets: ['host.docker.internal:8080']
        echo         labels:
        echo           service: 'pipeline'
        echo           tier: 'backend'
        echo.
        echo   - job_name: 'airflow'
        echo     static_configs:
        echo       - targets: ['host.docker.internal:8080']
        echo         labels:
        echo           service: 'airflow'
        echo           tier: 'orchestration'
    ) > prometheus.yml
)

docker ps -a | findstr gridcare-prometheus >nul
if errorlevel 1 (
    echo Creating new Prometheus container...
    docker run -d ^
        --name gridcare-prometheus ^
        -p 9090:9090 ^
        -v "%cd%\prometheus.yml:/etc/prometheus/prometheus.yml" ^
        --restart unless-stopped ^
        prom/prometheus:latest
) else (
    echo Starting existing Prometheus container...
    docker start gridcare-prometheus
)

timeout /t 3 /nobreak >nul
echo [✓] Prometheus started on port 9090
echo     └─ Access: http://localhost:9090
echo.

REM ============================================================================
REM SECTION 4: Start Airflow Data Pipeline
REM ============================================================================
echo ┌────────────────────────────────────────────────────────────────┐
echo │ 4/5 Starting Apache Airflow Pipeline...                        │
echo └────────────────────────────────────────────────────────────────┘

REM Check if docker-compose.yml exists for Airflow
if exist docker-compose-airflow.yml (
    echo Starting Airflow services...
    docker-compose -f docker-compose-airflow.yml up -d
    echo [✓] Airflow started
    echo     └─ Webserver: http://localhost:8081
    echo     └─ Username: admin
    echo     └─ Password: admin
) else (
    echo [!] Airflow configuration not found
    echo     Create docker-compose-airflow.yml to enable Airflow
    echo     Skipping Airflow startup...
)
echo.

REM ============================================================================
REM SECTION 5: Start GridCARE Dashboard
REM ============================================================================
echo ┌────────────────────────────────────────────────────────────────┐
echo │ 5/5 Starting GridCARE Dashboard...                             │
echo └────────────────────────────────────────────────────────────────┘

docker ps -a | findstr gridcare-dashboard >nul
if errorlevel 1 (
    echo Building GridCARE Dashboard...
    docker build -t gridcare-dashboard .
    
    echo Starting GridCARE Dashboard container...
    docker run -d ^
        --name gridcare-dashboard ^
        -p 8080:8080 ^
        -e API_URL=http://host.docker.internal:8000 ^
        --restart unless-stopped ^
        gridcare-dashboard
) else (
    echo Restarting existing GridCARE Dashboard...
    docker restart gridcare-dashboard
)

timeout /t 5 /nobreak >nul
echo [✓] GridCARE Dashboard started on port 8080
echo.

REM ============================================================================
REM Wait for all services to be ready
REM ============================================================================
echo.
echo ┌────────────────────────────────────────────────────────────────┐
echo │ Waiting for all services to be ready...                        │
echo └────────────────────────────────────────────────────────────────┘
timeout /t 5 /nobreak >nul

REM ============================================================================
REM Service Health Checks
REM ============================================================================
echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║                    SERVICE STATUS CHECK                        ║
echo ╚════════════════════════════════════════════════════════════════╝
echo.

REM Check Dashboard
curl -s http://localhost:8080/health >nul 2>&1
if errorlevel 1 (
    echo [✗] Dashboard       : http://localhost:8080     [FAILED]
) else (
    echo [✓] Dashboard       : http://localhost:8080     [RUNNING]
)

REM Check Prometheus
curl -s http://localhost:9090/-/healthy >nul 2>&1
if errorlevel 1 (
    echo [✗] Prometheus      : http://localhost:9090     [FAILED]
) else (
    echo [✓] Prometheus      : http://localhost:9090     [RUNNING]
)

REM Check Airflow (if running)
curl -s http://localhost:8081/health >nul 2>&1
if errorlevel 1 (
    echo [!] Airflow         : http://localhost:8081     [NOT CONFIGURED]
) else (
    echo [✓] Airflow         : http://localhost:8081     [RUNNING]
)

REM Check PostgreSQL
docker exec gridcare-postgres pg_isready -U gridcare >nul 2>&1
if errorlevel 1 (
    echo [✗] PostgreSQL      : localhost:5432            [FAILED]
) else (
    echo [✓] PostgreSQL      : localhost:5432            [RUNNING]
)

REM Check Redis
docker exec gridcare-redis redis-cli ping >nul 2>&1
if errorlevel 1 (
    echo [✗] Redis           : localhost:6379            [FAILED]
) else (
    echo [✓] Redis           : localhost:6379            [RUNNING]
)

echo.
echo ╔════════════════════════════════════════════════════════════════╗
echo ║                  ALL SERVICES STARTED!                         ║
echo ╚════════════════════════════════════════════════════════════════╝
echo.
echo 📊 Access Points:
echo ┌────────────────────────────────────────────────────────────────┐
echo │  Dashboard:       http://localhost:8080                        │
echo │  Prometheus:      http://localhost:9090                        │
echo │  Airflow:         http://localhost:8081 (if configured)        │
echo │  API Docs:        http://localhost:8080/docs                   │
echo │  Metrics:         http://localhost:8080/metrics                │
echo └────────────────────────────────────────────────────────────────┘
echo.
echo 🔍 Prometheus Queries:
echo ┌────────────────────────────────────────────────────────────────┐
echo │  Bronze Tier:     grid_bronze_throughput                       │
echo │  Silver Tier:     grid_silver_throughput                       │
echo │  Gold Tier:       grid_gold_throughput                         │
echo │  Total Load:      grid_total_load_mw                           │
echo │  Renewables:      grid_renewable_generation_mw                 │
echo │  Carbon:          grid_carbon_intensity                        │
echo └────────────────────────────────────────────────────────────────┘
echo.
echo 📋 Useful Commands:
echo ┌────────────────────────────────────────────────────────────────┐
echo │  View logs:       docker logs -f gridcare-dashboard            │
echo │  Stop all:        docker stop gridcare-dashboard gridcare-prometheus gridcare-postgres gridcare-redis
echo │  Restart:         docker restart gridcare-dashboard            │
echo │  Remove all:      docker rm -f gridcare-dashboard gridcare-prometheus gridcare-postgres gridcare-redis
echo └────────────────────────────────────────────────────────────────┘
echo.

REM Open browser automatically
set /p OPEN_BROWSER="Open dashboard in browser? (Y/N): "
if /i "%OPEN_BROWSER%"=="Y" (
    start http://localhost:8080
    start http://localhost:9090
)

echo.
echo Press any key to exit...
pause >nul