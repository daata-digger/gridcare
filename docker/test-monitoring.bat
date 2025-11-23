@echo off
REM GridCARE Monitoring Test Script for Windows
echo =================================
echo GridCARE Monitoring Health Check
echo =================================
echo.

echo [1/6] Checking Prometheus...
curl -s http://localhost:9090/-/healthy > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    [PASS] Prometheus is healthy
) else (
    echo    [FAIL] Prometheus is not accessible
)
echo.

echo [2/6] Checking Grafana...
curl -s http://localhost:3000/api/health > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    [PASS] Grafana is healthy
) else (
    echo    [FAIL] Grafana is not accessible
)
echo.

echo [3/6] Checking PostgreSQL Exporter...
curl -s http://localhost:9187/metrics | findstr "pg_up" > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    [PASS] PostgreSQL Exporter is working
) else (
    echo    [FAIL] PostgreSQL Exporter is not working
)
echo.

echo [4/6] Checking container status...
docker ps --filter "name=gridcare_prometheus" --format "{{.Names}}: {{.Status}}" | findstr "Up" > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    [PASS] Prometheus container is running
) else (
    echo    [FAIL] Prometheus container is not running
)
echo.

echo [5/6] Validating Prometheus config...
docker exec gridcare_prometheus promtool check config /etc/prometheus/prometheus.yml > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    [PASS] prometheus.yml is valid
) else (
    echo    [FAIL] prometheus.yml has errors
)
echo.

echo [6/6] Validating alert rules...
docker exec gridcare_prometheus promtool check rules /etc/prometheus/alerts.yml > nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo    [PASS] alerts.yml is valid
) else (
    echo    [FAIL] alerts.yml has errors
)
echo.

echo =================================
echo Access your monitoring:
echo   Prometheus: http://localhost:9090
echo   Grafana:    http://localhost:3000
echo =================================
echo.
echo Press any key to exit...
pause > nul