#!/bin/bash

# GridCARE Monitoring Test Script
# This script tests all components of your monitoring setup

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=================================${NC}"
echo -e "${BLUE}GridCARE Monitoring Test Suite${NC}"
echo -e "${BLUE}=================================${NC}"
echo ""

# Test counter
PASSED=0
FAILED=0

# Function to print test result
test_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC}: $2"
        ((PASSED++))
    else
        echo -e "${RED}✗ FAIL${NC}: $2"
        ((FAILED++))
    fi
}

# 1. Test Docker Compose is installed
echo -e "${YELLOW}[1/12] Checking docker-compose installation...${NC}"
if command -v docker-compose &> /dev/null; then
    test_result 0 "docker-compose is installed"
else
    test_result 1 "docker-compose is not installed"
    exit 1
fi
echo ""

# 2. Test all containers are running
echo -e "${YELLOW}[2/12] Checking container status...${NC}"
CONTAINERS=(
    "gridcare_prometheus"
    "gridcare_grafana"
    "gridcare_postgres_exporter"
    "gridcare_node_exporter"
    "gridcare_db"
    "gridcare_api"
)

for container in "${CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        test_result 0 "$container is running"
    else
        test_result 1 "$container is not running"
    fi
done
echo ""

# 3. Test Prometheus is accessible
echo -e "${YELLOW}[3/12] Testing Prometheus accessibility...${NC}"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:9090/-/healthy | grep -q "200"; then
    test_result 0 "Prometheus is accessible at http://localhost:9090"
else
    test_result 1 "Prometheus is not accessible"
fi
echo ""

# 4. Test Grafana is accessible
echo -e "${YELLOW}[4/12] Testing Grafana accessibility...${NC}"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health | grep -q "200"; then
    test_result 0 "Grafana is accessible at http://localhost:3000"
else
    test_result 1 "Grafana is not accessible"
fi
echo ""

# 5. Test Node Exporter metrics
echo -e "${YELLOW}[5/12] Testing Node Exporter...${NC}"
if curl -s http://localhost:9100/metrics | grep -q "node_cpu_seconds_total"; then
    test_result 0 "Node Exporter is exposing metrics"
else
    test_result 1 "Node Exporter is not exposing metrics"
fi
echo ""

# 6. Test PostgreSQL Exporter metrics
echo -e "${YELLOW}[6/12] Testing PostgreSQL Exporter...${NC}"
if curl -s http://localhost:9187/metrics | grep -q "pg_up"; then
    test_result 0 "PostgreSQL Exporter is exposing metrics"
else
    test_result 1 "PostgreSQL Exporter is not exposing metrics"
fi
echo ""

# 7. Test Prometheus targets
echo -e "${YELLOW}[7/12] Testing Prometheus targets...${NC}"
TARGETS_UP=$(curl -s http://localhost:9090/api/v1/targets 2>/dev/null | grep -o '"health":"up"' | wc -l)
if [ "$TARGETS_UP" -gt 0 ]; then
    test_result 0 "$TARGETS_UP Prometheus target(s) are UP"
else
    test_result 1 "No Prometheus targets are UP"
fi
echo ""

# 8. Test specific targets
echo -e "${YELLOW}[8/12] Testing individual Prometheus targets...${NC}"
TARGET_JOBS=("prometheus" "node" "postgres")
for job in "${TARGET_JOBS[@]}"; do
    if curl -s http://localhost:9090/api/v1/targets 2>/dev/null | grep -q "\"job\":\"$job\".*\"health\":\"up\""; then
        test_result 0 "Target '$job' is UP"
    else
        test_result 1 "Target '$job' is DOWN or missing"
    fi
done
echo ""

# 9. Test alert rules are loaded
echo -e "${YELLOW}[9/12] Testing alert rules...${NC}"
ALERT_COUNT=$(curl -s http://localhost:9090/api/v1/rules 2>/dev/null | grep -o '"type":"alerting"' | wc -l)
if [ "$ALERT_COUNT" -gt 0 ]; then
    test_result 0 "$ALERT_COUNT alert rule(s) loaded"
else
    test_result 1 "No alert rules loaded"
fi
echo ""

# 10. Test metrics collection
echo -e "${YELLOW}[10/12] Testing metrics collection...${NC}"
METRICS_TO_CHECK=(
    "up"
    "node_cpu_seconds_total"
    "node_memory_MemAvailable_bytes"
    "pg_up"
)

for metric in "${METRICS_TO_CHECK[@]}"; do
    if curl -s -G http://localhost:9090/api/v1/query --data-urlencode "query=$metric" 2>/dev/null | grep -q '"status":"success"'; then
        test_result 0 "Metric '$metric' is being collected"
    else
        test_result 1 "Metric '$metric' is not available"
    fi
done
echo ""

# 11. Test API metrics endpoint (optional)
echo -e "${YELLOW}[11/12] Testing API metrics endpoint (optional)...${NC}"
if curl -s http://localhost:8000/metrics 2>/dev/null | grep -q "# HELP"; then
    test_result 0 "API is exposing Prometheus metrics at /metrics"
else
    echo -e "${YELLOW}⚠ SKIP${NC}: API metrics endpoint not found (you may need to add prometheus_client)"
fi
echo ""

# 12. Test configuration files
echo -e "${YELLOW}[12/12] Testing configuration files...${NC}"
if docker exec gridcare_prometheus promtool check config /etc/prometheus/prometheus.yml &> /dev/null; then
    test_result 0 "prometheus.yml is valid"
else
    test_result 1 "prometheus.yml has errors"
fi

if docker exec gridcare_prometheus promtool check rules /etc/prometheus/alerts.yml &> /dev/null; then
    test_result 0 "alerts.yml is valid"
else
    test_result 1 "alerts.yml has errors"
fi
echo ""

# Summary
echo -e "${BLUE}=================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}=================================${NC}"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed! Your monitoring stack is working correctly.${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Open Prometheus: http://localhost:9090"
    echo "  2. Check Status → Targets (all should be UP)"
    echo "  3. Open Grafana: http://localhost:3000"
    echo "  4. Add Prometheus data source and import dashboards"
else
    echo -e "${RED}❌ Some tests failed. Please check the logs:${NC}"
    echo ""
    echo "Useful commands:"
    echo "  docker-compose logs prometheus"
    echo "  docker-compose logs postgres-exporter"
    echo "  docker-compose logs node-exporter"
    echo "  docker-compose ps"
fi

echo -e "${BLUE}=================================${NC}"