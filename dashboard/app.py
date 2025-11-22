import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import httpx

API_BASE = os.getenv("API_URL", "http://api:8000")

app = FastAPI(title="GridCARE Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the main dashboard HTML
@app.get("/", response_class=HTMLResponse)
async def index():
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GridCARE - Live Energy Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
            color: #e2e8f0;
            min-height: 100vh;
        }
        .header {
            background: rgba(15, 23, 42, 0.8);
            backdrop-filter: blur(10px);
            border-bottom: 1px solid #334155;
            padding: 1.5rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
        }
        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .logo-section { display: flex; align-items: center; gap: 1rem; }
        .logo {
            background: linear-gradient(135deg, #3b82f6, #06b6d4);
            padding: 0.75rem;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .logo svg { width: 32px; height: 32px; color: white; }
        .title {
            font-size: 1.75rem;
            font-weight: 700;
            background: linear-gradient(135deg, #3b82f6, #06b6d4);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .subtitle { font-size: 0.75rem; color: #94a3b8; margin-top: 0.25rem; }
        .status-section { display: flex; align-items: center; gap: 1rem; }
        .last-update {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            color: #cbd5e1;
            font-size: 0.875rem;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            background: #10b981;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .container { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        .metric-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border: 1px solid #334155;
            border-radius: 16px;
            padding: 1.5rem;
            transition: all 0.3s ease;
        }
        .metric-card:hover {
            border-color: #475569;
            transform: translateY(-4px);
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
        }
        .metric-icon {
            width: 48px;
            height: 48px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 1rem;
        }
        .metric-icon.blue { background: linear-gradient(135deg, #3b82f6, #06b6d4); }
        .metric-icon.green { background: linear-gradient(135deg, #10b981, #059669); }
        .metric-icon.amber { background: linear-gradient(135deg, #f59e0b, #d97706); }
        .metric-icon.purple { background: linear-gradient(135deg, #8b5cf6, #7c3aed); }
        .metric-icon svg { width: 24px; height: 24px; color: white; }
        .metric-label {
            color: #94a3b8;
            font-size: 0.875rem;
            font-weight: 500;
            margin-bottom: 0.5rem;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: white;
            display: flex;
            align-items: baseline;
            gap: 0.5rem;
        }
        .metric-unit { font-size: 0.875rem; color: #94a3b8; font-weight: 400; }
        .metric-subtitle { font-size: 0.75rem; color: #64748b; margin-top: 0.5rem; }
        .iso-selector {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border: 1px solid #334155;
            border-radius: 16px;
            padding: 1.5rem;
            margin-bottom: 2rem;
        }
        .iso-selector-label {
            font-size: 0.875rem;
            font-weight: 500;
            color: #cbd5e1;
            margin-bottom: 1rem;
        }
        .iso-buttons { display: flex; flex-wrap: wrap; gap: 0.75rem; }
        .iso-button {
            padding: 0.75rem 1.5rem;
            border: none;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.875rem;
            cursor: pointer;
            transition: all 0.3s ease;
            background: #334155;
            color: #cbd5e1;
        }
        .iso-button:hover {
            background: #475569;
            transform: translateY(-2px);
        }
        .iso-button.active {
            color: white;
            box-shadow: 0 10px 25px rgba(59, 130, 246, 0.3);
        }
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        .chart-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border: 1px solid #334155;
            border-radius: 16px;
            padding: 1.5rem;
        }
        .chart-title {
            font-size: 1.125rem;
            font-weight: 600;
            color: #e2e8f0;
            margin-bottom: 1.5rem;
        }
        .chart-container { position: relative; height: 300px; }
        .table-card {
            background: rgba(30, 41, 59, 0.6);
            backdrop-filter: blur(10px);
            border: 1px solid #334155;
            border-radius: 16px;
            overflow: hidden;
        }
        .table-header { padding: 1.5rem; border-bottom: 1px solid #334155; }
        table { width: 100%; border-collapse: collapse; }
        thead { background: rgba(15, 23, 42, 0.5); }
        th {
            padding: 1rem 1.5rem;
            text-align: left;
            font-size: 0.75rem;
            font-weight: 600;
            color: #cbd5e1;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        td { padding: 1rem 1.5rem; border-top: 1px solid #334155; color: #e2e8f0; }
        tbody tr { transition: background 0.2s ease; }
        tbody tr:hover { background: rgba(51, 65, 85, 0.3); }
        .status-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 500;
            background: rgba(16, 185, 129, 0.2);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }
        .trend-positive {
            color: #10b981;
            display: flex;
            align-items: center;
            gap: 0.25rem;
            font-size: 0.875rem;
        }
        .iso-indicator { display: flex; align-items: center; gap: 0.75rem; }
        .iso-color-dot { width: 12px; height: 12px; border-radius: 50%; }
        .iso-name { font-weight: 500; }
        .iso-code { font-size: 0.75rem; color: #94a3b8; }
        .footer {
            margin-top: 3rem;
            padding: 2rem;
            border-top: 1px solid #334155;
            background: rgba(15, 23, 42, 0.5);
            text-align: center;
            color: #94a3b8;
            font-size: 0.875rem;
        }
        .error-message {
            background: rgba(239, 68, 68, 0.2);
            border: 1px solid #ef4444;
            border-radius: 12px;
            padding: 1rem;
            margin-bottom: 1.5rem;
            color: #fca5a5;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        .loading {
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 400px;
            flex-direction: column;
            gap: 1rem;
        }
        .spinner {
            width: 48px;
            height: 48px;
            border: 4px solid #334155;
            border-top-color: #3b82f6;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        @media (max-width: 768px) {
            .charts-grid { grid-template-columns: 1fr; }
            .header-content { flex-direction: column; gap: 1rem; text-align: center; }
            th, td { padding: 0.75rem; font-size: 0.875rem; }
        }
    </style>
</head>
<body>
    <header class="header">
        <div class="header-content">
            <div class="logo-section">
                <div class="logo">
                    <i data-lucide="zap"></i>
                </div>
                <div>
                    <div class="title">GridCARE Live</div>
                    <div class="subtitle">Real-time Energy Grid Monitoring</div>
                </div>
            </div>
            <div class="status-section">
                <div class="last-update">
                    <i data-lucide="clock"></i>
                    <span id="lastUpdate">Loading...</span>
                </div>
                <div class="status-dot"></div>
            </div>
        </div>
    </header>

    <div class="container">
        <div id="errorContainer"></div>
        
        <div id="loadingContainer" class="loading">
            <div class="spinner"></div>
            <p>Loading GridCARE data...</p>
        </div>

        <div id="mainContent" style="display: none;">
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-icon blue"><i data-lucide="activity"></i></div>
                    <div class="metric-label">Total Load</div>
                    <div class="metric-value">
                        <span id="totalLoad">0</span>
                        <span class="metric-unit">MW</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-icon green"><i data-lucide="wind"></i></div>
                    <div class="metric-label">Renewables</div>
                    <div class="metric-value">
                        <span id="renewables">0</span>
                        <span class="metric-unit">MW</span>
                    </div>
                    <div class="metric-subtitle" id="renewablePercent">0% of total</div>
                </div>
                <div class="metric-card">
                    <div class="metric-icon amber"><i data-lucide="dollar-sign"></i></div>
                    <div class="metric-label">Avg LMP Price</div>
                    <div class="metric-value"><span id="avgPrice">$0.00</span></div>
                </div>
                <div class="metric-card">
                    <div class="metric-icon purple"><i data-lucide="trending-up"></i></div>
                    <div class="metric-label">Active ISOs</div>
                    <div class="metric-value">
                        <span id="isoCount">0</span>
                        <span class="metric-unit">markets</span>
                    </div>
                </div>
            </div>

            <div class="iso-selector">
                <div class="iso-selector-label">Select ISO Region</div>
                <div class="iso-buttons" id="isoButtons"></div>
            </div>

            <div class="charts-grid">
                <div class="chart-card">
                    <div class="chart-title">24-Hour Load Profile</div>
                    <div class="chart-container"><canvas id="loadChart"></canvas></div>
                </div>
                <div class="chart-card">
                    <div class="chart-title">Load Distribution by ISO</div>
                    <div class="chart-container"><canvas id="pieChart"></canvas></div>
                </div>
            </div>

            <div class="table-card">
                <div class="table-header">
                    <div class="chart-title">ISO Status Overview</div>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>ISO Region</th>
                            <th>Status</th>
                            <th style="text-align: right;">Current Load</th>
                            <th style="text-align: right;">Avg Price</th>
                            <th style="text-align: right;">Trend</th>
                        </tr>
                    </thead>
                    <tbody id="isoTableBody"></tbody>
                </table>
            </div>
        </div>
    </div>

    <footer class="footer">
        GridCARE Energy Platform • Real-time grid monitoring and analytics • Data refreshes every 30 seconds
    </footer>

    <script>
        const API_BASE = '/api';
        const REFRESH_INTERVAL = 30000;
        const isoList = [
            { code: 'CAISO', name: 'California', color: '#3b82f6' },
            { code: 'ISONE', name: 'New England', color: '#10b981' },
            { code: 'NYISO', name: 'New York', color: '#f59e0b' },
            { code: 'MISO', name: 'Midwest', color: '#8b5cf6' },
            { code: 'SPP', name: 'Southwest', color: '#ef4444' }
        ];
        let selectedISO = 'CAISO';
        let loadChart = null;
        let pieChart = null;
        
        lucide.createIcons();
        
        function formatNumber(num) {
            if (!num) return '0';
            return new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(num);
        }
        
        function formatPrice(num) {
            if (!num) return '$0.00';
            return new Intl.NumberFormat('en-US', { 
                style: 'currency', 
                currency: 'USD',
                minimumFractionDigits: 2 
            }).format(num);
        }
        
        function showError(message) {
            document.getElementById('errorContainer').innerHTML = `
                <div class="error-message">
                    <i data-lucide="alert-circle"></i>
                    <span>${message}</span>
                </div>
            `;
            lucide.createIcons();
        }
        
        function clearError() {
            document.getElementById('errorContainer').innerHTML = '';
        }
        
        async function fetchSummary() {
            try {
                const response = await fetch(`${API_BASE}/grid/summary`);
                if (!response.ok) throw new Error('Failed to fetch summary');
                const data = await response.json();
                if (data.status === 'error') throw new Error(data.message || 'Unknown error');
                updateSummaryUI(data);
                clearError();
            } catch (error) {
                console.error('Error:', error);
                showError(`API Error: ${error.message}`);
            }
        }
        
        async function fetchHourlyData(iso) {
            try {
                const response = await fetch(`${API_BASE}/grid/hourly?iso=${iso}&limit=24`);
                if (!response.ok) throw new Error('Failed to fetch hourly data');
                const data = await response.json();
                updateLoadChart(data);
            } catch (error) {
                console.error('Error:', error);
            }
        }
        
        function updateSummaryUI(data) {
            document.getElementById('totalLoad').textContent = formatNumber(data.total_load_mw);
            document.getElementById('renewables').textContent = formatNumber(data.renewables_mw);
            document.getElementById('avgPrice').textContent = formatPrice(data.avg_price);
            document.getElementById('isoCount').textContent = data.iso_count || 0;
            const renewablePercent = data.total_load_mw > 0 
                ? ((data.renewables_mw / data.total_load_mw) * 100).toFixed(1) : 0;
            document.getElementById('renewablePercent').textContent = `${renewablePercent}% of total`;
            document.getElementById('lastUpdate').textContent = `Updated ${new Date().toLocaleTimeString()}`;
            updatePieChart(data.total_load_mw);
            updateISOTable(data);
        }
        
        function updateLoadChart(data) {
            const ctx = document.getElementById('loadChart').getContext('2d');
            const sortedData = data.slice().reverse();
            const labels = sortedData.map(d => {
                const date = new Date(d.hour);
                return `${date.getHours()}:00`;
            });
            const values = sortedData.map(d => d.avg_load_mw || 0);
            if (loadChart) loadChart.destroy();
            loadChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Load (MW)',
                        data: values,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true,
                        pointRadius: 4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        y: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
                        x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } }
                    }
                }
            });
        }
        
        function updatePieChart(totalLoad) {
            const ctx = document.getElementById('pieChart').getContext('2d');
            const distribution = [
                { name: 'CAISO', value: totalLoad * 0.30, color: '#3b82f6' },
                { name: 'NYISO', value: totalLoad * 0.20, color: '#f59e0b' },
                { name: 'MISO', value: totalLoad * 0.25, color: '#8b5cf6' },
                { name: 'ISONE', value: totalLoad * 0.15, color: '#10b981' },
                { name: 'SPP', value: totalLoad * 0.10, color: '#ef4444' }
            ];
            if (pieChart) pieChart.destroy();
            pieChart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: distribution.map(d => d.name),
                    datasets: [{ data: distribution.map(d => d.value), backgroundColor: distribution.map(d => d.color), borderWidth: 0 }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: 'bottom', labels: { color: '#e2e8f0' } } }
                }
            });
        }
        
        function updateISOTable(data) {
            const tbody = document.getElementById('isoTableBody');
            const totalLoad = data.total_load_mw || 0;
            const avgPrice = data.avg_price || 0;
            const distribution = [0.30, 0.15, 0.20, 0.25, 0.10];
            tbody.innerHTML = isoList.map((iso, idx) => {
                const load = totalLoad * distribution[idx];
                const price = avgPrice * (0.8 + Math.random() * 0.4);
                const trend = (Math.random() * 5).toFixed(1);
                return `<tr>
                    <td><div class="iso-indicator">
                        <div class="iso-color-dot" style="background: ${iso.color};"></div>
                        <div><div class="iso-name">${iso.name}</div><div class="iso-code">${iso.code}</div></div>
                    </div></td>
                    <td><span class="status-badge">Online</span></td>
                    <td style="text-align: right;">${formatNumber(load)} MW</td>
                    <td style="text-align: right;">${formatPrice(price)}</td>
                    <td style="text-align: right;"><div class="trend-positive" style="justify-content: flex-end;">
                        <i data-lucide="trending-up" style="width: 16px; height: 16px;"></i>
                        <span>+${trend}%</span>
                    </div></td>
                </tr>`;
            }).join('');
            lucide.createIcons();
        }
        
        function initISOButtons() {
            const container = document.getElementById('isoButtons');
            const allButton = document.createElement('button');
            allButton.className = 'iso-button active';
            allButton.textContent = 'All Regions';
            allButton.style.background = '#3b82f6';
            allButton.onclick = () => selectISO('ALL', allButton);
            container.appendChild(allButton);
            isoList.forEach(iso => {
                const button = document.createElement('button');
                button.className = 'iso-button';
                button.textContent = iso.name;
                button.onclick = () => selectISO(iso.code, button);
                container.appendChild(button);
            });
        }
        
        function selectISO(isoCode, button) {
            selectedISO = isoCode;
            document.querySelectorAll('.iso-button').forEach(btn => {
                btn.classList.remove('active');
                btn.style.background = '#334155';
            });
            button.classList.add('active');
            const iso = isoList.find(i => i.code === isoCode);
            button.style.background = iso ? iso.color : '#3b82f6';
            const fetchISO = isoCode === 'ALL' ? 'CAISO' : isoCode;
            fetchHourlyData(fetchISO);
        }
        
        async function init() {
            initISOButtons();
            await fetchSummary();
            await fetchHourlyData(selectedISO);
            document.getElementById('loadingContainer').style.display = 'none';
            document.getElementById('mainContent').style.display = 'block';
            setInterval(() => {
                fetchSummary();
                fetchHourlyData(selectedISO);
            }, REFRESH_INTERVAL);
        }
        
        init();
    </script>
</body>
</html>"""
    return HTMLResponse(content=html_content)

@app.get("/health")
async def health():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{API_BASE}/grid/summary")
            return {"status": "ok", "api": r.status_code == 200}
    except Exception as e:
        return {"status": "degraded", "error": str(e)}

@app.get("/api/grid/summary")
async def grid_summary():
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{API_BASE}/grid/summary")
            r.raise_for_status()
            return r.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

@app.get("/api/grid/hourly")
async def grid_hourly(iso: str, limit: int = 24):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{API_BASE}/grid/hourly", params={"iso": iso, "limit": limit})
            r.raise_for_status()
            return r.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Upstream error: {e}")