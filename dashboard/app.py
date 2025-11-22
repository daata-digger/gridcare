import os
import httpx
from fastapi import FastAPI, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

# The API container hostname is "api" on the docker network; keep port 8000
API_BASE = os.getenv("API_BASE", "http://api:8000")

app = FastAPI(title="GridCARE Dashboard")

# static site
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(os.path.join(static_dir, "index.html"))

# Proxy endpoint used by the front-end
@app.get("/api/summary")
async def proxy_summary():
    url = f"{API_BASE}/grid/summary"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
        # pass through API JSON or error JSON; never raw text
        if r.headers.get("content-type", "").startswith("application/json"):
            return JSONResponse(status_code=r.status_code, content=r.json())
        # If API emitted text/HTML, wrap it as JSON error
        return JSONResponse(
            status_code=502,
            content={"status": "error", "message": "Upstream returned non-JSON", "body": r.text[:500]},
        )
    except Exception as e:
        return JSONResponse(status_code=502, content={"status":"error","message":"Upstream call failed","detail":str(e)})
