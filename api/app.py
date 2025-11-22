import os
import asyncpg
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from endpoints import grid
from endpoints import summary  # <-- add this

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

app = FastAPI(title="GridCARE Energy API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# existing endpoints
app.include_router(grid.router, tags=["grid"])
# NEW summary router
app.include_router(summary.router)

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, database=PG_DB,
        min_size=1, max_size=10, command_timeout=30,
    )

@app.on_event("shutdown")
async def shutdown():
    pool = getattr(app.state, "pool", None)
    if pool:
        await pool.close()

@app.get("/")
async def root():
    return {"message": "GridCARE API. See /docs for Swagger."}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
