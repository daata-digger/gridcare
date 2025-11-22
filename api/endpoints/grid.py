# api/endpoints/grid.py
from typing import List, Optional, Any, Dict
from datetime import datetime
from fastapi import APIRouter, Query, Request, HTTPException

router = APIRouter()

# Helpers
def _row_to_dict(row: Any) -> Dict[str, Any]:
    d = dict(row)
    for k, v in list(d.items()):
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d

async def _fetch_all(request: Request, sql: str, *params) -> List[Dict[str, Any]]:
    try:
        pool = request.app.state.pool
    except AttributeError:
        raise HTTPException(status_code=500, detail="DB pool not ready")
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return [_row_to_dict(r) for r in rows]

# /grid/hourly
@router.get("/grid/hourly")
async def get_grid_hourly(
    request: Request,
    iso: str = Query(..., description="ISO code, e.g. CAISO, ISONE, NYISO, MISO, SPP"),
    start: Optional[str] = Query(None, description="Start hour ISO8601, inclusive"),
    end: Optional[str] = Query(None, description="End hour ISO8601, exclusive"),
    limit: int = Query(100, ge=1, le=10000),
):
    base = """
        SELECT iso_code, hour, avg_load_mw
        FROM grid_iso_hourly_demand
        WHERE iso_code = $1
    """
    params: List[Any] = [iso.upper()]
    if start:
        base += " AND hour >= $2"
        params.append(start)
        if end:
            base += " AND hour < $3"
            params.append(end)
    elif end:
        base += " AND hour < $2"
        params.append(end)

    base += " ORDER BY hour DESC LIMIT $%d" % (len(params) + 1)
    params.append(limit)

    return await _fetch_all(request, base, *params)

# /grid/daily
@router.get("/grid/daily")
async def get_grid_daily(
    request: Request,
    iso: str = Query(..., description="ISO code"),
    start: Optional[str] = Query(None, description="Start day ISO8601, inclusive"),
    end: Optional[str] = Query(None, description="End day ISO8601, exclusive"),
    limit: int = Query(365, ge=1, le=10000),
):
    base = """
        SELECT iso_code, day, avg_load_mw
        FROM grid_iso_daily_demand
        WHERE iso_code = $1
    """
    params: List[Any] = [iso.upper()]
    if start:
        base += " AND day >= $2"
        params.append(start)
        if end:
            base += " AND day < $3"
            params.append(end)
    elif end:
        base += " AND day < $2"
        params.append(end)

    base += " ORDER BY day DESC LIMIT $%d" % (len(params) + 1)
    params.append(limit)

    return await _fetch_all(request, base, *params)

# /grid/enriched
@router.get("/grid/enriched")
async def get_grid_enriched(
    request: Request,
    iso: str = Query(..., description="ISO code"),
    start: Optional[str] = Query(None, description="Start hour ISO8601, inclusive"),
    end: Optional[str] = Query(None, description="End hour ISO8601, exclusive"),
    limit: int = Query(200, ge=1, le=10000),
):
    base = """
        SELECT
          iso_code,
          hour,
          load_mw,
          temperature,
          wind_speed,
          humidity
        FROM grid_iso_hourly_enriched
        WHERE iso_code = $1
    """
    params: List[Any] = [iso.upper()]
    if start:
        base += " AND hour >= $2"
        params.append(start)
        if end:
            base += " AND hour < $3"
            params.append(end)
    elif end:
        base += " AND hour < $2"
        params.append(end)

    base += " ORDER BY hour DESC LIMIT $%d" % (len(params) + 1)
    params.append(limit)

    return await _fetch_all(request, base, *params)
