from fastapi import APIRouter, Request, Response
from typing import Any, Dict

router = APIRouter(prefix="/grid", tags=["grid-summary"])

SUMMARY_SQL = """
WITH latest AS (
  SELECT iso_code,
         MAX(timestamp_utc) AS ts
  FROM grid_iso_hourly_enriched
  GROUP BY iso_code
),
curr AS (
  SELECT g.iso_code,
         g.timestamp_utc,
         g.total_load_mw,
         g.renewables_mw,
         g.lmp_avg
  FROM grid_iso_hourly_enriched g
  JOIN latest l
    ON l.iso_code = g.iso_code AND l.ts = g.timestamp_utc
)
SELECT
  COUNT(*)                                   AS iso_count,
  COALESCE(SUM(total_load_mw),0)             AS total_load_mw,
  COALESCE(SUM(renewables_mw),0)             AS renewables_mw,
  COALESCE(AVG(lmp_avg),0)                   AS avg_price,
  MAX(timestamp_utc)                         AS as_of_utc
FROM curr;
"""

@router.get("/summary")
async def grid_summary(request: Request, response: Response) -> Dict[str, Any]:
    """
    Returns a compact, dashboard-ready JSON with totals. Never emits HTML.
    """
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            row = await conn.fetchrow(SUMMARY_SQL)

        payload = {
            "iso_count": int(row["iso_count"]),
            "total_load_mw": float(row["total_load_mw"]),
            "renewables_mw": float(row["renewables_mw"]),
            "avg_price": float(row["avg_price"]),
            "as_of_utc": row["as_of_utc"].isoformat() if row["as_of_utc"] else None,
            "status": "ok",
        }
        return payload
    except Exception as e:
        # Always return JSON on failure so the dashboard can render an error state
        response.status_code = 500
        return {
            "status": "error",
            "message": "Failed to compute grid summary",
            "detail": str(e),
        }
