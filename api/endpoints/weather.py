from fastapi import APIRouter

router = APIRouter(prefix="/weather", tags=["weather"])


@router.get("/health")
def weather_health():
    return {"status": "ok", "service": "weather"}
