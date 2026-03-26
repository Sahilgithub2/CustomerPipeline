from __future__ import annotations

from fastapi import APIRouter

from .customers import router as customers_router
from .ingest import router as ingest_router

api_router = APIRouter()
api_router.include_router(ingest_router)
api_router.include_router(customers_router)

