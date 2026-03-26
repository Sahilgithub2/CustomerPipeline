from __future__ import annotations

import os

import dlt
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from services.ingestion import fetch_all_customers, upsert_customers

router = APIRouter()


@router.post("/api/ingest")
def ingest(db: Session = Depends(get_db)):
    try:
        customers = fetch_all_customers(limit=100)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch from mock server: {e}") from e

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")

    # Use dlt to ingest the raw payload into a staging table, then upsert into the
    # target SQLAlchemy-managed table. This avoids dlt adding internal _dlt_* columns
    # to the canonical `customers` table.
    try:
        pipeline = dlt.pipeline(
            pipeline_name="customer_pipeline",
            destination="postgres",
            dataset_name="staging",
        )
        pipeline.run(
            customers,
            table_name="customers_raw",
            write_disposition="append",
            credentials=database_url,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"dlt load failed: {e}") from e

    try:
        records_processed = upsert_customers(db, customers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to ingest customers: {e}") from e

    return {"status": "success", "records_processed": records_processed}

