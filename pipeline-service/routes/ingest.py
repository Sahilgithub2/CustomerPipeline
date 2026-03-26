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

    records_processed = 0

    try:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is not set")

        pipeline = dlt.pipeline(
            pipeline_name="customer_pipeline",
            destination="postgres",
            dataset_name="public",
        )
        pipeline.run(
            customers,
            table_name="customers",
            write_disposition="merge",
            primary_key="customer_id",
            credentials=database_url,
        )
        records_processed = len(customers)
    except Exception:
        try:
            records_processed = upsert_customers(db, customers)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to ingest customers: {e}") from e

    return {"status": "success", "records_processed": records_processed}

