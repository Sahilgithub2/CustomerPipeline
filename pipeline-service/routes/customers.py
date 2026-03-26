from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database import get_db
from models.customer import Customer
from services.ingestion import get_customer_by_id

router = APIRouter()


def _customer_to_dict(c: Customer) -> dict[str, Any]:
    return {
        "customer_id": c.customer_id,
        "first_name": c.first_name,
        "last_name": c.last_name,
        "email": c.email,
        "phone": c.phone,
        "address": c.address,
        "date_of_birth": c.date_of_birth.isoformat() if c.date_of_birth else None,
        "account_balance": float(c.account_balance) if c.account_balance is not None else None,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


@router.get("/api/customers")
def list_customers(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=200),
    db: Session = Depends(get_db),
):
    total = db.execute(select(func.count()).select_from(Customer)).scalar_one()
    offset = (page - 1) * limit

    rows = (
        db.execute(select(Customer).order_by(Customer.customer_id).offset(offset).limit(limit))
        .scalars()
        .all()
    )
    return {
        "data": [_customer_to_dict(c) for c in rows],
        "total": int(total),
        "page": page,
        "limit": limit,
    }


@router.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    c = get_customer_by_id(db, customer_id)
    if c is None:
        raise HTTPException(status_code=404, detail="Customer not found")
    return _customer_to_dict(c)

