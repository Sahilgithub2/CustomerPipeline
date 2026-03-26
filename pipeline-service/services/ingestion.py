from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from models.customer import Customer


MOCK_SERVER_BASE_URL = os.getenv("MOCK_SERVER_BASE_URL", "http://localhost:5000")


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            return None
    return None


def _parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def fetch_all_customers(limit: int = 100) -> list[dict[str, Any]]:
    customers: list[dict[str, Any]] = []
    page = 1

    while True:
        resp = requests.get(
            f"{MOCK_SERVER_BASE_URL}/api/customers",
            params={"page": page, "limit": limit},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("data", [])
        if not isinstance(batch, list):
            raise ValueError("Mock server returned invalid payload: data is not a list")

        customers.extend(batch)

        total = int(payload.get("total", len(customers)))
        if len(customers) >= total or len(batch) == 0:
            break
        page += 1

    return customers


def normalize_customer(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "customer_id": str(raw.get("customer_id", "")).strip(),
        "first_name": (raw.get("first_name") or "").strip(),
        "last_name": (raw.get("last_name") or "").strip(),
        "email": (raw.get("email") or "").strip(),
        "phone": (raw.get("phone") or None),
        "address": (raw.get("address") or None),
        "date_of_birth": _parse_date(raw.get("date_of_birth")),
        "account_balance": _parse_decimal(raw.get("account_balance")),
        "created_at": _parse_datetime(raw.get("created_at")),
    }


def upsert_customers(db: Session, customers: list[dict[str, Any]]) -> int:
    normalized = [normalize_customer(c) for c in customers]
    normalized = [c for c in normalized if c.get("customer_id")]

    if not normalized:
        return 0

    stmt = pg_insert(Customer).values(normalized)
    update_cols = {
        c.name: getattr(stmt.excluded, c.name)
        for c in Customer.__table__.columns
        if c.name != "customer_id"
    }
    stmt = stmt.on_conflict_do_update(index_elements=[Customer.customer_id], set_=update_cols)
    db.execute(stmt)
    db.commit()
    return len(normalized)


def get_customer_by_id(db: Session, customer_id: str) -> Customer | None:
    return db.execute(select(Customer).where(Customer.customer_id == customer_id)).scalar_one_or_none()
