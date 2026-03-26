from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from flask import Flask, jsonify, request


@dataclass(frozen=True)
class CustomersData:
    customers: list[dict[str, Any]]
    by_id: dict[str, dict[str, Any]]


def _data_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "data", "customers.json")


_CACHE: CustomersData | None = None
_CACHE_MTIME: float | None = None


def load_customers() -> CustomersData:
    global _CACHE, _CACHE_MTIME

    path = _data_path()
    mtime = os.path.getmtime(path)
    if _CACHE is not None and _CACHE_MTIME == mtime:
        return _CACHE

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    customers = payload if isinstance(payload, list) else payload.get("customers", [])
    if not isinstance(customers, list):
        raise ValueError("customers.json must contain a list of customers or {customers: [...]} object")

    by_id: dict[str, dict[str, Any]] = {}
    for c in customers:
        cid = str(c.get("customer_id", "")).strip()
        if cid:
            by_id[cid] = c

    _CACHE = CustomersData(customers=customers, by_id=by_id)
    _CACHE_MTIME = mtime
    return _CACHE


def parse_positive_int(name: str, default: int) -> int:
    raw = request.args.get(name, None)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


app = Flask(__name__)


@app.get("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/api/customers")
def list_customers():
    data = load_customers()

    page = parse_positive_int("page", 1)
    limit = parse_positive_int("limit", 10)

    total = len(data.customers)
    start = (page - 1) * limit
    end = start + limit
    items = data.customers[start:end] if start < total else []

    return jsonify(
        {
            "data": items,
            "total": total,
            "page": page,
            "limit": limit,
        }
    )


@app.get("/api/customers/<customer_id>")
def get_customer(customer_id: str):
    data = load_customers()
    customer = data.by_id.get(str(customer_id))
    if customer is None:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(customer)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
