from __future__ import annotations

import os

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/customer_db",
)


class Base(DeclarativeBase):
    pass


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def ensure_customers_table_is_canonical() -> None:
    """
    If `customers` was created by dlt (contains _dlt_* columns), move it aside and
    allow SQLAlchemy to create the canonical table shape.
    """
    inspector = inspect(engine)
    if "customers" not in inspector.get_table_names():
        return

    cols = inspector.get_columns("customers")
    if not any(str(c.get("name", "")).startswith("_dlt_") for c in cols):
        return

    with engine.begin() as conn:
        suffix = conn.execute(text("SELECT to_char(now(), 'YYYYMMDDHH24MISS')")).scalar_one()
        backup_name = f"customers_dlt_backup_{suffix}"
        conn.execute(text(f'ALTER TABLE customers RENAME TO "{backup_name}"'))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
