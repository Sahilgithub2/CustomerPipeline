from __future__ import annotations

from fastapi import FastAPI

from database import Base, engine, ensure_customers_table_is_canonical
from routes import api_router


app = FastAPI(title="Pipeline Service", version="1.0.0")
app.include_router(api_router)


@app.on_event("startup")
def on_startup() -> None:
    ensure_customers_table_is_canonical()
    Base.metadata.create_all(bind=engine)

