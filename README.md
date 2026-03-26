# Customer Data Pipeline (Flask → FastAPI → Postgres)

## Prerequisites

- Docker Desktop (running)
- Docker Compose (`docker-compose --version`)

## Services

- **PostgreSQL**: `localhost:5432`
- **Flask mock server**: `http://localhost:5000`
- **FastAPI pipeline service**: `http://localhost:8000`

## Quickstart

Start everything:

```bash
docker-compose up -d --build
```

Test Flask (paginated customers):

```bash
curl "http://localhost:5000/api/customers?page=1&limit=5"
```

Ingest data into Postgres:

```bash
curl -X POST "http://localhost:8000/api/ingest"
```

Query customers from Postgres via FastAPI:

```bash
curl "http://localhost:8000/api/customers?page=1&limit=5"
```
