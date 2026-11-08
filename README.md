
# Express Delivery Data Warehouse

Medallion Architecture (stg → silver → gold) + Kimball Dimensional Modeling

+ Apache Airflow + PostgreSQL.

## Kiến trúc

- **OLTP**: PostgreSQL (source simulation)
- **DWH**: PostgreSQL (stg / silver / gold / meta)
- **Orchestration**: Apache Airflow 2.9
- **Patterns**: SCD Type 2, Accumulating Snapshot, Data Quality Gate, Idempotent UPSERT

## Chạy local bằng Docker

```bash
cd docker
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
```
