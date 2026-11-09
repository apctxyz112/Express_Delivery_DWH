
```
express-delivery-dwh/
├── config/
│   ├── db_config.py
│   └── settings.py
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql
│   │   └── 02_seed_constraints.sql
│   └── snowflake_init/
│       └── 01_init_snowflake.sql
├── scripts/
│   ├── generate_oltp_data.py
│   ├── extract_load_snowflake.py
│   ├── dq_gate_snowflake.py
│   ├── post_load_watermark.py
│   └── run_local_pipeline.py
│── README.md
│───.env
│───.gitignore
│───venv/
├── dbt_project/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── snapshots/
│   │   ├── snap_customers.sql
│   │   └── snap_couriers.sql
│   └── models/
│       ├── sources.yml
│       ├── schema.yml
│       ├── silver/
│       │   ├── silver_customers.sql
│       │   ├── silver_hubs.sql
│       │   ├── silver_vehicles.sql
│       │   ├── silver_couriers.sql
│       │   ├── silver_service_types.sql
│       │   ├── silver_payment_methods.sql
│       │   ├── silver_status_reasons.sql
│       │   ├── silver_shipments.sql
│       │   ├── silver_shipment_tracking_events.sql
│       │   ├── silver_cod_settlements.sql
│       │   └── silver_customer_tickets.sql
│       └── gold/
│           ├── dimensions/
│           │   ├── dim_date.sql
│           │   ├── dim_hub.sql
│           │   ├── dim_vehicle.sql
│           │   ├── dim_service_type.sql
│           │   ├── dim_status_reason.sql
│           │   ├── dim_payment_method.sql
│           │   ├── dim_customer.sql
│           │   └── dim_courier.sql
│           ├── facts/
│           │   ├── fact_shipment_lifecycle.sql
│           │   ├── fact_tracking_events.sql
│           │   └── fact_daily_hub_snapshot.sql
│           └── analytics/
│               ├── v_slaship_summary.sql
│               ├── mv_daily_hub_performance.sql
│               └── mv_courier_performance.sql
├── dags/
│   └── express_delivery_snowflake_dbt_dag.py
└── docker/
    ├── docker-compose.yml
    ├── init-db.sql
    └── requirements.txt
```
