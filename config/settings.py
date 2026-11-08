import os

# Cấu hình kết nối PostgreSQL OLTP và DWH (Hỗ trợ cả chạy Local và trong Docker)
OLTP_DB_URI = os.getenv(
    "OLTP_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
)

DWH_DB_URI = os.getenv(
    "DWH_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_dwh"
)

# Quy mô sinh dữ liệu mặc định theo Blueprint
DEFAULT_CUSTOMERS = 1500
DEFAULT_HUBS = 30
DEFAULT_COURIERS = 200
DEFAULT_VEHICLES = 80
DEFAULT_SHIPMENTS = 10000