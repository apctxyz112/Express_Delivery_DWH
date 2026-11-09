import os

# 1. Kết nối PostgreSQL OLTP (Nguồn giao dịch)
OLTP_DB_URI = os.getenv(
    "OLTP_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
)

# 2. Kết nối Snowflake Data Cloud (Kho dữ liệu đích)
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "EXPRESS_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "EXPRESS_DWH")

# 3. Quy mô sinh dữ liệu OLTP
DEFAULT_CUSTOMERS = 1500
DEFAULT_HUBS = 30
DEFAULT_COURIERS = 200
DEFAULT_VEHICLES = 80
DEFAULT_SHIPMENTS = 10000
SIMULATE_NEW_SHIPMENTS = 200