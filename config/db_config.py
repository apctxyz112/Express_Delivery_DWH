from contextlib import contextmanager
from sqlalchemy import create_engine
import snowflake.connector
from config.settings import (
    OLTP_DB_URI,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
)

oltp_engine = create_engine(OLTP_DB_URI, pool_pre_ping=True, future=True)


@contextmanager
def get_oltp_conn():
    conn = oltp_engine.connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_snowflake_conn(schema: str = "META"):
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=schema,
        session_parameters={"TIMEZONE": "Asia/Ho_Chi_Minh"},
    )
    try:
        yield conn
    finally:
        conn.close()