from sqlalchemy import create_engine
from contextlib import contextmanager
from config.settings import OLTP_DB_URI, DWH_DB_URI


oltp_engine = create_engine(OLTP_DB_URI, pool_pre_ping=True, future=True)
dwh_engine = create_engine(DWH_DB_URI, pool_pre_ping=True, future=True)

@contextmanager
def get_oltp_conn():
    conn = oltp_engine.connect()
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_dwh_conn():
    conn = dwh_engine.connect()
    try:
        yield conn
    finally:
        conn.close()