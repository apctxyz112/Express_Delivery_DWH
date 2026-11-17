from config.db_config import get_oltp_conn
from sqlalchemy import text

with get_oltp_conn() as conn:
    print(conn.execute(text("SELECT version()")).scalar())
    print(conn.execute(text("SELECT current_database()")).scalar())