import os
import sqlite3

from app.config import NEWS_DB_PATH


def connect(db_path: str | None = None) -> sqlite3.Connection:
    resolved_db_path = db_path or os.getenv("NEWS_DB_PATH", NEWS_DB_PATH)
    con = sqlite3.connect(resolved_db_path)
    con.row_factory = sqlite3.Row
    return con
