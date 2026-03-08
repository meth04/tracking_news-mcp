import sqlite3
from datetime import UTC, datetime


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def get_crawl_state_last_published_at(
    con: sqlite3.Connection,
    *,
    source: str,
    section: str,
) -> str | None:
    row = con.execute(
        """
        select last_published_at
        from crawl_state
        where source = ? and section = ?
        limit 1
        """,
        (source, section),
    ).fetchone()
    if row is None:
        return None
    return row["last_published_at"]


def upsert_crawl_state(
    con: sqlite3.Connection,
    *,
    source: str,
    section: str,
    status: str,
    error: str | None = None,
    last_published_at: str | None = None,
) -> None:
    con.execute(
        """
        insert into crawl_state (source, section, last_published_at, last_run_at, status, error)
        values (?, ?, ?, ?, ?, ?)
        on conflict(source, section) do update set
            last_published_at = case
                when excluded.last_published_at is null then crawl_state.last_published_at
                when crawl_state.last_published_at is null then excluded.last_published_at
                when excluded.last_published_at > crawl_state.last_published_at then excluded.last_published_at
                else crawl_state.last_published_at
            end,
            last_run_at = excluded.last_run_at,
            status = excluded.status,
            error = excluded.error
        """,
        (source, section, last_published_at, _now_iso(), status, error),
    )
    con.commit()
