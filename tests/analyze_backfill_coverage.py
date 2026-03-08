from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path

NEWS_DB_PATH = "./data/news.db"

RUN_RE = re.compile(r"^\[(?P<source>[^\]:]+)(?::(?P<section>[^\]]+))?\]\s+(?P<body>.+)$")
KV_RE = re.compile(r"(?P<key>[a-zA-Z_]+)=(?P<value>[^\s]+)")


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=NEWS_DB_PATH)
    parser.add_argument("--log")
    parser.add_argument("--date-from", default="2026-01-01")
    parser.add_argument("--date-to")
    parser.add_argument("--limit", type=int, default=12)
    return parser.parse_args()


def print_section(title: str) -> None:
    print(f"\n=== {title} ===")


def query_rows(con: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return con.execute(sql, params).fetchall()


def print_rows(rows: list[sqlite3.Row], *, limit: int | None = None) -> None:
    for row in rows[: limit or len(rows)]:
        print(dict(row))
    if limit is not None and len(rows) > limit:
        print(f"... ({len(rows) - limit} more)")


def analyze_totals(
    con: sqlite3.Connection, date_from: str, date_to: str | None, limit: int
) -> None:
    params = [date_from]
    date_sql = "published_date >= ?"
    if date_to:
        date_sql += " and published_date <= ?"
        params.append(date_to)

    print_section("overview")
    overview = con.execute(
        f"""
        select count(*) as articles,
               count(distinct source) as sources,
               min(published_date) as min_date,
               max(published_date) as max_date,
               sum(case when published_at is null or trim(published_at) = '' then 1 else 0 end) as missing_published_at,
               sum(case when seed_section is null or trim(seed_section) = '' then 1 else 0 end) as missing_seed_section,
               sum(case when topic_label is null or trim(topic_label) = '' then 1 else 0 end) as missing_topic_label
        from articles
        where {date_sql}
        """,
        tuple(params),
    ).fetchone()
    print(dict(overview))

    print_section("counts by source / section / topic")
    rows = query_rows(
        con,
        f"""
        select source, seed_section, topic_label,
               count(*) as n,
               min(published_date) as min_date,
               max(published_date) as max_date
        from articles
        where {date_sql}
        group by source, seed_section, topic_label
        order by n desc, source asc, seed_section asc
        """,
        tuple(params),
    )
    print_rows(rows, limit=limit * 2)

    print_section("cafef tai-chinh-ngan-hang count")
    row = con.execute(
        f"""
        select count(*) as n
        from articles
        where source = 'cafef'
          and seed_section = 'tai-chinh-ngan-hang'
          and {date_sql}
        """,
        tuple(params),
    ).fetchone()
    print(dict(row))

    print_section("duplicate pressure")
    rows = query_rows(
        con,
        f"""
        select source,
               count(*) as rows,
               count(distinct content_sha256) as distinct_sha,
               count(*) - count(distinct content_sha256) as duplicate_sha_pressure
        from articles
        where {date_sql}
        group by source
        order by duplicate_sha_pressure desc, rows desc
        """,
        tuple(params),
    )
    print_rows(rows, limit=limit)


def analyze_daily_gaps(
    con: sqlite3.Connection, date_from: str, date_to: str | None, limit: int
) -> None:
    end_date = date_to or con.execute("select max(published_date) from articles").fetchone()[0]
    if end_date is None:
        return

    print_section("daily totals")
    rows = query_rows(
        con,
        """
        select published_date, count(*) as n, avg(fomo_score) as avg_fomo
        from articles
        where published_date between ? and ?
        group by published_date
        order by published_date desc
        """,
        (date_from, end_date),
    )
    print_rows(rows, limit=limit)

    print_section("source gap days")
    rows = query_rows(
        con,
        """
        with recursive days(day) as (
          select date(?)
          union all
          select date(day, '+1 day') from days where day < date(?)
        ),
        combos as (
          select distinct source, seed_section from articles
        ),
        daily as (
          select source, seed_section, published_date, count(*) as n
          from articles
          where published_date between ? and ?
          group by source, seed_section, published_date
        )
        select combos.source,
               combos.seed_section,
               count(*) filter (where coalesce(daily.n, 0) = 0) as zero_days,
               max(days.day) filter (where coalesce(daily.n, 0) > 0) as latest_day,
               cast(julianday(?) - julianday(max(days.day) filter (where coalesce(daily.n, 0) > 0)) as integer) as freshness_gap_days
        from combos
        cross join days
        left join daily
          on daily.source = combos.source
         and daily.seed_section = combos.seed_section
         and daily.published_date = days.day
        group by combos.source, combos.seed_section
        order by zero_days desc, freshness_gap_days desc, combos.source asc, combos.seed_section asc
        """,
        (date_from, end_date, date_from, end_date, end_date),
    )
    print_rows(rows, limit=limit)

    print_section("largest day-over-day drops")
    rows = query_rows(
        con,
        """
        with daily as (
          select source, seed_section, published_date, count(*) as n
          from articles
          where published_date between ? and ?
          group by source, seed_section, published_date
        ),
        lagged as (
          select source,
                 seed_section,
                 published_date,
                 n,
                 lag(n) over (partition by source, seed_section order by published_date) as prev_n
          from daily
        )
        select source,
               seed_section,
               published_date,
               prev_n,
               n,
               (n - prev_n) as delta
        from lagged
        where prev_n is not null and prev_n >= 5 and n <= prev_n * 0.35
        order by delta asc, prev_n desc
        limit ?
        """,
        (date_from, end_date, limit),
    )
    print_rows(rows)


def analyze_runtime_stats(con: sqlite3.Connection, limit: int) -> None:
    print_section("latest ingest_runs")
    runs = query_rows(
        con,
        "select * from ingest_runs order by id desc limit ?",
        (limit,),
    )
    print_rows(runs)

    tables = {row[0] for row in con.execute("select name from sqlite_master where type='table'")}
    if "ingest_section_runs" in tables:
        print_section("latest ingest_section_runs")
        rows = query_rows(
            con,
            """
            select run_id, source, section, pages_scanned, discovered_raw, discovered_unique,
                   processed_urls, inserted_count, dropped_no_date_count, dropped_irrelevant_count,
                   dropped_out_of_window_count, dedup_dropped_count, failed_count, latest_published_at
            from ingest_section_runs
            order by run_id desc, source asc, section asc
            limit ?
            """,
            (limit * 4,),
        )
        print_rows(rows)

        print_section("budget waste by section")
        rows = query_rows(
            con,
            """
            select source,
                   section,
                   avg(pages_scanned) as avg_pages_scanned,
                   avg(discovered_unique) as avg_discovered_unique,
                   avg(inserted_count) as avg_inserted_count,
                   avg(dropped_out_of_window_count) as avg_dropped_out_of_window_count,
                   avg(failed_count) as avg_failed_count
            from ingest_section_runs
            group by source, section
            order by avg_dropped_out_of_window_count desc, avg_pages_scanned desc, source asc
            limit ?
            """,
            (limit * 2,),
        )
        print_rows(rows)

    print_section("crawl_state")
    rows = query_rows(
        con,
        "select source, section, status, last_published_at, last_run_at, error from crawl_state order by source, section",
    )
    print_rows(rows, limit=limit * 3)


def parse_ingest_log(log_path: str) -> None:
    print_section(f"parsed ingest log: {log_path}")
    by_key: dict[tuple[str, str | None], dict[str, str]] = {}
    for raw_line in Path(log_path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        match = RUN_RE.match(line)
        if not match:
            continue
        source = match.group("source")
        section = match.group("section")
        metrics = {m.group("key"): m.group("value") for m in KV_RE.finditer(match.group("body"))}
        by_key[(source, section)] = metrics

    for key in sorted(by_key):
        source, section = key
        label = f"{source}:{section}" if section else source
        print(label, by_key[key])


def main() -> None:
    args = parse_args()
    with connect(args.db) as con:
        analyze_totals(con, args.date_from, args.date_to, args.limit)
        analyze_daily_gaps(con, args.date_from, args.date_to, args.limit)
        analyze_runtime_stats(con, args.limit)
    if args.log:
        parse_ingest_log(args.log)


if __name__ == "__main__":
    main()
