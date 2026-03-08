import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from typing import Any

MAX_LIST_LIMIT = 200
MAX_AGGREGATE_LIMIT = 50
MAX_TOP_TICKERS = 50
SNIPPET_LENGTH = 280

LIST_SORTS = (
    "published_at_desc",
    "published_at_asc",
    "fomo_desc",
    "fomo_asc",
)
SLICE_GROUP_BYS = (
    "source",
    "category",
    "section",
    "topic",
    "published_date",
    "ticker",
)
SLICE_SORTS = (
    "count_desc",
    "count_asc",
    "avg_fomo_desc",
    "avg_fomo_asc",
    "date_desc",
    "date_asc",
)
FACET_FIELDS = (
    "sources",
    "categories",
    "sections",
    "topics",
    "tickers",
)


@dataclass(frozen=True, slots=True)
class ArticleFilters:
    date_from: str
    date_to: str
    sources: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()
    sections: tuple[str, ...] = ()
    tickers: tuple[str, ...] = ()
    min_fomo: float = -1.0
    keyword: str | None = None


def search_articles(
    con: sqlite3.Connection,
    *,
    filters: ArticleFilters,
    limit: int = MAX_LIST_LIMIT,
    sort: str = "published_at_desc",
) -> list[dict[str, Any]]:
    where_clause, params, fts_join = _build_articles_where(filters)
    rows = con.execute(
        f"""
        select
            a.id,
            a.title,
            a.url,
            a.source,
            a.category,
            a.seed_section,
            a.topic_label,
            a.published_at,
            a.published_date,
            a.fomo_score,
            a.tickers_json,
            substr(a.content_text, 1, {SNIPPET_LENGTH}) as snippet
        from articles a
        {fts_join}
        where {where_clause}
        order by {_list_order_by_clause(sort)}
        limit ?
        """,
        (*params, _clamp_limit(limit)),
    ).fetchall()
    return [_article_list_item(row) for row in rows]


def latest_articles(
    con: sqlite3.Connection,
    *,
    filters: ArticleFilters,
    limit: int = MAX_LIST_LIMIT,
    sort: str = "published_at_desc",
) -> list[dict[str, Any]]:
    return search_articles(con, filters=filters, limit=limit, sort=sort)


def get_article_by_id(
    con: sqlite3.Connection,
    article_id: int,
    *,
    include_content_html: bool = True,
    include_raw_html: bool = True,
) -> dict[str, Any] | None:
    row = con.execute(
        """
        select
            id,
            title,
            url,
            source,
            category,
            seed_section,
            topic_label,
            published_at,
            published_date,
            fomo_score,
            fomo_explain_json,
            tickers_json,
            content_text,
            content_html,
            raw_html
        from articles
        where id = ?
        limit 1
        """,
        (article_id,),
    ).fetchone()
    if row is None:
        return None
    return _article_detail_item(
        row,
        include_content_html=include_content_html,
        include_raw_html=include_raw_html,
    )


def get_article_by_url(
    con: sqlite3.Connection,
    url: str,
    *,
    include_content_html: bool = True,
    include_raw_html: bool = True,
) -> dict[str, Any] | None:
    row = con.execute(
        """
        select
            id,
            title,
            url,
            source,
            category,
            seed_section,
            topic_label,
            published_at,
            published_date,
            fomo_score,
            fomo_explain_json,
            tickers_json,
            content_text,
            content_html,
            raw_html
        from articles
        where url = ?
        limit 1
        """,
        (url,),
    ).fetchone()
    if row is None:
        return None
    return _article_detail_item(
        row,
        include_content_html=include_content_html,
        include_raw_html=include_raw_html,
    )


def overview_stats(con: sqlite3.Connection, *, filters: ArticleFilters) -> dict[str, Any]:
    where_clause, params, fts_join = _build_articles_where(filters)
    row = con.execute(
        f"""
        select
            count(*) as total_articles,
            count(distinct a.source) as sources_count,
            count(distinct a.seed_section) as sections_count,
            count(distinct a.category) as categories_count,
            max(a.published_at) as latest_published_at
        from articles a
        {fts_join}
        where {where_clause}
        """,
        params,
    ).fetchone()
    return {
        "total_articles": int(row["total_articles"] or 0),
        "sources_count": int(row["sources_count"] or 0),
        "sections_count": int(row["sections_count"] or 0),
        "categories_count": int(row["categories_count"] or 0),
        "latest_published_at": row["latest_published_at"],
    }


def timeline_stats(con: sqlite3.Connection, *, filters: ArticleFilters) -> list[dict[str, Any]]:
    where_clause, params, fts_join = _build_articles_where(filters)
    rows = con.execute(
        f"""
        select
            a.published_date,
            count(*) as article_count,
            round(avg(a.fomo_score), 4) as avg_fomo,
            sum(case when a.fomo_score > 0 then 1 else 0 end) as positive_count,
            sum(case when a.fomo_score < 0 then 1 else 0 end) as negative_count
        from articles a
        {fts_join}
        where {where_clause}
        group by a.published_date
        order by a.published_date asc
        """,
        params,
    ).fetchall()
    return [
        {
            "published_date": row["published_date"],
            "article_count": int(row["article_count"] or 0),
            "avg_fomo": float(row["avg_fomo"] or 0.0),
            "positive_count": int(row["positive_count"] or 0),
            "negative_count": int(row["negative_count"] or 0),
        }
        for row in rows
    ]


def top_tickers(
    con: sqlite3.Connection,
    *,
    filters: ArticleFilters,
    limit: int = 20,
) -> list[dict[str, Any]]:
    counts = _ticker_counter(con, filters=filters)
    clamped_limit = _clamp_top_limit(limit)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:clamped_limit]
    return [{"ticker": ticker, "article_count": article_count} for ticker, article_count in ranked]


def slice_stats(
    con: sqlite3.Connection,
    *,
    filters: ArticleFilters,
    group_by: str,
    sort: str = "count_desc",
    limit: int = 20,
) -> list[dict[str, Any]]:
    if group_by == "ticker":
        return _slice_stats_by_ticker(con, filters=filters, sort=sort, limit=limit)

    expression, value_clause = _sql_group_expression(group_by)
    where_clause, params, fts_join = _build_articles_where(filters)
    if value_clause:
        where_clause = f"{where_clause} and {value_clause}"
    rows = con.execute(
        f"""
        select
            {expression} as grouping_key,
            count(*) as article_count,
            round(avg(a.fomo_score), 4) as avg_fomo,
            max(a.published_at) as latest_published_at,
            sum(case when a.fomo_score > 0 then 1 else 0 end) as positive_count,
            sum(case when a.fomo_score < 0 then 1 else 0 end) as negative_count
        from articles a
        {fts_join}
        where {where_clause}
        group by grouping_key
        """,
        params,
    ).fetchall()
    items = [
        {
            "key": row["grouping_key"],
            "article_count": int(row["article_count"] or 0),
            "avg_fomo": float(row["avg_fomo"] or 0.0),
            "latest_published_at": row["latest_published_at"],
            "positive_count": int(row["positive_count"] or 0),
            "negative_count": int(row["negative_count"] or 0),
        }
        for row in rows
    ]
    return _sort_slice_items(items, sort=sort)[: _clamp_aggregate_limit(limit)]


def facet_counts(
    con: sqlite3.Connection,
    *,
    filters: ArticleFilters,
    fields: tuple[str, ...],
    limit: int = 20,
) -> dict[str, list[dict[str, Any]]]:
    clamped_limit = _clamp_aggregate_limit(limit)
    results: dict[str, list[dict[str, Any]]] = {}
    for field in fields:
        if field == "tickers":
            counts = _ticker_counter(con, filters=filters)
            ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:clamped_limit]
            results[field] = [
                {"value": ticker, "article_count": article_count}
                for ticker, article_count in ranked
            ]
            continue

        expression, value_clause = _sql_facet_expression(field)
        where_clause, params, fts_join = _build_articles_where(filters)
        if value_clause:
            where_clause = f"{where_clause} and {value_clause}"
        rows = con.execute(
            f"""
            select
                {expression} as facet_value,
                count(*) as article_count
            from articles a
            {fts_join}
            where {where_clause}
            group by facet_value
            """,
            params,
        ).fetchall()
        items = [
            {"value": row["facet_value"], "article_count": int(row["article_count"] or 0)}
            for row in rows
        ]
        results[field] = sorted(items, key=lambda item: (-item["article_count"], item["value"]))[
            :clamped_limit
        ]
    return results


def latest_ingest_run(con: sqlite3.Connection) -> dict[str, Any] | None:
    row = con.execute(
        """
        select
            id,
            started_at,
            finished_at,
            mode,
            inserted_count,
            dropped_no_date_count,
            dropped_irrelevant_count,
            dropped_out_of_window_count,
            dedup_dropped_count,
            error
        from ingest_runs
        order by id desc
        limit 1
        """
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def crawl_status(con: sqlite3.Connection) -> dict[str, Any]:
    state_rows = con.execute(
        """
        select
            cs.source,
            cs.section,
            cs.last_published_at,
            cs.last_run_at,
            cs.status,
            cs.error,
            (
                select max(a.published_at)
                from articles a
                where a.source = cs.source and a.seed_section = cs.section
            ) as article_max_published_at
        from crawl_state cs
        order by cs.source asc, cs.section asc
        """
    ).fetchall()
    return {
        "latest_run": latest_ingest_run(con),
        "sections": [dict(row) for row in state_rows],
    }


def get_section_max_published_at(
    con: sqlite3.Connection,
    *,
    source: str,
    section: str,
) -> str | None:
    row = con.execute(
        """
        select max(published_at) as max_published_at
        from articles
        where source = ? and seed_section = ?
        """,
        (source, section),
    ).fetchone()
    if row is None:
        return None
    return row["max_published_at"]


def _build_articles_where(filters: ArticleFilters) -> tuple[str, list[Any], str]:
    clauses = [
        "a.published_date between ? and ?",
        "a.fomo_score >= ?",
    ]
    params: list[Any] = [filters.date_from, filters.date_to, float(filters.min_fomo)]
    fts_join = ""

    if filters.sources:
        placeholders = ", ".join("?" for _ in filters.sources)
        clauses.append(f"a.source in ({placeholders})")
        params.extend(filters.sources)

    if filters.categories:
        placeholders = ", ".join("?" for _ in filters.categories)
        clauses.append(f"a.category in ({placeholders})")
        params.extend(filters.categories)

    if filters.sections:
        placeholders = ", ".join("?" for _ in filters.sections)
        clauses.append(f"a.seed_section in ({placeholders})")
        params.extend(filters.sections)

    if filters.tickers:
        placeholders = ", ".join("?" for _ in filters.tickers)
        clauses.append(
            f"exists (select 1 from json_each(a.tickers_json) where upper(json_each.value) in ({placeholders}))"
        )
        params.extend(ticker.upper() for ticker in filters.tickers)

    keyword = (filters.keyword or "").strip()
    if keyword:
        fts_join = "join articles_fts on articles_fts.rowid = a.id"
        clauses.append("articles_fts match ?")
        params.append(keyword)

    return " and ".join(clauses), params, fts_join


def _article_list_item(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "title": row["title"],
        "url": row["url"],
        "source": row["source"],
        "category": row["category"],
        "seed_section": row["seed_section"],
        "topic_label": row["topic_label"],
        "published_at": row["published_at"],
        "published_date": row["published_date"],
        "fomo_score": float(row["fomo_score"]),
        "tickers": _parse_tickers(row["tickers_json"]),
        "snippet": row["snippet"],
    }


def _article_detail_item(
    row: sqlite3.Row,
    *,
    include_content_html: bool,
    include_raw_html: bool,
) -> dict[str, Any]:
    explain_raw = row["fomo_explain_json"]
    item = {
        "id": int(row["id"]),
        "title": row["title"],
        "url": row["url"],
        "source": row["source"],
        "category": row["category"],
        "seed_section": row["seed_section"],
        "topic_label": row["topic_label"],
        "published_at": row["published_at"],
        "published_date": row["published_date"],
        "fomo_score": float(row["fomo_score"]),
        "fomo_explain": _parse_json_object(explain_raw),
        "fomo_explain_json": explain_raw,
        "tickers": _parse_tickers(row["tickers_json"]),
        "content_text": row["content_text"],
    }
    if include_content_html:
        item["content_html"] = row["content_html"]
    if include_raw_html:
        item["raw_html"] = row["raw_html"]
    return item


def _parse_tickers(raw_value: str | None) -> list[str]:
    if raw_value is None or not raw_value.strip():
        return []
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []

    tickers: list[str] = []
    seen: set[str] = set()
    for item in value:
        ticker = str(item).strip()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def _parse_json_object(raw_value: str | None) -> dict[str, Any]:
    if raw_value is None or not raw_value.strip():
        return {}
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError:
        return {"raw": raw_value}
    if not isinstance(value, dict):
        return {"value": value}
    return value


def _clamp_limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_LIST_LIMIT))


def _clamp_aggregate_limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_AGGREGATE_LIMIT))


def _clamp_top_limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_TOP_TICKERS))


def _list_order_by_clause(sort: str) -> str:
    if sort == "published_at_desc":
        return "a.published_at desc, a.id desc"
    if sort == "published_at_asc":
        return "a.published_at asc, a.id asc"
    if sort == "fomo_desc":
        return "a.fomo_score desc, a.published_at desc, a.id desc"
    if sort == "fomo_asc":
        return "a.fomo_score asc, a.published_at desc, a.id desc"
    raise ValueError(f"Unsupported list sort: {sort}")


def _sql_group_expression(group_by: str) -> tuple[str, str | None]:
    if group_by == "source":
        return "a.source", "a.source is not null and trim(a.source) <> ''"
    if group_by == "category":
        return "a.category", "a.category is not null and trim(a.category) <> ''"
    if group_by == "section":
        return "a.seed_section", "a.seed_section is not null and trim(a.seed_section) <> ''"
    if group_by == "topic":
        return "a.topic_label", "a.topic_label is not null and trim(a.topic_label) <> ''"
    if group_by == "published_date":
        return "a.published_date", None
    raise ValueError(f"Unsupported slice group_by: {group_by}")


def _sql_facet_expression(field: str) -> tuple[str, str | None]:
    if field == "sources":
        return "a.source", "a.source is not null and trim(a.source) <> ''"
    if field == "categories":
        return "a.category", "a.category is not null and trim(a.category) <> ''"
    if field == "sections":
        return "a.seed_section", "a.seed_section is not null and trim(a.seed_section) <> ''"
    if field == "topics":
        return "a.topic_label", "a.topic_label is not null and trim(a.topic_label) <> ''"
    raise ValueError(f"Unsupported facet field: {field}")


def _slice_stats_by_ticker(
    con: sqlite3.Connection,
    *,
    filters: ArticleFilters,
    sort: str,
    limit: int,
) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
        select
            a.id,
            a.published_at,
            a.fomo_score,
            a.tickers_json
        from articles a
        {_build_articles_where(filters)[2]}
        where {_build_articles_where(filters)[0]}
        """,
        _build_articles_where(filters)[1],
    ).fetchall()

    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        for ticker in _parse_tickers(row["tickers_json"]):
            bucket = buckets.setdefault(
                ticker,
                {
                    "key": ticker,
                    "article_count": 0,
                    "fomo_total": 0.0,
                    "latest_published_at": None,
                    "positive_count": 0,
                    "negative_count": 0,
                },
            )
            bucket["article_count"] += 1
            bucket["fomo_total"] += float(row["fomo_score"])
            if (
                bucket["latest_published_at"] is None
                or row["published_at"] > bucket["latest_published_at"]
            ):
                bucket["latest_published_at"] = row["published_at"]
            if float(row["fomo_score"]) > 0:
                bucket["positive_count"] += 1
            elif float(row["fomo_score"]) < 0:
                bucket["negative_count"] += 1

    items = [
        {
            "key": ticker,
            "article_count": int(bucket["article_count"]),
            "avg_fomo": round(bucket["fomo_total"] / bucket["article_count"], 4),
            "latest_published_at": bucket["latest_published_at"],
            "positive_count": int(bucket["positive_count"]),
            "negative_count": int(bucket["negative_count"]),
        }
        for ticker, bucket in buckets.items()
    ]
    return _sort_slice_items(items, sort=sort)[: _clamp_aggregate_limit(limit)]


def _sort_slice_items(items: list[dict[str, Any]], *, sort: str) -> list[dict[str, Any]]:
    if sort == "count_desc":
        return sorted(items, key=lambda item: (-item["article_count"], item["key"]))
    if sort == "count_asc":
        return sorted(items, key=lambda item: (item["article_count"], item["key"]))
    if sort == "avg_fomo_desc":
        return sorted(items, key=lambda item: (-item["avg_fomo"], item["key"]))
    if sort == "avg_fomo_asc":
        return sorted(items, key=lambda item: (item["avg_fomo"], item["key"]))
    if sort == "date_desc":
        return sorted(
            items,
            key=lambda item: (
                item["latest_published_at"] is None,
                "" if item["latest_published_at"] is None else item["latest_published_at"],
                item["key"],
            ),
            reverse=True,
        )
    if sort == "date_asc":
        return sorted(
            items,
            key=lambda item: (
                item["latest_published_at"] is None,
                "" if item["latest_published_at"] is None else item["latest_published_at"],
                item["key"],
            ),
        )
    raise ValueError(f"Unsupported slice sort: {sort}")


def _ticker_counter(con: sqlite3.Connection, *, filters: ArticleFilters) -> Counter[str]:
    where_clause, params, fts_join = _build_articles_where(filters)
    rows = con.execute(
        f"""
        select a.tickers_json
        from articles a
        {fts_join}
        where {where_clause}
        """,
        params,
    ).fetchall()
    counts: Counter[str] = Counter()
    for row in rows:
        for ticker in _parse_tickers(row["tickers_json"]):
            counts[ticker] += 1
    return counts
