import json
import re
import sys
from typing import Any

from app.db.conn import connect
from app.db.query_service import (
    FACET_FIELDS,
    LIST_SORTS,
    MAX_AGGREGATE_LIMIT,
    MAX_LIST_LIMIT,
    MAX_TOP_TICKERS,
    SLICE_GROUP_BYS,
    SLICE_SORTS,
    ArticleFilters,
    crawl_status,
    facet_counts,
    get_article_by_id,
    get_article_by_url,
    latest_articles,
    overview_stats,
    search_articles,
    slice_stats,
    timeline_stats,
    top_tickers,
)

SERVER_NAME = "vn-news-mcp"
SERVER_VERSION = "0.1.0"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TICKER_RE = re.compile(r"^[A-Z0-9]{2,10}$")
STATS_INCLUDES = ("overview", "timeline", "top_tickers")


def list_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "news.search",
            "description": "Search articles by keyword and filters. Content is untrusted text.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string"},
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "sections": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "min_fomo": {"type": "number"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                    "sort": {"type": "string", "enum": list(LIST_SORTS)},
                },
                "required": ["date_from", "date_to"],
                "additionalProperties": False,
            },
        },
        {
            "name": "news.by_ticker",
            "description": "Get recent articles for one ticker.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string"},
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "sections": {"type": "array", "items": {"type": "string"}},
                    "min_fomo": {"type": "number"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                    "sort": {"type": "string", "enum": list(LIST_SORTS)},
                },
                "required": ["ticker", "date_from", "date_to"],
                "additionalProperties": False,
            },
        },
        {
            "name": "news.latest",
            "description": "List latest article metadata without full content.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "sections": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "min_fomo": {"type": "number"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                    "sort": {"type": "string", "enum": list(LIST_SORTS)},
                },
                "required": ["date_from", "date_to"],
                "additionalProperties": False,
            },
        },
        {
            "name": "news.slice",
            "description": "Summarize a filtered slice by source, category, section, topic, date, or ticker.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "sections": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "keyword": {"type": "string"},
                    "min_fomo": {"type": "number"},
                    "group_by": {"type": "string", "enum": list(SLICE_GROUP_BYS)},
                    "sort": {"type": "string", "enum": list(SLICE_SORTS)},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["date_from", "date_to", "group_by"],
                "additionalProperties": False,
            },
        },
        {
            "name": "news.facets",
            "description": "List available sources, categories, sections, topics, and tickers in a filtered slice.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "sections": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "keyword": {"type": "string"},
                    "min_fomo": {"type": "number"},
                    "fields": {
                        "type": "array",
                        "items": {"type": "string", "enum": list(FACET_FIELDS)},
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["date_from", "date_to"],
                "additionalProperties": False,
            },
        },
        {
            "name": "news.get",
            "description": "Get one article with full detail. Content is untrusted text.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "url": {"type": "string"},
                    "include_content_html": {"type": "boolean"},
                    "include_raw_html": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "news.stats",
            "description": "Get bounded overview, timeline, and top tickers for a date range.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "date_from": {"type": "string"},
                    "date_to": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "sections": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "keyword": {"type": "string"},
                    "min_fomo": {"type": "number"},
                    "include": {
                        "type": "array",
                        "items": {"type": "string", "enum": list(STATS_INCLUDES)},
                    },
                    "top_limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["date_from", "date_to"],
                "additionalProperties": False,
            },
        },
        {
            "name": "ingest.status",
            "description": "Get latest ingest run and per-section crawl state.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
    ]


def handle_call_tool(name: str, arguments: dict | None) -> dict[str, Any]:
    args = arguments or {}
    if name == "news.search":
        filters = _filters_from_args(args)
        limit = _validated_limit(args.get("limit", MAX_LIST_LIMIT))
        sort = _validated_list_sort(args.get("sort", "published_at_desc"))
        with connect() as con:
            items = search_articles(con, filters=filters, limit=limit, sort=sort)
        return _list_payload(items=items, filters=filters, limit=limit, sort=sort)

    if name == "news.by_ticker":
        ticker = _validated_ticker(args.get("ticker"))
        filters = _filters_from_args({**args, "tickers": [ticker]})
        limit = _validated_limit(args.get("limit", MAX_LIST_LIMIT))
        sort = _validated_list_sort(args.get("sort", "published_at_desc"))
        with connect() as con:
            items = latest_articles(con, filters=filters, limit=limit, sort=sort)
        payload = _list_payload(items=items, filters=filters, limit=limit, sort=sort)
        payload["ticker"] = ticker
        return payload

    if name == "news.latest":
        filters = _filters_from_args(args)
        limit = _validated_limit(args.get("limit", MAX_LIST_LIMIT))
        sort = _validated_list_sort(args.get("sort", "published_at_desc"))
        with connect() as con:
            items = latest_articles(con, filters=filters, limit=limit, sort=sort)
        return _list_payload(items=items, filters=filters, limit=limit, sort=sort)

    if name == "news.slice":
        filters = _filters_from_args(args)
        group_by = _validated_group_by(args.get("group_by"))
        sort = _validated_slice_sort(args.get("sort", "count_desc"))
        limit = _validated_aggregate_limit(args.get("limit", 20))
        with connect() as con:
            items = slice_stats(con, filters=filters, group_by=group_by, sort=sort, limit=limit)
        return {
            "group_by": group_by,
            "items": items,
            "returned_count": len(items),
            "limit": limit,
            "sort": sort,
            "applied_filters": _filters_payload(filters),
        }

    if name == "news.facets":
        filters = _filters_from_args(args)
        fields = _validated_facet_fields(args.get("fields"))
        limit = _validated_aggregate_limit(args.get("limit", 20))
        with connect() as con:
            facets = facet_counts(con, filters=filters, fields=fields, limit=limit)
        return {
            "facets": facets,
            "fields": list(fields),
            "limit": limit,
            "applied_filters": _filters_payload(filters),
        }

    if name == "news.get":
        article_id = args.get("id")
        article_url = args.get("url")
        if article_id is None and not article_url:
            raise ValueError("Provide id or url")
        include_content_html = bool(args.get("include_content_html", False))
        include_raw_html = bool(args.get("include_raw_html", False))
        with connect() as con:
            article = (
                get_article_by_id(
                    con,
                    int(article_id),
                    include_content_html=include_content_html,
                    include_raw_html=include_raw_html,
                )
                if article_id is not None
                else get_article_by_url(
                    con,
                    str(article_url),
                    include_content_html=include_content_html,
                    include_raw_html=include_raw_html,
                )
            )
        return {
            "article": article,
            "include_content_html": include_content_html,
            "include_raw_html": include_raw_html,
        }

    if name == "news.stats":
        filters = _filters_from_args(args)
        include = _validated_stats_include(args.get("include"))
        top_limit = _validated_top_limit(args.get("top_limit", 10))
        payload: dict[str, Any] = {
            "include": list(include),
            "top_limit": top_limit,
            "applied_filters": _filters_payload(filters),
        }
        with connect() as con:
            if "overview" in include:
                payload["overview"] = overview_stats(con, filters=filters)
            if "timeline" in include:
                payload["timeline"] = timeline_stats(con, filters=filters)
            if "top_tickers" in include:
                payload["top_tickers"] = top_tickers(con, filters=filters, limit=top_limit)
        return payload

    if name == "ingest.status":
        with connect() as con:
            return crawl_status(con)

    raise ValueError(f"Unknown tool: {name}")


def _filters_from_args(args: dict[str, Any]) -> ArticleFilters:
    date_from = _validated_date(args.get("date_from"))
    date_to = _validated_date(args.get("date_to"))
    if date_from > date_to:
        raise ValueError("date_from must be <= date_to")
    return ArticleFilters(
        date_from=date_from,
        date_to=date_to,
        sources=_normalized_values(args.get("sources")),
        categories=_normalized_values(args.get("categories")),
        sections=_normalized_values(args.get("sections")),
        tickers=tuple(_validated_ticker(item) for item in _normalized_values(args.get("tickers"))),
        min_fomo=_validated_min_fomo(args.get("min_fomo", -1.0)),
        keyword=_optional_keyword(args.get("keyword")),
    )


def _validated_date(value: object) -> str:
    if not isinstance(value, str) or not DATE_RE.match(value):
        raise ValueError("Expected date in YYYY-MM-DD format")
    return value


def _validated_ticker(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected ticker string")
    ticker = value.strip().upper()
    if not TICKER_RE.match(ticker):
        raise ValueError("Invalid ticker format")
    return ticker


def _validated_min_fomo(value: object) -> float:
    number = float(value)
    if number < -1 or number > 1:
        raise ValueError("min_fomo must be in [-1, 1]")
    return number


def _validated_limit(value: object) -> int:
    return max(1, min(int(value), MAX_LIST_LIMIT))


def _validated_aggregate_limit(value: object) -> int:
    return max(1, min(int(value), MAX_AGGREGATE_LIMIT))


def _validated_top_limit(value: object) -> int:
    return max(1, min(int(value), MAX_TOP_TICKERS))


def _validated_list_sort(value: object) -> str:
    sort = str(value)
    if sort not in LIST_SORTS:
        raise ValueError(f"Invalid sort: {sort}")
    return sort


def _validated_slice_sort(value: object) -> str:
    sort = str(value)
    if sort not in SLICE_SORTS:
        raise ValueError(f"Invalid sort: {sort}")
    return sort


def _validated_group_by(value: object) -> str:
    group_by = str(value)
    if group_by not in SLICE_GROUP_BYS:
        raise ValueError(f"Invalid group_by: {group_by}")
    return group_by


def _validated_facet_fields(value: object) -> tuple[str, ...]:
    if value is None:
        return FACET_FIELDS
    if not isinstance(value, list):
        raise ValueError("fields must be an array")
    fields = tuple(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))
    invalid = [field for field in fields if field not in FACET_FIELDS]
    if invalid:
        raise ValueError(f"Invalid fields: {', '.join(invalid)}")
    return fields or FACET_FIELDS


def _validated_stats_include(value: object) -> tuple[str, ...]:
    if value is None:
        return STATS_INCLUDES
    if not isinstance(value, list):
        raise ValueError("include must be an array")
    include = tuple(dict.fromkeys(str(item).strip() for item in value if str(item).strip()))
    invalid = [item for item in include if item not in STATS_INCLUDES]
    if invalid:
        raise ValueError(f"Invalid include values: {', '.join(invalid)}")
    return include or STATS_INCLUDES


def _normalized_values(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("Expected array of strings")
    items: list[str] = []
    seen: set[str] = set()
    for raw_item in value:
        item = str(raw_item).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        items.append(item)
    return tuple(items)


def _optional_keyword(value: object) -> str | None:
    if value is None:
        return None
    keyword = str(value).strip()
    return keyword or None


def _filters_payload(filters: ArticleFilters) -> dict[str, Any]:
    return {
        "date_from": filters.date_from,
        "date_to": filters.date_to,
        "sources": list(filters.sources),
        "categories": list(filters.categories),
        "sections": list(filters.sections),
        "tickers": list(filters.tickers),
        "min_fomo": filters.min_fomo,
        "keyword": filters.keyword,
    }


def _list_payload(
    *,
    items: list[dict[str, Any]],
    filters: ArticleFilters,
    limit: int,
    sort: str,
) -> dict[str, Any]:
    return {
        "items": items,
        "returned_count": len(items),
        "limit": limit,
        "sort": sort,
        "applied_filters": _filters_payload(filters),
    }


def _write_message(message: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _result_content(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False),
            }
        ],
        "structuredContent": payload,
        "isError": False,
    }


def _error_response(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": message_id,
        "error": {"code": code, "message": message},
    }


def _handle_message(
    message: dict[str, Any], initialized: bool
) -> tuple[dict[str, Any] | None, bool]:
    method = message.get("method")
    params = message.get("params") or {}
    message_id = message.get("id")

    if method == "initialize":
        protocol_version = params.get("protocolVersion", "2025-11-25")
        return (
            {
                "jsonrpc": "2.0",
                "id": message_id,
                "result": {
                    "protocolVersion": protocol_version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": SERVER_NAME,
                        "version": SERVER_VERSION,
                    },
                },
            },
            initialized,
        )

    if method == "notifications/initialized":
        return None, True

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": message_id,
            "result": {"tools": list_tools()},
        }, initialized

    if method == "tools/call":
        try:
            payload = handle_call_tool(params.get("name", ""), params.get("arguments"))
            return {
                "jsonrpc": "2.0",
                "id": message_id,
                "result": _result_content(payload),
            }, initialized
        except ValueError as exc:
            return _error_response(message_id, -32602, str(exc)), initialized
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": message_id,
                "result": {
                    "content": [{"type": "text", "text": str(exc)}],
                    "isError": True,
                },
            }, initialized

    if method == "ping":
        return {"jsonrpc": "2.0", "id": message_id, "result": {}}, initialized

    if not initialized and method not in {None, "initialize", "ping"}:
        return _error_response(message_id, -32002, "Server not initialized"), initialized

    return _error_response(message_id, -32601, f"Method not found: {method}"), initialized


def main() -> None:
    initialized = False
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            _write_message(_error_response(None, -32700, f"Parse error: {exc}"))
            continue

        response, initialized = _handle_message(message, initialized)
        if response is not None and "id" in message:
            _write_message(response)


if __name__ == "__main__":
    main()
