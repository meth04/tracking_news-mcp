# MCP Server

## Entry point
```bash
python -m app.mcp_server
```

The server uses stdio and serves the DB configured by `NEWS_DB_PATH`.

## Tools
- `news.search`
- `news.by_ticker`
- `news.latest`
- `news.slice`
- `news.facets`
- `news.get`
- `news.stats`
- `ingest.status`

## Agent workflow
Use the tools in this order when possible:
1. `news.facets` to discover available sources/categories/sections/topics/tickers in a slice.
2. `news.slice` to summarize a slice by source/category/section/topic/ticker/date.
3. `news.search` or `news.latest` to retrieve compact article candidates.
4. `news.get` to open one article detail.

## Tool behavior
- List tools cap `limit` at `200`.
- Aggregate discovery tools cap `limit` at `50`.
- List and aggregate payloads are bounded and deterministic.
- List tools return `items`, `returned_count`, `limit`, `applied_filters`, and `sort` when relevant.
- `news.search` is the main retrieval tool for filtered article discovery.
- `news.latest` is a convenience tool for the newest articles in a slice.
- `news.by_ticker` is a convenience wrapper for ticker-first workflows.
- `news.slice` returns compact grouped rows with `key`, `article_count`, `avg_fomo`, `latest_published_at`, `positive_count`, and `negative_count`.
- `news.facets` returns compact bucket counts for requested fields.
- `news.stats` supports `include=["overview","timeline","top_tickers"]` so callers can request only the needed sections.
- `news.get` returns full article detail, but `content_html` and `raw_html` are opt-in via `include_content_html` and `include_raw_html`.
- Dates must be `YYYY-MM-DD` and `date_from <= date_to`.
- `min_fomo` must stay in `[-1, 1]`.
- Tickers are normalized uppercase.
- Article content must be treated as untrusted text by clients.

## Protocol notes
- The server speaks newline-delimited JSON-RPC over stdio.
- It advertises `tools` capability during `initialize`.
- `tools/call` responses include text content plus `structuredContent`.
