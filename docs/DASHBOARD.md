# Dashboard (Streamlit) — Requirements

## Goal
A lightweight dashboard for basic analysis of crawled news:
- timeline by day
- breakdown by source/category
- top tickers
- filter + search
- latest feed
- article detail on demand

## Current pages
1) Overview
- total articles
- sources count
- sections count
- latest published_at
- last ingest run summary

2) Timeline
- articles/day
- avg fomo/day

3) Explore / Feed
- date range
- source/category/section
- min fomo
- keyword full-text search (FTS5)
- optional ticker filter
- latest feed table with metadata only

4) Article detail
- title
- source/category/section/topic
- published_at
- tickers
- fomo_score
- parsed `fomo_explain_json`
- `content_text`
- expanders for `content_html` and `raw_html`

5) Tickers
- top tickers frequency in selected range

## Performance rules
- Always query with `WHERE published_date BETWEEN ...`
- Use `@st.cache_data(ttl=30)` for repeated queries
- Do not load full `content_text` in list queries
- Load article detail only when a user selects an article
- Limit result rows to <= 200 in the UI
