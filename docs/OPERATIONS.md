# Operations

## Resume behavior
- Runtime planning is gap-aware per `(source, section)`.
- Resume boundary priority:
  1. `crawl_state.last_published_at`
  2. `max(articles.published_at)` for `(source, seed_section)`
- If no coverage exists, the configured ingest window is used.
- If coverage exists, the planner uses a forward gap window with a small overlap.
- Watermarks are never cleared by `None` writes.

## Run once
```bash
python -m app.ingest.run_once
```

Expected behavior:
- logs planned `date_from/date_to` per section
- only crawls the missing forward window
- does not force a historical recrawl
- keeps old watermark on failed sections

## Smoke checks
```bash
python -m app.db.init_db
python -m app.ingest.run_once
streamlit run apps/dashboard_streamlit.py
python -m app.mcp_server
pytest -q
```

## Non-goals for current operation
- No schema changes
- No DB reset
- No rebuild workflow
- No forced recrawl of the full historical range
