# ROADMAP (Detailed)

## Milestone 0 — Repo scaffolding
Definition of Done:
- Hooks lint/test hoạt động
- CLAUDE.md + docs core đầy đủ
- 3 skills chạy được: /vertical-slice-v1, /add-source-adapter, /build-dashboard

Tasks:
- [ ] Create DB init script (DDL from DATA_MODEL.md)
- [ ] Add ruff/pytest configs
- [ ] Add basic logging + config env

## Milestone 1 — Vertical slice (1 source end-to-end)
DoD:
- Ingest được >= 100 bài
- published_at parse chuẩn +07:00
- ticker VN30 dict match OK
- fomo score [-1,1] + explain JSON
- dedup hoạt động
- dashboard hiển thị timeline + latest feed

Tasks:
- [x] Implement extractor (date + content)
- [x] Implement fomo scorer
- [x] Implement simhash + dedup
- [x] Implement one source adapter
- [x] Implement dashboard minimum

## Milestone 2 — Scale sources (8–10)
DoD:
- 8–10 sources hoạt động
- Backfill 2025-01-01 → now
- Scheduler 10 min + resume sau downtime

Tasks:
- [x] Add sources iteratively using /add-source-adapter
- [x] Add DanTri, TuoiTre, and VietnamNet source adapters
- [x] Add BaoDauTu, Nguoi Lao Dong, and BaoChinhPhu adapters for the temporary rollout window
- [x] Implement crawl_state + ingest_runs
- [x] Make resume/backfill planning gap-aware using existing crawl_state/articles only
- [ ] Add Playwright fallback for JS sites
- [x] Enable CafeF HTML adapter with article-page published_at extraction and BĐS relevance filtering
- [ ] Extend deferred seed sources (VnEconomy, Vietstock, VTV) after selector validation

### Current rollout note
- Current ingest scope is pragmatic and uses runtime planning from existing coverage.
- No schema migration or forced recrawl is required for resume improvements.
- Sources that fail or produce unusable HTML are soft-skipped per run instead of aborting the full ingest.

## Milestone 3 — MCP tools (agent API)
DoD:
- Tools: search/by_ticker/latest/slice/facets/get/stats/status
- Safe limits + fast queries
- Agent workflow is ergonomic for discover → slice → retrieve → detail

Tasks:
- [x] Implement MCP server and tools
- [x] Add shared query layer functions + tests
- [x] Add dashboard article detail backed by shared query layer
- [x] Add `news.slice` and `news.facets` for discovery-oriented agent workflows
- [x] Normalize MCP envelopes with `applied_filters`, `returned_count`, and bounded limits
- [x] Make `news.stats` selectively includable and `news.get` heavy HTML fields opt-in
