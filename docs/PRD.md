# PRD — VN News MCP

## 1) Problem
Cần một nguồn tin tức (VN báo lớn) để phục vụ hệ thống multi-agent phân tích thị trường chứng khoán VN.
Hệ thống phải:
- crawl được tin theo thời gian
- lưu được full text
- trích VN30 tickers
- chấm FOMO mạnh tay
- query nhanh theo keyword/ticker/chủ đề/thời gian
- có dashboard quan sát nhanh

## 2) Goals (V1)
- Crawl HTML từ 2025-01-01 → hiện tại, sau đó chạy định kỳ 10 phút.
- Chỉ giữ bài có `published_at` (VN timezone). Không có ngày đăng => bỏ.
- Dedup theo nội dung: chỉ giữ 1 bài canonical.
- Fields phải có:
  - title, url, source, published_at, category, content_full(text), tickers(VN30), fomo_score
  - + optional content_html/raw_html để debug
- MCP tools để agent query.
- Dashboard: timeline + filters + top tickers + latest feed + search.

## 3) Non-goals (V1)
- Không cần mô hình ML phức tạp cho FOMO (rule-based trước).
- Không cần NER ngoài dictionary VN30.
- Không cần distributed scaling / cloud infra (chạy máy cá nhân).
- Không cần crawl 100% bài của toàn site (ưu tiên đa dạng nguồn và ổn định).

## 4) Users
- Operator/Developer (bạn) vận hành crawler.
- Multi-agent consumer (query qua MCP tools).
- Người theo dõi dashboard.

## 5) Requirements
### Functional
- Backfill đến 2025-01-01, dừng khi lùi quá mốc.
- Resume: nếu tắt rồi bật, tự fill phần thiếu dựa vào max(published_at) trong DB.
- Incremental: mỗi 10 phút crawl bài mới nhất.
- Query:
  - search by keyword (FTS)
  - filter by date range/source/category/min fomo
  - filter by ticker
  - sort by published_at desc
- FOMO:
  - output in [-1, 1]
  - “mạnh tay” (polarized), có explain JSON

### Non-functional
- Robust: site đổi layout thì dễ debug (lưu content_html/raw_html, có tests fixtures).
- Token-efficient for Claude Code: docs rõ, tasks nhỏ, hooks chạy lint/test.
- Performance target:
  - 10k–100k bài trong SQLite vẫn query < 1–2s cho dashboard cơ bản.

## 6) Success metrics
- ≥ 8 nguồn hoạt động ổn định.
- Backfill hoàn tất, scheduler chạy 24h không crash.
- Dashboard load nhanh, filter/search usable.
- Agent dùng MCP tools truy vấn đúng và nhanh.

## 7) Risks
- Anti-bot / JS rendering: cần Playwright fallback.
- Layout thay đổi: cần adapter per-site + fixtures tests.
- DB file phình to khi lưu raw_html: default off; bật khi debug.