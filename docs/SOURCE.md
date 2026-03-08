
# SOURCES (V1 list)

## Principles
- Prefer reputable Vietnamese sources.
- Crawl HTML (no RSS).
- Each source should define 2–5 **sections** (category list pages) that are already time-sorted.
- Use rate limit + jitter; allow Playwright fallback via `USE_PLAYWRIGHT=1`.

## Seed sources (full 15 URL)
1) https://vnexpress.net/kinh-doanh
2) https://vnexpress.net/bat-dong-san
3) https://dantri.com.vn/kinh-doanh.htm
4) https://tuoitre.vn/kinh-doanh.htm
5) https://vietnamnet.vn/kinh-doanh
6) https://cafef.vn/thi-truong-chung-khoan.chn
7) https://cafef.vn/bat-dong-san.chn
8) https://vneconomy.vn/thi-truong-chung-khoan.htm
9) https://vietstock.vn/chung-khoan.htm
10) https://baodautu.vn/tai-chinh-chung-khoan-d6/
11) https://vtv.vn/kinh-te/tai-chinh.htm
12) https://nld.com.vn/kinh-te.htm
13) https://laodong.vn/kinh-doanh
14) https://baochinhphu.vn/chinh-sach-va-cuoc-song/chinh-sach-moi.htm
15) https://mof.gov.vn/tin-tuc-tai-chinh

## Rollout status (temporary window: 2026-02-20 → today)
- Enabled now: `vnexpress`, `dantri`, `tuoitre`, `vietnamnet`, `baodautu`, `nld`, `baochinhphu`, `cafef`
- Deferred pending more adapter work: `vneconomy`, `vietstock`, `vtv`
- Soft-skipped in current HTML-only rollout: `laodong`, `mof`
- `vnexpress` now applies article-page relevance filtering to cut bất động sản lifestyle/PR drift.
- `cafef` is enabled with HTML-only parsing for `thi-truong-chung-khoan` and `bat-dong-san`.
- Registry is now the source of truth for enabled vs skipped seed sources.

## Section strategy (examples)
> Bạn sẽ cập nhật URL cụ thể theo thời gian, đây là khung để Claude Code “biết cần điền gì”.

### VnExpress
- Kinh doanh
- Bất động sản

### DanTri
- Kinh doanh — https://dantri.com.vn/kinh-doanh.htm

### TuoiTre
- Kinh doanh — https://tuoitre.vn/kinh-doanh.htm

### VietnamNet
- Kinh doanh — https://vietnamnet.vn/kinh-doanh

### CafeF
- Chứng khoán
- Tài chính
- Bất động sản

### Vietstock
- Chứng khoán (tin thị trường)
- Doanh nghiệp

## Per-source adapter checklist
- list page:
  - extract article URLs (absolute)
  - avoid duplicates
  - support pagination / load more
- article page:
  - extract title
  - extract published_at (VN tz) — if missing => drop
  - extract category if possible
  - extract content_text + (optional) html/raw
- resilience:
  - prefer readability/trafilatura for main text
  - fallback selectors only if needed
  - tests with fixtures (saved HTML)

## Anti-bot / JS rendering
- Default: httpx + parse HTML
- If JS-heavy: `USE_PLAYWRIGHT=1` and implement `fetch_html_playwright(url)` in a shared helper.