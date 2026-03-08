# DATA MODEL (SQLite)

## Tables

### 1) articles
Canonical article table.

Fields:
- id INTEGER PK
- title TEXT NOT NULL
- url TEXT NOT NULL UNIQUE
- source TEXT NOT NULL
- category TEXT
- published_at TEXT NOT NULL  (ISO8601 with +07:00)
- published_date TEXT NOT NULL (YYYY-MM-DD)
- content_text TEXT NOT NULL
- content_html TEXT (optional)
- raw_html TEXT (optional)
- tickers_json TEXT (JSON list of tickers, e.g. ["VCB","HPG"])
- fomo_score REAL NOT NULL  (range [-1,1])
- fomo_explain_json TEXT (JSON object)
- content_sha256 TEXT NOT NULL UNIQUE
- simhash64 INTEGER NOT NULL
- simhash_bucket INTEGER NOT NULL
- created_at TEXT NOT NULL DEFAULT (datetime('now'))

Indexes:
- idx_articles_published_at(published_at)
- idx_articles_source_published_at(source, published_at)
- idx_articles_category_published_at(category, published_at)
- idx_articles_simhash_bucket_date(simhash_bucket, published_date)

### 2) crawl_state
Track resume/backfill status per (source, section).

Fields:
- source TEXT NOT NULL
- section TEXT NOT NULL
- last_published_at TEXT   (ISO8601)
- last_run_at TEXT
- status TEXT  ("ok"|"error"|"running")
- error TEXT
PK: (source, section)

### 3) ingest_runs (optional but recommended)
Fields:
- id INTEGER PK
- started_at TEXT
- finished_at TEXT
- mode TEXT ("backfill"|"incremental"|"manual")
- inserted_count INTEGER
- dropped_no_date_count INTEGER
- dedup_dropped_count INTEGER
- error TEXT

## Full DDL (recommended)
```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS articles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  url TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  category TEXT,
  published_at TEXT NOT NULL,
  published_date TEXT NOT NULL,
  content_text TEXT NOT NULL,
  content_html TEXT,
  raw_html TEXT,
  tickers_json TEXT,
  fomo_score REAL NOT NULL,
  fomo_explain_json TEXT,
  content_sha256 TEXT NOT NULL UNIQUE,
  simhash64 INTEGER NOT NULL,
  simhash_bucket INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_articles_published_at
  ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_source_published_at
  ON articles(source, published_at);
CREATE INDEX IF NOT EXISTS idx_articles_category_published_at
  ON articles(category, published_at);
CREATE INDEX IF NOT EXISTS idx_articles_simhash_bucket_date
  ON articles(simhash_bucket, published_date);

CREATE TABLE IF NOT EXISTS crawl_state (
  source TEXT NOT NULL,
  section TEXT NOT NULL,
  last_published_at TEXT,
  last_run_at TEXT,
  status TEXT,
  error TEXT,
  PRIMARY KEY (source, section)
);

CREATE TABLE IF NOT EXISTS ingest_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT,
  finished_at TEXT,
  mode TEXT,
  inserted_count INTEGER DEFAULT 0,
  dropped_no_date_count INTEGER DEFAULT 0,
  dedup_dropped_count INTEGER DEFAULT 0,
  error TEXT
);

-- FTS5: title + content_text
CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts
USING fts5(title, content_text, content='articles', content_rowid='id');

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON articles BEGIN
  INSERT INTO articles_fts(rowid, title, content_text)
  VALUES (new.id, new.title, new.content_text);
END;

CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON articles BEGIN
  INSERT INTO articles_fts(articles_fts, rowid, title, content_text)
  VALUES('delete', old.id, old.title, old.content_text);
END;

CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON articles BEGIN
  INSERT INTO articles_fts(articles_fts, rowid, title, content_text)
  VALUES('delete', old.id, old.title, old.content_text);
  INSERT INTO articles_fts(rowid, title, content_text)
  VALUES (new.id, new.title, new.content_text);
END;