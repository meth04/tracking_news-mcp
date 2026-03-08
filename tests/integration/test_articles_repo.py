import json

from app.db.articles_repo import ArticleRecord, insert_article
from app.db.conn import connect
from app.db.ingest_runs_repo import IngestRunCounts, finish_ingest_run, start_ingest_run
from app.db.init_db import init_db
from app.dedup.hashers import compute_content_sha256, compute_simhash64, compute_simhash_bucket


def _build_article(url: str, text: str) -> ArticleRecord:
    simhash64 = compute_simhash64(text)
    return ArticleRecord(
        title="VIC tăng sốc",
        url=url,
        source="vnexpress",
        category="Chứng khoán",
        seed_section="thi-truong-chung-khoan",
        topic_label="stocks",
        published_at="2026-03-07T10:00:00+07:00",
        published_date="2026-03-07",
        content_text=text,
        content_html="<article>text</article>",
        raw_html=None,
        tickers=["VIC"],
        fomo_score=0.95,
        fomo_explain_json=json.dumps({"final": 0.95}),
        content_sha256=compute_content_sha256(text),
        simhash64=simhash64,
        simhash_bucket=compute_simhash_bucket(simhash64),
    )


def test_insert_article_persists_and_fts_triggers(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    with connect(db_path) as con:
        result = insert_article(
            con, _build_article("https://example.com/a", "VIC tăng sốc lập đỉnh")
        )
        assert result.inserted is True

        row = con.execute(
            "select title, tickers_json, fomo_score from articles where id = ?",
            (result.article_id,),
        ).fetchone()
        assert row["title"] == "VIC tăng sốc"
        assert json.loads(row["tickers_json"]) == ["VIC"]
        assert row["fomo_score"] == 0.95

        fts_row = con.execute(
            "select rowid from articles_fts where articles_fts match ?",
            ("VIC",),
        ).fetchone()
        assert int(fts_row["rowid"]) == result.article_id


def test_insert_article_drops_duplicates_and_tracks_ingest_run(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    with connect(db_path) as con:
        run_id = start_ingest_run(con)
        first = insert_article(
            con, _build_article("https://example.com/a", "VIC tăng sốc lập đỉnh")
        )
        second = insert_article(
            con, _build_article("https://example.com/b", "VIC tăng sốc lập đỉnh")
        )
        counts = IngestRunCounts(
            inserted_count=int(first.inserted), dedup_dropped_count=int(not second.inserted)
        )
        finish_ingest_run(con, run_id, counts)

        assert first.inserted is True
        assert second.inserted is False
        assert second.reason in {"exact_sha256", "near_simhash"}

        run = con.execute(
            "select inserted_count, dedup_dropped_count from ingest_runs where id = ?",
            (run_id,),
        ).fetchone()
        assert run["inserted_count"] == 1
        assert run["dedup_dropped_count"] == 1
