from app.db.conn import connect
from app.db.init_db import init_db


def test_dashboard_queries_work_with_cafef_articles_filters(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    with connect(db_path) as con:
        con.execute(
            """
            insert into articles (
                title, url, source, category, seed_section, topic_label, published_at, published_date,
                content_text, content_html, raw_html, tickers_json,
                fomo_score, fomo_explain_json, content_sha256, simhash64, simhash_bucket
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "VIC tăng sốc",
                "https://cafef.vn/vic-1.chn",
                "cafef",
                "Thị trường chứng khoán",
                "thi-truong-chung-khoan",
                "stocks",
                "2026-03-07T10:00:00+07:00",
                "2026-03-07",
                "VIC tăng sốc lập đỉnh",
                None,
                None,
                '["VIC"]',
                0.9,
                '{"final":0.9}',
                "sha1",
                123,
                1,
            ),
        )
        con.execute(
            """
            insert into articles (
                title, url, source, category, seed_section, topic_label, published_at, published_date,
                content_text, content_html, raw_html, tickers_json,
                fomo_score, fomo_explain_json, content_sha256, simhash64, simhash_bucket
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Tin bất động sản",
                "https://cafef.vn/bds-1.chn",
                "cafef",
                "Bất động sản",
                "bat-dong-san",
                "real_estate",
                "2026-03-07T11:00:00+07:00",
                "2026-03-07",
                "Bài viết khác",
                None,
                None,
                '["VHM"]',
                0.2,
                '{"final":0.2}',
                "sha2",
                456,
                2,
            ),
        )
        con.execute(
            """
            insert into ingest_runs (
                started_at, finished_at, mode, inserted_count,
                dropped_no_date_count, dropped_irrelevant_count,
                dropped_out_of_window_count, dedup_dropped_count, error
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-03-07T12:00:00+07:00",
                "2026-03-07T12:01:00+07:00",
                "manual",
                2,
                0,
                0,
                0,
                0,
                None,
            ),
        )
        con.commit()

        timeline = con.execute(
            """
            select a.published_date, count(*) as n
            from articles a
            where a.source = ?
              and a.published_date between ? and ?
              and a.fomo_score >= ?
              and a.seed_section in (?)
              and a.category in (?)
            group by a.published_date
            order by a.published_date
            """,
            (
                "cafef",
                "2026-01-01",
                "2026-12-31",
                -1.0,
                "thi-truong-chung-khoan",
                "Thị trường chứng khoán",
            ),
        ).fetchall()
        assert len(timeline) == 1
        assert timeline[0]["n"] == 1

        section_breakdown = con.execute(
            """
            select a.seed_section, count(*) as n
            from articles a
            where a.source = ?
              and a.published_date between ? and ?
              and a.fomo_score >= ?
            group by a.seed_section
            order by n desc, a.seed_section asc
            """,
            ("cafef", "2026-01-01", "2026-12-31", -1.0),
        ).fetchall()
        assert [row["seed_section"] for row in section_breakdown] == [
            "bat-dong-san",
            "thi-truong-chung-khoan",
        ]

        latest = con.execute(
            """
            select a.title, a.seed_section, a.category, a.url
            from articles a
            join articles_fts on articles_fts.rowid = a.id
            where a.source = ?
              and a.published_date between ? and ?
              and a.fomo_score >= ?
              and articles_fts match ?
            order by a.published_at desc
            limit 200
            """,
            ("cafef", "2026-01-01", "2026-12-31", -1.0, "VIC"),
        ).fetchall()
        assert latest[0]["title"] == "VIC tăng sốc"
        assert latest[0]["seed_section"] == "thi-truong-chung-khoan"
        assert latest[0]["category"] == "Thị trường chứng khoán"
        assert latest[0]["url"] == "https://cafef.vn/vic-1.chn"
