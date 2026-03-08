from app.db.conn import connect
from app.db.init_db import init_db
from app.db.query_service import (
    ArticleFilters,
    crawl_status,
    facet_counts,
    get_article_by_id,
    get_article_by_url,
    get_section_max_published_at,
    latest_articles,
    latest_ingest_run,
    overview_stats,
    search_articles,
    slice_stats,
    timeline_stats,
    top_tickers,
)


def _insert_article(
    con,
    *,
    article_id: str,
    title: str,
    url: str,
    source: str,
    category: str,
    seed_section: str,
    topic_label: str | None,
    published_at: str,
    published_date: str,
    content_text: str,
    tickers_json: str,
    fomo_score: float,
    fomo_explain_json: str,
) -> None:
    con.execute(
        """
        insert into articles (
            title, url, source, category, seed_section, topic_label, published_at, published_date,
            content_text, content_html, raw_html, tickers_json,
            fomo_score, fomo_explain_json, content_sha256, simhash64, simhash_bucket
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            url,
            source,
            category,
            seed_section,
            topic_label,
            published_at,
            published_date,
            content_text,
            f"<p>{article_id}</p>",
            f"<html>{article_id}</html>",
            tickers_json,
            fomo_score,
            fomo_explain_json,
            f"sha-{article_id}",
            100 + len(article_id),
            10 + len(article_id),
        ),
    )


def _seed_db(db_path: str) -> None:
    init_db(db_path)
    with connect(db_path) as con:
        _insert_article(
            con,
            article_id="a1",
            title="VIC tăng sốc",
            url="https://example.com/vic-1",
            source="cafef",
            category="Thị trường chứng khoán",
            seed_section="thi-truong-chung-khoan",
            topic_label="stocks",
            published_at="2026-03-07T10:00:00+07:00",
            published_date="2026-03-07",
            content_text="VIC tăng sốc lập đỉnh mới với dòng tiền mạnh.",
            tickers_json='["VIC","VHM"]',
            fomo_score=0.9,
            fomo_explain_json='{"final":0.9,"signals":["surge"]}',
        )
        _insert_article(
            con,
            article_id="a2",
            title="Ngân hàng giữ nhịp",
            url="https://example.com/vcb",
            source="vnexpress",
            category="Kinh doanh",
            seed_section="kinh-doanh",
            topic_label="business",
            published_at="2026-03-06T09:00:00+07:00",
            published_date="2026-03-06",
            content_text="VCB và BID dẫn dắt nhóm ngân hàng.",
            tickers_json='["VCB","BID"]',
            fomo_score=0.3,
            fomo_explain_json='{"final":0.3}',
        )
        _insert_article(
            con,
            article_id="a3",
            title="Áp lực chốt lời VIC",
            url="https://example.com/vic-2",
            source="cafef",
            category="Thị trường chứng khoán",
            seed_section="thi-truong-chung-khoan",
            topic_label="stocks",
            published_at="2026-03-06T15:00:00+07:00",
            published_date="2026-03-06",
            content_text="VIC chịu áp lực chốt lời sau nhịp tăng nóng.",
            tickers_json='["VIC"]',
            fomo_score=-0.4,
            fomo_explain_json='{"final":-0.4}',
        )
        _insert_article(
            con,
            article_id="a4",
            title="Bất động sản hồi phục",
            url="https://example.com/vhm",
            source="dantri",
            category="Bất động sản",
            seed_section="bat-dong-san",
            topic_label="property",
            published_at="2026-03-05T08:30:00+07:00",
            published_date="2026-03-05",
            content_text="VHM hưởng lợi từ tín hiệu hồi phục bất động sản.",
            tickers_json='["VHM"]',
            fomo_score=0.7,
            fomo_explain_json='{"final":0.7}',
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
                4,
                0,
                0,
                0,
                0,
                None,
            ),
        )
        con.execute(
            """
            insert into crawl_state (source, section, last_published_at, last_run_at, status, error)
            values (?, ?, ?, ?, ?, ?)
            """,
            (
                "cafef",
                "thi-truong-chung-khoan",
                "2026-03-07T10:00:00+07:00",
                "2026-03-07T12:01:00+07:00",
                "ok",
                None,
            ),
        )
        con.commit()


def test_query_service_search_detail_and_stats(tmp_path):
    db_path = str(tmp_path / "news.db")
    _seed_db(db_path)

    with connect(db_path) as con:
        filters = ArticleFilters(
            date_from="2026-03-01",
            date_to="2026-03-08",
            sources=("cafef", "vnexpress"),
            min_fomo=-1.0,
            keyword="VIC",
        )

        search_results = search_articles(con, filters=filters, limit=50, sort="fomo_asc")
        assert [item["url"] for item in search_results] == [
            "https://example.com/vic-2",
            "https://example.com/vic-1",
        ]
        assert "content_text" not in search_results[0]
        assert search_results[0]["tickers"] == ["VIC"]
        assert search_results[1]["snippet"].startswith("VIC tăng sốc")

        latest_results = latest_articles(
            con,
            filters=ArticleFilters(
                date_from="2026-03-01",
                date_to="2026-03-08",
                tickers=("VHM",),
            ),
            limit=20,
            sort="published_at_asc",
        )
        assert [item["url"] for item in latest_results] == [
            "https://example.com/vhm",
            "https://example.com/vic-1",
        ]

        detail_by_url = get_article_by_url(
            con,
            "https://example.com/vic-1",
            include_content_html=False,
            include_raw_html=False,
        )
        assert detail_by_url is not None
        assert detail_by_url["content_text"] == "VIC tăng sốc lập đỉnh mới với dòng tiền mạnh."
        assert detail_by_url["fomo_explain"]["final"] == 0.9
        assert "content_html" not in detail_by_url
        assert "raw_html" not in detail_by_url

        detail_by_id = get_article_by_id(con, detail_by_url["id"])
        assert detail_by_id is not None
        assert detail_by_id["content_html"] == "<p>a1</p>"
        assert detail_by_id["raw_html"] == "<html>a1</html>"

        overview = overview_stats(
            con,
            filters=ArticleFilters(date_from="2026-03-01", date_to="2026-03-08"),
        )
        assert overview == {
            "total_articles": 4,
            "sources_count": 3,
            "sections_count": 3,
            "categories_count": 3,
            "latest_published_at": "2026-03-07T10:00:00+07:00",
        }

        timeline = timeline_stats(
            con,
            filters=ArticleFilters(date_from="2026-03-01", date_to="2026-03-08"),
        )
        assert timeline == [
            {
                "published_date": "2026-03-05",
                "article_count": 1,
                "avg_fomo": 0.7,
                "positive_count": 1,
                "negative_count": 0,
            },
            {
                "published_date": "2026-03-06",
                "article_count": 2,
                "avg_fomo": -0.05,
                "positive_count": 1,
                "negative_count": 1,
            },
            {
                "published_date": "2026-03-07",
                "article_count": 1,
                "avg_fomo": 0.9,
                "positive_count": 1,
                "negative_count": 0,
            },
        ]

        tickers = top_tickers(
            con,
            filters=ArticleFilters(date_from="2026-03-01", date_to="2026-03-08"),
            limit=4,
        )
        assert tickers == [
            {"ticker": "VHM", "article_count": 2},
            {"ticker": "VIC", "article_count": 2},
            {"ticker": "BID", "article_count": 1},
            {"ticker": "VCB", "article_count": 1},
        ]

        latest_run = latest_ingest_run(con)
        assert latest_run is not None
        assert latest_run["mode"] == "manual"
        assert latest_run["inserted_count"] == 4

        status = crawl_status(con)
        assert status["latest_run"]["inserted_count"] == 4
        assert status["sections"] == [
            {
                "source": "cafef",
                "section": "thi-truong-chung-khoan",
                "last_published_at": "2026-03-07T10:00:00+07:00",
                "last_run_at": "2026-03-07T12:01:00+07:00",
                "status": "ok",
                "error": None,
                "article_max_published_at": "2026-03-07T10:00:00+07:00",
            }
        ]

        assert (
            get_section_max_published_at(
                con,
                source="cafef",
                section="thi-truong-chung-khoan",
            )
            == "2026-03-07T10:00:00+07:00"
        )


def test_query_service_slice_stats_and_facets(tmp_path):
    db_path = str(tmp_path / "news.db")
    _seed_db(db_path)

    with connect(db_path) as con:
        filters = ArticleFilters(date_from="2026-03-01", date_to="2026-03-08")

        assert slice_stats(
            con, filters=filters, group_by="source", sort="count_desc", limit=10
        ) == [
            {
                "key": "cafef",
                "article_count": 2,
                "avg_fomo": 0.25,
                "latest_published_at": "2026-03-07T10:00:00+07:00",
                "positive_count": 1,
                "negative_count": 1,
            },
            {
                "key": "dantri",
                "article_count": 1,
                "avg_fomo": 0.7,
                "latest_published_at": "2026-03-05T08:30:00+07:00",
                "positive_count": 1,
                "negative_count": 0,
            },
            {
                "key": "vnexpress",
                "article_count": 1,
                "avg_fomo": 0.3,
                "latest_published_at": "2026-03-06T09:00:00+07:00",
                "positive_count": 1,
                "negative_count": 0,
            },
        ]

        assert [
            row["key"]
            for row in slice_stats(
                con, filters=filters, group_by="category", sort="avg_fomo_desc", limit=10
            )
        ] == [
            "Bất động sản",
            "Kinh doanh",
            "Thị trường chứng khoán",
        ]
        assert [
            row["key"]
            for row in slice_stats(
                con, filters=filters, group_by="section", sort="count_desc", limit=10
            )
        ] == [
            "thi-truong-chung-khoan",
            "bat-dong-san",
            "kinh-doanh",
        ]
        assert [
            row["key"]
            for row in slice_stats(
                con, filters=filters, group_by="topic", sort="count_desc", limit=10
            )
        ] == [
            "stocks",
            "business",
            "property",
        ]
        assert [
            row["key"]
            for row in slice_stats(
                con, filters=filters, group_by="published_date", sort="date_asc", limit=10
            )
        ] == [
            "2026-03-05",
            "2026-03-06",
            "2026-03-07",
        ]
        assert slice_stats(
            con, filters=filters, group_by="ticker", sort="count_desc", limit=10
        ) == [
            {
                "key": "VHM",
                "article_count": 2,
                "avg_fomo": 0.8,
                "latest_published_at": "2026-03-07T10:00:00+07:00",
                "positive_count": 2,
                "negative_count": 0,
            },
            {
                "key": "VIC",
                "article_count": 2,
                "avg_fomo": 0.25,
                "latest_published_at": "2026-03-07T10:00:00+07:00",
                "positive_count": 1,
                "negative_count": 1,
            },
            {
                "key": "BID",
                "article_count": 1,
                "avg_fomo": 0.3,
                "latest_published_at": "2026-03-06T09:00:00+07:00",
                "positive_count": 1,
                "negative_count": 0,
            },
            {
                "key": "VCB",
                "article_count": 1,
                "avg_fomo": 0.3,
                "latest_published_at": "2026-03-06T09:00:00+07:00",
                "positive_count": 1,
                "negative_count": 0,
            },
        ]

        facets = facet_counts(
            con,
            filters=ArticleFilters(
                date_from="2026-03-01",
                date_to="2026-03-08",
                sources=("cafef",),
            ),
            fields=("categories", "sections", "topics", "tickers"),
            limit=10,
        )
        assert facets == {
            "categories": [{"value": "Thị trường chứng khoán", "article_count": 2}],
            "sections": [{"value": "thi-truong-chung-khoan", "article_count": 2}],
            "topics": [{"value": "stocks", "article_count": 2}],
            "tickers": [
                {"value": "VIC", "article_count": 2},
                {"value": "VHM", "article_count": 1},
            ],
        }


def test_query_service_rejects_invalid_sort_and_group_by(tmp_path):
    db_path = str(tmp_path / "news.db")
    _seed_db(db_path)

    with connect(db_path) as con:
        filters = ArticleFilters(date_from="2026-03-01", date_to="2026-03-08")

        try:
            search_articles(con, filters=filters, sort="bad_sort")
        except ValueError as exc:
            assert str(exc) == "Unsupported list sort: bad_sort"
        else:
            raise AssertionError("Expected invalid list sort to raise")

        try:
            slice_stats(con, filters=filters, group_by="bad_group", sort="count_desc")
        except ValueError as exc:
            assert str(exc) == "Unsupported slice group_by: bad_group"
        else:
            raise AssertionError("Expected invalid group_by to raise")

        try:
            slice_stats(con, filters=filters, group_by="source", sort="bad_sort")
        except ValueError as exc:
            assert str(exc) == "Unsupported slice sort: bad_sort"
        else:
            raise AssertionError("Expected invalid slice sort to raise")
