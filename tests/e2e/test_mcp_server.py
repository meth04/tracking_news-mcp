from app.db.conn import connect
from app.db.init_db import init_db
from app.mcp_server import handle_call_tool


def _seed_db(db_path: str) -> None:
    init_db(db_path)
    with connect(db_path) as con:
        rows = [
            (
                "Article 0",
                "https://example.com/0",
                "cafef",
                "Thị trường chứng khoán",
                "thi-truong-chung-khoan",
                "stocks",
                "2026-03-01T10:00:00+07:00",
                "2026-03-01",
                "Content 0 with VIC.",
                "<p>0</p>",
                "<html>0</html>",
                '["VIC"]',
                0.2,
                '{"final":0.2}',
                "sha-0",
                100,
                10,
            ),
            (
                "Article 1",
                "https://example.com/1",
                "cafef",
                "Bất động sản",
                "bat-dong-san",
                "property",
                "2026-03-02T10:00:00+07:00",
                "2026-03-02",
                "Content 1 with VHM and VIC.",
                "<p>1</p>",
                "<html>1</html>",
                '["VHM","VIC"]',
                0.8,
                '{"final":0.8}',
                "sha-1",
                101,
                11,
            ),
            (
                "Article 2",
                "https://example.com/2",
                "vnexpress",
                "Kinh doanh",
                "kinh-doanh",
                "business",
                "2026-03-03T10:00:00+07:00",
                "2026-03-03",
                "Content 2 with VCB.",
                "<p>2</p>",
                "<html>2</html>",
                '["VCB"]',
                -0.4,
                '{"final":-0.4}',
                "sha-2",
                102,
                12,
            ),
        ]
        for row in rows:
            con.execute(
                """
                insert into articles (
                    title, url, source, category, seed_section, topic_label, published_at, published_date,
                    content_text, content_html, raw_html, tickers_json,
                    fomo_score, fomo_explain_json, content_sha256, simhash64, simhash_bucket
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
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
                "2026-03-08T12:00:00+07:00",
                "2026-03-08T12:01:00+07:00",
                "manual",
                3,
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
                "2026-03-02T10:00:00+07:00",
                "2026-03-08T12:01:00+07:00",
                "ok",
                None,
            ),
        )
        con.commit()


def test_mcp_tools_return_stable_payloads_and_limits(tmp_path, monkeypatch):
    db_path = str(tmp_path / "news.db")
    _seed_db(db_path)
    monkeypatch.setenv("NEWS_DB_PATH", db_path)

    search_result = handle_call_tool(
        "news.search",
        {
            "keyword": "VIC",
            "date_from": "2026-03-01",
            "date_to": "2026-03-09",
            "limit": 999,
            "sort": "fomo_desc",
        },
    )
    assert search_result["limit"] == 200
    assert search_result["returned_count"] == 2
    assert search_result["sort"] == "fomo_desc"
    assert [item["url"] for item in search_result["items"]] == [
        "https://example.com/1",
        "https://example.com/0",
    ]
    assert "content_text" not in search_result["items"][0]
    assert search_result["applied_filters"]["keyword"] == "VIC"

    latest_result = handle_call_tool(
        "news.latest",
        {
            "date_from": "2026-03-01",
            "date_to": "2026-03-09",
            "tickers": ["VIC"],
            "limit": 2,
            "sort": "published_at_asc",
        },
    )
    assert latest_result["limit"] == 2
    assert latest_result["returned_count"] == 2
    assert [item["url"] for item in latest_result["items"]] == [
        "https://example.com/0",
        "https://example.com/1",
    ]

    by_ticker_result = handle_call_tool(
        "news.by_ticker",
        {
            "ticker": "vic",
            "date_from": "2026-03-01",
            "date_to": "2026-03-09",
        },
    )
    assert by_ticker_result["ticker"] == "VIC"
    assert by_ticker_result["applied_filters"]["tickers"] == ["VIC"]

    slice_result = handle_call_tool(
        "news.slice",
        {
            "date_from": "2026-03-01",
            "date_to": "2026-03-09",
            "group_by": "ticker",
            "sort": "count_desc",
            "limit": 10,
        },
    )
    assert slice_result["group_by"] == "ticker"
    assert slice_result["returned_count"] == 3
    assert slice_result["items"][0] == {
        "key": "VIC",
        "article_count": 2,
        "avg_fomo": 0.5,
        "latest_published_at": "2026-03-02T10:00:00+07:00",
        "positive_count": 2,
        "negative_count": 0,
    }

    facets_result = handle_call_tool(
        "news.facets",
        {
            "date_from": "2026-03-01",
            "date_to": "2026-03-09",
            "sources": ["cafef", "cafef"],
            "fields": ["categories", "tickers", "tickers"],
        },
    )
    assert facets_result["fields"] == ["categories", "tickers"]
    assert facets_result["applied_filters"]["sources"] == ["cafef"]
    assert facets_result["facets"] == {
        "categories": [
            {"value": "Bất động sản", "article_count": 1},
            {"value": "Thị trường chứng khoán", "article_count": 1},
        ],
        "tickers": [
            {"value": "VIC", "article_count": 2},
            {"value": "VHM", "article_count": 1},
        ],
    }

    get_result = handle_call_tool("news.get", {"url": "https://example.com/2"})
    assert get_result["article"]["content_text"] == "Content 2 with VCB."
    assert get_result["article"]["tickers"] == ["VCB"]
    assert "content_html" not in get_result["article"]
    assert "raw_html" not in get_result["article"]

    get_with_html_result = handle_call_tool(
        "news.get",
        {
            "url": "https://example.com/2",
            "include_content_html": True,
            "include_raw_html": True,
        },
    )
    assert get_with_html_result["article"]["content_html"] == "<p>2</p>"
    assert get_with_html_result["article"]["raw_html"] == "<html>2</html>"

    stats_result = handle_call_tool(
        "news.stats",
        {
            "date_from": "2026-03-01",
            "date_to": "2026-03-09",
            "include": ["overview"],
            "top_limit": 1,
        },
    )
    assert stats_result["include"] == ["overview"]
    assert stats_result["overview"]["total_articles"] == 3
    assert "timeline" not in stats_result
    assert "top_tickers" not in stats_result

    status_result = handle_call_tool("ingest.status", {})
    assert status_result["latest_run"]["inserted_count"] == 3
    assert status_result["sections"][0]["source"] == "cafef"


def test_mcp_tools_reject_invalid_inputs(tmp_path, monkeypatch):
    db_path = str(tmp_path / "news.db")
    _seed_db(db_path)
    monkeypatch.setenv("NEWS_DB_PATH", db_path)

    invalid_cases = [
        (
            "news.search",
            {"date_from": "2026-03-10", "date_to": "2026-03-09"},
            "date_from must be <= date_to",
        ),
        (
            "news.search",
            {"date_from": "2026-03-01", "date_to": "2026-03-09", "sort": "bad_sort"},
            "Invalid sort: bad_sort",
        ),
        (
            "news.slice",
            {"date_from": "2026-03-01", "date_to": "2026-03-09", "group_by": "bad_group"},
            "Invalid group_by: bad_group",
        ),
        (
            "news.slice",
            {
                "date_from": "2026-03-01",
                "date_to": "2026-03-09",
                "group_by": "source",
                "sort": "bad_sort",
            },
            "Invalid sort: bad_sort",
        ),
        (
            "news.facets",
            {"date_from": "2026-03-01", "date_to": "2026-03-09", "fields": ["bad_field"]},
            "Invalid fields: bad_field",
        ),
        (
            "news.stats",
            {"date_from": "2026-03-01", "date_to": "2026-03-09", "include": ["bad_part"]},
            "Invalid include values: bad_part",
        ),
    ]

    for tool_name, arguments, message in invalid_cases:
        try:
            handle_call_tool(tool_name, arguments)
        except ValueError as exc:
            assert str(exc) == message
        else:
            raise AssertionError(f"Expected {tool_name} to reject invalid inputs")
