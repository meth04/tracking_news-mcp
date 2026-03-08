import httpx

from app.db.conn import connect
from app.db.crawl_state_repo import upsert_crawl_state
from app.db.ingest_runs_repo import insert_ingest_section_runs, start_ingest_run
from app.db.init_db import init_db
from app.extract.http_client import build_client
from app.ingest.pipeline import CafeFRebuildPipeline, RunOncePipeline
from app.ingest.rebuild_cafef import reset_db_in_place
from app.ingest.run_once import (
    _resolve_article_fetch_workers,
    _resolve_article_rate_limit_seconds,
    _resolve_enabled_sources,
)
from app.sources import ArticleCandidate, SectionSeed, SkipArticleError
from app.sources.cafef import CafeFAdapter
from app.sources.registry import get_seed_sources


class MissingDateAdapter:
    source_name = "missing-date"
    sections = (SectionSeed("main", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        return ["https://example.com/article"]

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        return ArticleCandidate(
            title="Bài không có ngày đăng",
            url=url,
            source=self.source_name,
            category="Kinh doanh",
            published_at=None,
            content_text="Nội dung hợp lệ nhưng thiếu published_at.",
        )


class IrrelevantAdapter:
    source_name = "irrelevant"
    sections = (SectionSeed("main", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        return ["https://example.com/article"]

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        raise SkipArticleError("out of scope")


class PaginatedAdapter:
    source_name = "paginated"
    sections = (SectionSeed("main", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/page/2"):
            return [
                "https://example.com/article-2",
                "https://example.com/article-3",
            ]
        if base_url.endswith("/page/3"):
            return ["https://example.com/article-3"]
        return [
            "https://example.com/article-1",
            "https://example.com/article-2",
        ]

    def discover_next_page_url(
        self, html: str, *, section: SectionSeed, current_url: str
    ) -> str | None:
        if current_url.endswith("/page/3"):
            return None
        if current_url.endswith("/page/2"):
            return "https://example.com/list/page/3"
        return "https://example.com/list/page/2"

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        slug = url.rsplit("/", 1)[-1]
        article_number = slug.rsplit("-", 1)[-1]
        content_by_number = {
            "1": "alpha brewery logistics seaport fertilizer hedging copper soybean derivative warehouse cashflow",
            "2": "quantum satellite semiconductor photon cryogenic wafer lithography compiler kernel robotics autonomy",
            "3": "mangrove estuary monsoon sediment fisheries coral biodiversity wetland conservation shoreline delta",
        }
        return ArticleCandidate(
            title=f"Title for {slug}",
            url=url,
            source=self.source_name,
            category="Kinh doanh",
            published_at="2026-03-07T08:00:00+07:00",
            content_text=content_by_number[article_number],
        )


class StalePaginatedAdapter:
    source_name = "stale-paginated"
    sections = (SectionSeed("kinh-doanh", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/page/1") or base_url.endswith("/list"):
            return ["https://example.com/article-1"]
        return []

    def discover_next_page_url(
        self, html: str, *, section: SectionSeed, current_url: str
    ) -> str | None:
        if current_url.endswith("/page/5"):
            return None
        if current_url.endswith("/page/4"):
            return "https://example.com/list/page/5"
        if current_url.endswith("/page/3"):
            return "https://example.com/list/page/4"
        if current_url.endswith("/page/2"):
            return "https://example.com/list/page/3"
        return "https://example.com/list/page/2"

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        return ArticleCandidate(
            title="Single fresh article",
            url=url,
            source=self.source_name,
            category="Kinh doanh",
            published_at="2026-03-07T08:00:00+07:00",
            content_text="Fresh business article with VCB BID bank credit margin lending.",
        )


class ParallelArticleAdapter:
    source_name = "parallel-articles"
    sections = (SectionSeed("main", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        return [
            "https://example.com/article-1",
            "https://example.com/article-2",
            "https://example.com/article-3",
            "https://example.com/article-4",
        ]

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        slug = url.rsplit("/", 1)[-1]
        content_by_slug = {
            "article-1": "brokerage derivatives liquidity margin lending SSI HCM VND VNINDEX breadth catalyst.",
            "article-2": "steel export furnace HPG HSG NKG inventory ore spread shipping demand recovery cycle.",
            "article-3": "bank CASA NIM provisioning VCB BID CTG treasury yield deposit growth credit demand outlook.",
            "article-4": "public investment expressway airport EPC disbursement asphalt cement contractor backlog tender pipeline.",
        }
        return ArticleCandidate(
            title=f"Parallel {slug}",
            url=url,
            source=self.source_name,
            category="Kinh doanh",
            published_at="2026-03-07T08:00:00+07:00",
            content_text=content_by_slug[slug],
        )


class OutOfWindowListAdapter:
    source_name = "out-of-window-list"
    sections = (SectionSeed("main", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/page/2"):
            return ["https://example.com/old-2"]
        if base_url.endswith("/page/1") or base_url.endswith("/list"):
            return ["https://example.com/old-1"]
        return []

    def list_page_published_at_values(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/page/2"):
            return ["2025-12-31T10:00:00+07:00"]
        return ["2025-12-30T10:00:00+07:00"]

    def discover_next_page_url(
        self, html: str, *, section: SectionSeed, current_url: str
    ) -> str | None:
        if current_url.endswith("/page/3"):
            return None
        if current_url.endswith("/page/2"):
            return "https://example.com/list/page/3"
        return "https://example.com/list/page/2"

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        return ArticleCandidate(
            title="Old article",
            url=url,
            source=self.source_name,
            category="Kinh doanh",
            published_at="2025-12-31T08:00:00+07:00",
            content_text="Old article outside ingest window.",
        )


class RebuildCafeFAdapter(CafeFAdapter):
    sections = (
        SectionSeed("thi-truong-chung-khoan", "https://cafef.vn/thi-truong-chung-khoan.chn"),
    )

    def timelinelist_url(self, *, section: SectionSeed, page_number: int) -> str:
        return f"https://cafef.vn/timelinelist/18831/{page_number}.chn"

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/2.chn"):
            return [
                "https://cafef.vn/old-article-188260101000000002.chn",
                "https://cafef.vn/old-article-188260101000000003.chn",
            ]
        return [
            "https://cafef.vn/fresh-article-188260307000000001.chn",
            "https://cafef.vn/fresh-article-188260307000000001.chn",
        ]

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        if "old-article" in url:
            return ArticleCandidate(
                title="Old CafeF article",
                url=url,
                source="cafef",
                category="Thị trường chứng khoán",
                published_at="2025-12-31T08:00:00+07:00",
                content_text="SSI old article before rebuild window.",
            )
        return ArticleCandidate(
            title="Fresh CafeF article",
            url=url,
            source="cafef",
            category="Thị trường chứng khoán",
            published_at="2026-03-07T08:00:00+07:00",
            content_text="SSI fresh canonical content inside rebuild window.",
        )


class RebuildCafeFMixedOldPageAdapter(CafeFAdapter):
    sections = (
        SectionSeed("thi-truong-chung-khoan", "https://cafef.vn/thi-truong-chung-khoan.chn"),
    )

    def timelinelist_url(self, *, section: SectionSeed, page_number: int) -> str:
        return f"https://cafef.vn/timelinelist/18831/{page_number}.chn"

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/3.chn"):
            return ["https://cafef.vn/third-page-article-188260307000000004.chn"]
        if base_url.endswith("/2.chn"):
            return [
                "https://cafef.vn/old-article-188260101000000002.chn",
                "https://cafef.vn/fresh-article-188260307000000003.chn",
            ]
        return ["https://cafef.vn/fresh-article-188260307000000001.chn"]

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        if "old-article" in url:
            return ArticleCandidate(
                title="Old CafeF article",
                url=url,
                source="cafef",
                category="Thị trường chứng khoán",
                published_at="2025-12-31T08:00:00+07:00",
                content_text="SSI old article before rebuild window.",
            )
        return ArticleCandidate(
            title="Fresh CafeF article",
            url=url,
            source="cafef",
            category="Thị trường chứng khoán",
            published_at="2026-03-07T08:00:00+07:00",
            content_text=f"Canonical content for {url} with SSI liquidity rally.",
        )


def test_pipeline_drops_articles_without_published_at(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            text="<html>ok</html>",
            request=request,
        )
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(MissingDateAdapter(), client=client).run(con)

        assert result.counts.dropped_no_date_count == 1
        assert result.counts.dropped_irrelevant_count == 0
        row = con.execute("select count(*) as count from articles").fetchone()
        assert row["count"] == 0


def test_pipeline_tracks_irrelevant_articles_separately(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            text="<html>ok</html>",
            request=request,
        )
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(IrrelevantAdapter(), client=client).run(con)

        assert result.counts.dropped_no_date_count == 0
        assert result.counts.dropped_irrelevant_count == 1
        row = con.execute("select count(*) as count from articles").fetchone()
        assert row["count"] == 0


def test_pipeline_discovers_multiple_pages_and_dedupes_urls(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            text="<html>ok</html>",
            request=request,
        )
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(PaginatedAdapter(), client=client).run(con)

        assert result.fetched_urls == 3
        assert result.processed_urls == 3
        assert result.counts.inserted_count == 3
        assert len(result.section_stats) == 1
        assert result.section_stats[0].pages_scanned == 3
        assert result.section_stats[0].discovered_urls == 5
        assert result.section_stats[0].unique_urls == 3

        rows = con.execute(
            "select url, seed_section, topic_label from articles order by url"
        ).fetchall()
        assert [row["seed_section"] for row in rows] == ["main", "main", "main"]
        assert [row["topic_label"] for row in rows] == [None, None, None]


def test_pipeline_stops_after_consecutive_stale_pages(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(StalePaginatedAdapter(), client=client).run(con)

        assert result.fetched_urls == 1
        assert result.processed_urls == 1
        assert result.counts.inserted_count == 1
        assert result.section_stats[0].pages_scanned == 4


def test_pipeline_stops_after_consecutive_out_of_window_list_pages(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(OutOfWindowListAdapter(), client=client).run(con)

        assert result.section_stats[0].pages_scanned == 2
        assert result.counts.dropped_out_of_window_count == 2
        assert result.section_stats[0].dropped_out_of_window_count == 2


def test_pipeline_parallel_fetch_keeps_single_writer_counts_consistent(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(
            ParallelArticleAdapter(), client=client, article_fetch_workers=4
        ).run(con)

        assert result.fetched_urls == 4
        assert result.processed_urls == 4
        assert result.counts.inserted_count == 4
        assert result.section_stats[0].processed_urls == 4
        assert result.section_stats[0].inserted_count == 4


def test_cafef_rebuild_pipeline_stops_after_full_old_page_and_keeps_in_window_articles(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = CafeFRebuildPipeline(
            adapter=RebuildCafeFAdapter(),
            client=client,
            page_cap=5,
            old_page_streak=1,
        ).run(con)

        assert result.section_stats[0].pages_scanned == 2
        assert result.processed_urls == 3
        assert result.counts.inserted_count == 1
        assert result.counts.dropped_out_of_window_count == 2

        rows = con.execute(
            "select source, seed_section, published_date from articles order by published_at"
        ).fetchall()
        assert [(row["source"], row["seed_section"], row["published_date"]) for row in rows] == [
            ("cafef", "thi-truong-chung-khoan", "2026-03-07")
        ]


def test_cafef_rebuild_pipeline_does_not_stop_on_mixed_old_page(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = CafeFRebuildPipeline(
            adapter=RebuildCafeFMixedOldPageAdapter(),
            client=client,
            page_cap=3,
            old_page_streak=1,
        ).run(con)

        assert result.section_stats[0].pages_scanned == 3
        assert result.counts.inserted_count == 3
        assert result.counts.dropped_out_of_window_count == 1


def test_reset_db_in_place_clears_tables_without_replacing_file(tmp_path):
    db_file = tmp_path / "news.db"
    db_path = str(db_file)
    init_db(db_path)

    with connect(db_path) as con:
        con.execute(
            "insert into articles (title, url, source, category, seed_section, topic_label, published_at, published_date, content_text, content_html, raw_html, tickers_json, fomo_score, fomo_explain_json, content_sha256, simhash64, simhash_bucket) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "title",
                "https://example.com/a",
                "cafef",
                "cat",
                "section",
                "topic",
                "2026-03-07T10:00:00+07:00",
                "2026-03-07",
                "content",
                None,
                None,
                "[]",
                0.1,
                "{}",
                "sha1",
                1,
                1,
            ),
        )
        run_id = start_ingest_run(con)
        con.execute(
            "insert into crawl_state (source, section, status) values (?, ?, ?)",
            ("cafef", "section", "ok"),
        )
        con.execute(
            "insert into ingest_section_runs (run_id, source, section) values (?, ?, ?)",
            (run_id, "cafef", "section"),
        )
        con.execute(
            "insert into cafef_timelinelist_raw (zone_id, page_number, page_url, item_rank, article_url) values (?, ?, ?, ?, ?)",
            ("18831", 1, "https://cafef.vn/timelinelist/18831/1.chn", 1, "https://cafef.vn/a.chn"),
        )
        con.commit()

    inode_before = db_file.stat().st_ino
    reset_db_in_place(db_path)

    assert db_file.exists()
    assert db_file.stat().st_ino == inode_before

    with connect(db_path) as con:
        for table_name in (
            "articles",
            "crawl_state",
            "ingest_section_runs",
            "ingest_runs",
            "cafef_timelinelist_raw",
        ):
            row = con.execute(f"select count(*) as count from {table_name}").fetchone()
            assert row["count"] == 0


def test_resolve_enabled_sources_prefers_env_and_supports_cafef_only(monkeypatch):
    monkeypatch.delenv("ENABLED_SOURCES", raising=False)
    monkeypatch.delenv("CAFEF_ONLY_MODE", raising=False)
    assert _resolve_enabled_sources() is None

    monkeypatch.setenv("CAFEF_ONLY_MODE", "1")
    assert _resolve_enabled_sources() == "cafef"

    monkeypatch.setenv("ENABLED_SOURCES", "vnexpress,cafef")
    assert _resolve_enabled_sources() == "vnexpress,cafef"


def test_resolve_cafef_only_runtime_tuning(monkeypatch):
    monkeypatch.delenv("CAFEF_ONLY_MODE", raising=False)
    assert _resolve_article_fetch_workers("cafef") == 1
    assert _resolve_article_rate_limit_seconds("cafef") is None

    monkeypatch.setenv("CAFEF_ONLY_MODE", "1")
    assert _resolve_article_fetch_workers("cafef") == 4
    assert _resolve_article_rate_limit_seconds("cafef") == 0.0
    assert _resolve_article_fetch_workers("vnexpress") == 1
    assert _resolve_article_rate_limit_seconds("vnexpress") is None


def test_seed_sources_include_full_cafef_rollout():
    cafef_sections = [item.seed.name for item in get_seed_sources() if item.source_name == "cafef"]

    assert cafef_sections == [
        "thi-truong-chung-khoan",
        "bat-dong-san",
        "doanh-nghiep",
        "tai-chinh-ngan-hang",
        "tai-chinh-quoc-te",
        "vi-mo-dau-tu",
        "thi-truong",
        "song",
    ]


def test_pipeline_persists_section_run_stats_and_per_section_crawl_state(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )
    with build_client(transport=transport) as client, connect(db_path) as con:
        result = RunOncePipeline(PaginatedAdapter(), client=client).run(con)
        run_id = start_ingest_run(con)
        insert_ingest_section_runs(con, run_id, "paginated", result.section_stats)
        for stat in result.section_stats:
            upsert_crawl_state(
                con,
                source="paginated",
                section=stat.section_name,
                status="ok",
                last_published_at=stat.latest_published_at,
            )

        row = con.execute(
            "select source, section, pages_scanned, processed_urls, inserted_count, latest_published_at from ingest_section_runs"
        ).fetchone()
        assert row["source"] == "paginated"
        assert row["section"] == "main"
        assert row["pages_scanned"] == 3
        assert row["processed_urls"] == 3
        assert row["inserted_count"] == 3
        assert row["latest_published_at"] == "2026-03-07T08:00:00+07:00"

        crawl_state = con.execute(
            "select last_published_at from crawl_state where source = 'paginated' and section = 'main'"
        ).fetchone()
        assert crawl_state["last_published_at"] == "2026-03-07T08:00:00+07:00"
