import httpx

from app.db.conn import connect
from app.db.crawl_state_repo import get_crawl_state_last_published_at, upsert_crawl_state
from app.db.init_db import init_db
from app.extract.http_client import build_client
from app.ingest.pipeline import RunOncePipeline
from app.ingest.planner import build_section_plan
from app.sources import ArticleCandidate, SectionSeed


class ResumeAwareAdapter:
    source_name = "resume-aware"
    sections = (SectionSeed("main", "https://example.com/list"),)

    def parse_list_page(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/page/2"):
            return ["https://example.com/article-old"]
        return [
            "https://example.com/article-new",
            "https://example.com/article-at-boundary",
        ]

    def discover_next_page_url(
        self, html: str, *, section: SectionSeed, current_url: str
    ) -> str | None:
        if current_url.endswith("/page/2"):
            return None
        return "https://example.com/list/page/2"

    def list_page_published_at_values(self, html: str, *, base_url: str) -> list[str]:
        if base_url.endswith("/page/2"):
            return ["2026-03-02T09:00:00+07:00"]
        return ["2026-04-07T08:00:00+07:00", "2026-03-03T10:00:00+07:00"]

    def parse_article(self, url: str, html: str) -> ArticleCandidate:
        published_at_by_url = {
            "https://example.com/article-new": "2026-04-07T08:00:00+07:00",
            "https://example.com/article-at-boundary": "2026-03-03T10:00:00+07:00",
            "https://example.com/article-old": "2026-03-02T09:00:00+07:00",
        }
        return ArticleCandidate(
            title=url.rsplit("/", 1)[-1],
            url=url,
            source=self.source_name,
            category="Kinh doanh",
            published_at=published_at_by_url[url],
            content_text=f"Content for {url}",
        )


def test_upsert_crawl_state_preserves_existing_watermark_when_incoming_none(tmp_path):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    with connect(db_path) as con:
        upsert_crawl_state(
            con,
            source="vnexpress",
            section="kinh-doanh",
            status="ok",
            last_published_at="2026-03-03T10:00:00+07:00",
        )
        upsert_crawl_state(
            con,
            source="vnexpress",
            section="kinh-doanh",
            status="running",
            last_published_at=None,
        )

        assert (
            get_crawl_state_last_published_at(
                con,
                source="vnexpress",
                section="kinh-doanh",
            )
            == "2026-03-03T10:00:00+07:00"
        )


def test_section_plan_falls_back_to_articles_max_published_at(tmp_path):
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
                "Existing",
                "https://example.com/existing",
                "resume-aware",
                "Kinh doanh",
                "main",
                None,
                "2026-03-03T10:00:00+07:00",
                "2026-03-03",
                "existing content",
                None,
                None,
                "[]",
                0.1,
                "{}",
                "sha-existing",
                11,
                1,
            ),
        )
        con.commit()

        plan = build_section_plan(
            con, "resume-aware", SectionSeed("main", "https://example.com/list")
        )

        assert plan.resume_from_published_at == "2026-03-03T10:00:00+07:00"
        assert plan.has_existing_coverage is True
        assert plan.date_from == "2026-03-03"


def test_resume_gap_plan_only_inserts_articles_after_watermark(tmp_path, monkeypatch):
    db_path = str(tmp_path / "news.db")
    init_db(db_path)

    monkeypatch.setenv("INGEST_DATE_TO", "2026-04-07")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>ok</html>", request=request)
    )

    with build_client(transport=transport) as client, connect(db_path) as con:
        upsert_crawl_state(
            con,
            source="resume-aware",
            section="main",
            status="ok",
            last_published_at="2026-03-03T10:00:00+07:00",
        )
        plan = build_section_plan(
            con, "resume-aware", SectionSeed("main", "https://example.com/list")
        )

        assert plan.date_from == "2026-03-03"
        assert plan.date_to == "2026-04-07"
        assert plan.resume_from_published_at == "2026-03-03T10:00:00+07:00"

        result = RunOncePipeline(
            ResumeAwareAdapter(),
            client=client,
            section_plans=(plan,),
        ).run(con)

        assert result.section_stats[0].pages_scanned == 2
        assert result.counts.inserted_count == 1
        assert result.counts.dropped_out_of_window_count == 2

        rows = con.execute(
            "select url, published_at from articles order by published_at desc"
        ).fetchall()
        assert [(row["url"], row["published_at"]) for row in rows] == [
            ("https://example.com/article-new", "2026-04-07T08:00:00+07:00")
        ]
