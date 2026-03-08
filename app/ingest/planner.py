import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.config import INGEST_DATE_FROM, INGEST_DATE_TO, RESUME_OVERLAP_HOURS
from app.db.crawl_state_repo import get_crawl_state_last_published_at
from app.sources import SectionSeed, SourceAdapter


@dataclass(frozen=True, slots=True)
class SectionPlan:
    source: str
    section: str
    section_url: str
    date_from: str
    date_to: str
    resume_from_published_at: str | None
    has_existing_coverage: bool


@dataclass(frozen=True, slots=True)
class SourcePlan:
    source: str
    sections: tuple[SectionPlan, ...]


def build_source_plan(con: sqlite3.Connection, adapter: SourceAdapter) -> SourcePlan:
    return SourcePlan(
        source=adapter.source_name,
        sections=tuple(
            build_section_plan(con, adapter.source_name, section) for section in adapter.sections
        ),
    )


def build_section_plan(
    con: sqlite3.Connection,
    source_name: str,
    section: SectionSeed,
    *,
    date_to: str | None = None,
) -> SectionPlan:
    resume_from_published_at = _resolve_resume_boundary(
        con, source_name=source_name, section_name=section.name
    )
    if resume_from_published_at is None:
        date_from = INGEST_DATE_FROM
        has_existing_coverage = False
    else:
        date_from = _date_with_overlap(resume_from_published_at)
        has_existing_coverage = True
    resolved_date_to = date_to or os.getenv("INGEST_DATE_TO", INGEST_DATE_TO)
    return SectionPlan(
        source=source_name,
        section=section.name,
        section_url=section.url,
        date_from=max(date_from, INGEST_DATE_FROM),
        date_to=resolved_date_to,
        resume_from_published_at=resume_from_published_at,
        has_existing_coverage=has_existing_coverage,
    )


def _resolve_resume_boundary(
    con: sqlite3.Connection,
    *,
    source_name: str,
    section_name: str,
) -> str | None:
    crawl_state_boundary = get_crawl_state_last_published_at(
        con,
        source=source_name,
        section=section_name,
    )
    if crawl_state_boundary:
        return crawl_state_boundary

    row = con.execute(
        """
        select max(published_at) as max_published_at
        from articles
        where source = ? and seed_section = ?
        """,
        (source_name, section_name),
    ).fetchone()
    if row is None:
        return None
    return row["max_published_at"]


def _date_with_overlap(published_at: str) -> str:
    boundary = datetime.fromisoformat(published_at) - timedelta(hours=RESUME_OVERLAP_HOURS)
    return boundary.date().isoformat()


def section_plan_log_line(plan: SectionPlan) -> str:
    coverage = "resume" if plan.has_existing_coverage else "initial"
    return (
        f"[{plan.source}:{plan.section}] "
        f"mode={coverage} "
        f"date_from={plan.date_from} "
        f"date_to={plan.date_to} "
        f"resume_from={plan.resume_from_published_at}"
    )
