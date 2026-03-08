import json
import os
from datetime import date

import pandas as pd
import streamlit as st

from app.db.conn import connect
from app.db.query_service import (
    MAX_LIST_LIMIT,
    ArticleFilters,
    get_article_by_id,
    latest_ingest_run,
    overview_stats,
    search_articles,
    timeline_stats,
    top_tickers,
)

DB_PATH = os.getenv("NEWS_DB_PATH", "./data/news.db")
DEFAULT_DATE_FROM = date.fromisoformat(os.getenv("INGEST_DATE_FROM", "2025-01-01"))
DEFAULT_DATE_TO = date.fromisoformat(os.getenv("INGEST_DATE_TO", date.today().isoformat()))
TABLE_LIMIT = min(MAX_LIST_LIMIT, 200)

st.set_page_config(page_title="VN News Dashboard", layout="wide")
st.title("VN News Dashboard")


@st.cache_data(ttl=30)
def load_filter_options(date_from: str, date_to: str) -> dict[str, list[str]]:
    with connect(DB_PATH) as con:
        sources = [
            row["source"]
            for row in con.execute(
                """
                select distinct source
                from articles
                where published_date between ? and ?
                order by source
                """,
                (date_from, date_to),
            ).fetchall()
        ]
        sections = [
            row["seed_section"]
            for row in con.execute(
                """
                select distinct seed_section
                from articles
                where published_date between ? and ?
                  and seed_section is not null and trim(seed_section) <> ''
                order by seed_section
                """,
                (date_from, date_to),
            ).fetchall()
        ]
        categories = [
            row["category"]
            for row in con.execute(
                """
                select distinct category
                from articles
                where published_date between ? and ?
                  and category is not null and trim(category) <> ''
                order by category
                """,
                (date_from, date_to),
            ).fetchall()
        ]
    return {
        "sources": sources,
        "sections": sections,
        "categories": categories,
    }


@st.cache_data(ttl=30)
def load_overview(filters: ArticleFilters) -> dict:
    with connect(DB_PATH) as con:
        return overview_stats(con, filters=filters)


@st.cache_data(ttl=30)
def load_timeline(filters: ArticleFilters) -> list[dict]:
    with connect(DB_PATH) as con:
        return timeline_stats(con, filters=filters)


@st.cache_data(ttl=30)
def load_articles(filters: ArticleFilters, limit: int) -> list[dict]:
    with connect(DB_PATH) as con:
        return search_articles(con, filters=filters, limit=limit)


@st.cache_data(ttl=30)
def load_top_tickers(filters: ArticleFilters, limit: int) -> list[dict]:
    with connect(DB_PATH) as con:
        return top_tickers(con, filters=filters, limit=limit)


@st.cache_data(ttl=30)
def load_latest_run() -> dict | None:
    with connect(DB_PATH) as con:
        return latest_ingest_run(con)


@st.cache_data(ttl=30)
def load_article_detail(article_id: int) -> dict | None:
    with connect(DB_PATH) as con:
        return get_article_by_id(con, article_id)


st.sidebar.header("Filters")
date_from = st.sidebar.date_input("From", value=DEFAULT_DATE_FROM)
date_to = st.sidebar.date_input("To", value=DEFAULT_DATE_TO)
filter_options = load_filter_options(str(date_from), str(date_to))
selected_sources = st.sidebar.multiselect(
    "Sources", filter_options["sources"], default=filter_options["sources"]
)
selected_sections = st.sidebar.multiselect("Sections", filter_options["sections"], default=[])
selected_categories = st.sidebar.multiselect("Categories", filter_options["categories"], default=[])
min_fomo = st.sidebar.slider("Min fomo", min_value=-1.0, max_value=1.0, value=-1.0, step=0.1)
keyword = st.sidebar.text_input("Keyword (FTS)", value="")
selected_ticker = st.sidebar.text_input("Ticker", value="").strip().upper()

filters = ArticleFilters(
    date_from=str(date_from),
    date_to=str(date_to),
    sources=tuple(selected_sources),
    categories=tuple(selected_categories),
    sections=tuple(selected_sections),
    tickers=(selected_ticker,) if selected_ticker else (),
    min_fomo=min_fomo,
    keyword=keyword or None,
)

overview = load_overview(filters)
latest_run = load_latest_run()
timeline_rows = load_timeline(filters)
article_rows = load_articles(filters, TABLE_LIMIT)
ticker_rows = load_top_tickers(filters, 20)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Articles", overview["total_articles"])
c2.metric("Sources", overview["sources_count"])
c3.metric("Sections", overview["sections_count"])
c4.metric("Latest published_at", overview["latest_published_at"] or "-")

if latest_run:
    st.caption(
        "Last ingest run: "
        f"mode={latest_run['mode']} inserted={latest_run['inserted_count']} "
        f"dropped_no_date={latest_run['dropped_no_date_count']} "
        f"dedup_dropped={latest_run['dedup_dropped_count']}"
    )

st.subheader("Timeline")
if timeline_rows:
    timeline_df = pd.DataFrame(timeline_rows)
    st.line_chart(
        timeline_df.set_index("published_date")[["article_count", "avg_fomo"]],
        width="stretch",
    )
else:
    st.info("No articles for the current filters.")

left, right = st.columns([1, 2])

left.subheader("Top tickers")
if ticker_rows:
    left.dataframe(pd.DataFrame(ticker_rows), width="stretch")
else:
    left.info("No ticker matches.")

right.subheader("Latest feed")
if article_rows:
    articles_df = pd.DataFrame(article_rows)
    right.dataframe(
        articles_df[
            [
                "id",
                "published_at",
                "source",
                "seed_section",
                "category",
                "title",
                "fomo_score",
                "tickers",
                "url",
            ]
        ],
        column_config={
            "url": st.column_config.LinkColumn("URL"),
            "tickers": st.column_config.ListColumn("Tickers"),
        },
        width="stretch",
    )
else:
    right.info("No articles available.")

st.subheader("Article detail")
selected_article_id = (
    st.selectbox(
        "Select article",
        options=[row["id"] for row in article_rows],
        format_func=lambda article_id: next(
            row["title"] for row in article_rows if row["id"] == article_id
        ),
    )
    if article_rows
    else None
)

if selected_article_id is not None:
    detail = load_article_detail(int(selected_article_id))
    if detail is not None:
        st.markdown(f"### {detail['title']}")
        meta1, meta2, meta3 = st.columns(3)
        meta1.write(f"Source: {detail['source']}")
        meta1.write(f"Section: {detail['seed_section'] or '-'}")
        meta2.write(f"Category: {detail['category'] or '-'}")
        meta2.write(f"Topic: {detail['topic_label'] or '-'}")
        meta3.write(f"Published: {detail['published_at']}")
        meta3.write(f"FOMO: {detail['fomo_score']}")
        st.write(f"Tickers: {', '.join(detail['tickers']) if detail['tickers'] else '-'}")
        st.write(f"URL: {detail['url']}")
        st.code(json.dumps(detail["fomo_explain"], ensure_ascii=False, indent=2), language="json")
        st.text_area("Content", value=detail["content_text"], height=320)
        if detail["content_html"]:
            with st.expander("content_html"):
                st.code(detail["content_html"])
        if detail["raw_html"]:
            with st.expander("raw_html"):
                st.code(detail["raw_html"])
