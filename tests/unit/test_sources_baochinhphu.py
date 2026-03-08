from pathlib import Path

from app.sources.baochinhphu import BaoChinhPhuAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "baochinhphu_list.html").read_text(encoding="utf-8")
    adapter = BaoChinhPhuAdapter()

    urls = adapter.parse_list_page(
        html,
        base_url="https://baochinhphu.vn/chinh-sach-va-cuoc-song/chinh-sach-moi.htm",
    )

    assert urls
    assert urls[0].startswith("https://baochinhphu.vn/")
    assert all(url.endswith(".htm") for url in urls)
    assert len(urls) == len(set(urls))


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "baochinhphu_article.html").read_text(encoding="utf-8")
    adapter = BaoChinhPhuAdapter()
    url = "https://baochinhphu.vn/chinh-sach-moi-co-hieu-luc-tu-thang-3-2026-102260228093004191.htm"

    article = adapter.parse_article(url, html)

    assert article.source == "baochinhphu"
    assert article.url == url
    assert article.title == "Chính sách mới có hiệu lực từ tháng 3/2026"
    assert article.published_at == "2026-03-01T09:51:00+07:00"
    assert article.category == "Chính sách mới"
    assert "Nghị định số 357/2025/NĐ-CP" in article.content_text
    assert article.content_html is not None


def test_discover_next_page_url_builds_timelinelist_url_from_zone_id():
    html = (FIXTURES_DIR / "baochinhphu_list.html").read_text(encoding="utf-8")
    adapter = BaoChinhPhuAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://baochinhphu.vn/chinh-sach-va-cuoc-song/chinh-sach-moi.htm",
    )

    assert next_url == "https://baochinhphu.vn/timelinelist/1021119/2.htm"
