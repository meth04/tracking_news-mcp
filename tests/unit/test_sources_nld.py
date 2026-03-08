from pathlib import Path

from app.sources.nld import NguoiLaoDongAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "nld_list.html").read_text(encoding="utf-8")
    adapter = NguoiLaoDongAdapter()

    urls = adapter.parse_list_page(html, base_url="https://nld.com.vn/kinh-te.htm")

    assert urls
    assert urls[0].startswith("https://nld.com.vn/")
    assert all(url.endswith(".htm") for url in urls)
    assert len(urls) == len(set(urls))


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "nld_article.html").read_text(encoding="utf-8")
    adapter = NguoiLaoDongAdapter()
    url = "https://nld.com.vn/nhung-sai-sot-ho-kinh-doanh-thuong-gap-khi-thuc-hien-quy-dinh-moi-ve-thue-196260307071308923.htm"

    article = adapter.parse_article(url, html)

    assert article.source == "nld"
    assert article.url == url
    assert (
        article.title == "Những sai sót hộ kinh doanh thường gặp khi thực hiện quy định mới về thuế"
    )
    assert article.published_at == "2026-03-07T08:06:00+07:00"
    assert article.category == "Kinh tế"
    assert "Chính phủ vừa ban hành Nghị định 68/2026/NĐ-CP" in article.content_text
    assert article.content_html is not None


def test_discover_next_page_url_builds_timelinelist_url_from_zone_id():
    html = (FIXTURES_DIR / "nld_list.html").read_text(encoding="utf-8")
    adapter = NguoiLaoDongAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://nld.com.vn/kinh-te.htm",
    )

    assert next_url == "https://nld.com.vn/timelinelist/1961014/2.htm"
