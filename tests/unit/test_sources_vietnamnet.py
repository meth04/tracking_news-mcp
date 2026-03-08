from pathlib import Path

from app.sources.vietnamnet import VietnamNetAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "vietnamnet_list.html").read_text(encoding="utf-8")
    adapter = VietnamNetAdapter()

    urls = adapter.parse_list_page(html, base_url="https://vietnamnet.vn/kinh-doanh")

    assert urls
    assert urls[0].startswith("https://vietnamnet.vn/")
    assert all(url.endswith(".html") for url in urls)
    assert len(urls) == len(set(urls))


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "vietnamnet_article.html").read_text(encoding="utf-8")
    adapter = VietnamNetAdapter()
    url = "https://vietnamnet.vn/co-phieu-flc-duoc-giao-dich-tro-lai-2495349.html"

    article = adapter.parse_article(url, html)

    assert article.source == "vietnamnet"
    assert article.url == url
    assert article.title == "Cổ phiếu FLC được giao dịch trở lại"
    assert article.published_at == "2026-03-07T08:52:11.000 +07:00"
    assert article.category == "Tài chính"
    assert "CTCP Tập đoàn FLC" in article.content_text
    assert article.content_html is not None


def test_discover_next_page_url_uses_rel_next_link():
    html = (FIXTURES_DIR / "vietnamnet_list.html").read_text(encoding="utf-8")
    adapter = VietnamNetAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://vietnamnet.vn/kinh-doanh",
    )

    assert next_url == "https://vietnamnet.vn/kinh-doanh-page1"
