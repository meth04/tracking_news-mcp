from pathlib import Path

from app.sources.dantri import DanTriAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "dantri_list.html").read_text(encoding="utf-8")
    adapter = DanTriAdapter()

    urls = adapter.parse_list_page(html, base_url="https://dantri.com.vn/kinh-doanh.htm")

    assert urls
    assert urls[0].startswith("https://dantri.com.vn/kinh-doanh/")
    assert all(url.endswith(".htm") for url in urls)
    assert len(urls) == len(set(urls))


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "dantri_article.html").read_text(encoding="utf-8")
    adapter = DanTriAdapter()
    url = "https://dantri.com.vn/kinh-doanh/gia-vang-giam-van-cao-hon-the-gioi-gan-20-trieu-dongluong-20260307080054340.htm"

    article = adapter.parse_article(url, html)

    assert article.source == "dantri"
    assert article.url == url
    assert article.title == "Giá vàng giảm, vẫn cao hơn thế giới gần 20 triệu đồng/lượng"
    assert article.published_at == "2026-03-07 08:07"
    assert article.category == "Tài chính"
    assert "Giá vàng trong nước tiếp tục giảm" in article.content_text
    assert article.content_html is not None


def test_discover_next_page_url_finds_trang_pattern():
    html = (FIXTURES_DIR / "dantri_list.html").read_text(encoding="utf-8")
    adapter = DanTriAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://dantri.com.vn/kinh-doanh.htm",
    )

    assert next_url == "https://dantri.com.vn/kinh-doanh/trang-2.htm"
