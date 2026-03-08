from pathlib import Path

from app.sources.baodautu import BaoDauTuAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "baodautu_list.html").read_text(encoding="utf-8")
    adapter = BaoDauTuAdapter()

    urls = adapter.parse_list_page(html, base_url="https://baodautu.vn/tai-chinh-chung-khoan-d6/")

    assert urls
    assert urls[0].startswith("https://baodautu.vn/")
    assert all(url.endswith(".html") for url in urls)
    assert len(urls) == len(set(urls))


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "baodautu_article.html").read_text(encoding="utf-8")
    adapter = BaoDauTuAdapter()
    url = "https://baodautu.vn/xu-phat-giam-doc-tai-chinh-tap-doan-tien-son-thanh-hoa-vi-thao-tung-co-phieu-d538952.html"

    article = adapter.parse_article(url, html)

    assert article.source == "baodautu"
    assert article.url == url
    assert (
        article.title
        == "Xử phạt Giám đốc tài chính Tập đoàn Tiên Sơn Thanh Hoá vì thao túng cổ phiếu"
    )
    assert article.published_at == "07/03/2026 08:47"
    assert article.category == "Đầu tư tài chính"
    assert "Ông Tống Anh Linh" in article.content_text
    assert article.content_html is not None


def test_discover_next_page_url_finds_relative_pagination_link():
    html = (FIXTURES_DIR / "baodautu_list.html").read_text(encoding="utf-8")
    adapter = BaoDauTuAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://baodautu.vn/tai-chinh-chung-khoan-d6/",
    )

    assert next_url == "https://baodautu.vn/tai-chinh-chung-khoan-d6/p2"
