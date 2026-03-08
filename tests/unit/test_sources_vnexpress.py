from pathlib import Path

import pytest

from app.sources import SkipArticleError
from app.sources.vnexpress import VnExpressAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "vnexpress_list.html").read_text(encoding="utf-8")
    adapter = VnExpressAdapter()

    urls = adapter.parse_list_page(html, base_url="https://vnexpress.net/kinh-doanh")

    assert urls
    assert urls[0].startswith("https://vnexpress.net/")
    assert all("#" not in url for url in urls)


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "vnexpress_article.html").read_text(encoding="utf-8")
    adapter = VnExpressAdapter()
    url = "https://vnexpress.net/cach-cac-nuoc-doi-pho-cu-soc-nang-luong-tu-trung-dong-5047363.html"

    article = adapter.parse_article(url, html)

    assert article.source == "vnexpress"
    assert article.url == url
    assert article.title
    assert article.published_at == "2026-03-07T00:05:00+07:00"
    assert article.category == "Phân tích"
    assert "Trung Đông" in article.content_text
    assert article.content_html is not None


def test_parse_article_rejects_irrelevant_real_estate_story():
    html = (FIXTURES_DIR / "vnexpress_article.html").read_text(encoding="utf-8")
    html = (
        html.replace(
            '<meta content="Phân tích" itemprop="articleSection"/>',
            '<meta content="Nội thất" itemprop="articleSection"/>',
        )
        .replace(
            '<meta name="tt_list_folder_name" content="VnExpress,Kinh doanh,Quốc tế,Phân tích"/>',
            '<meta name="tt_list_folder_name" content="VnExpress,Bất động sản,Nội thất"/>',
        )
        .replace(
            '<meta name="its_subsection" content="kinh doanh, quốc tế, phân tích"/>',
            '<meta name="its_subsection" content="bất động sản, nội thất"/>',
        )
        .replace(
            '<li><a data-medium="Menu-KinhDoanh" href="/kinh-doanh" title="Kinh doanh">Kinh doanh</a></li><li><a data-medium="Menu-QuocTe" href="/kinh-doanh/quoc-te" title="Quốc tế">Quốc tế</a></li><li><a data-medium="Menu-PhanTich" href="/kinh-doanh/quoc-te/phan-tich" title="Phân tích">Phân tích</a></li>',
            '<li><a data-medium="Menu-BatDongSan" href="/bat-dong-san" title="Bất động sản">Bất động sản</a></li><li><a data-medium="Menu-NoiThat" href="/bat-dong-san/noi-that" title="Nội thất">Nội thất</a></li>',
        )
    )
    adapter = VnExpressAdapter()

    with pytest.raises(SkipArticleError):
        adapter.parse_article("https://vnexpress.net/bai-noi-that-5047363.html", html)


def test_parse_article_returns_none_when_article_has_no_publish_signals():
    html = (FIXTURES_DIR / "vnexpress_article.html").read_text(encoding="utf-8")
    html = html.replace(
        '<meta content="2026-03-07T00:05:00+07:00" itemprop="datePublished" name="pubdate"/>', ""
    )
    html = html.replace('"datePublished":"2026-03-07T00:05:00+07:00", ', "")
    html = html.replace('<span class="date">Thứ bảy, 7/3/2026, 00:05 (GMT+7)</span>', "")
    adapter = VnExpressAdapter()

    article = adapter.parse_article(
        "https://vnexpress.net/cach-cac-nuoc-doi-pho-cu-soc-nang-luong-tu-trung-dong-5047363.html",
        html,
    )

    assert article.published_at is None


def test_discover_next_page_url_uses_rel_next_link():
    html = (FIXTURES_DIR / "vnexpress_list.html").read_text(encoding="utf-8")
    adapter = VnExpressAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://vnexpress.net/kinh-doanh",
    )

    assert next_url == "https://vnexpress.net/kinh-doanh-p2"
