from pathlib import Path

from app.sources.tuoitre import TuoiTreAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "tuoitre_list.html").read_text(encoding="utf-8")
    adapter = TuoiTreAdapter()

    urls = adapter.parse_list_page(html, base_url="https://tuoitre.vn/kinh-doanh.htm")

    assert urls
    assert urls[0].startswith("https://tuoitre.vn/")
    assert all(url.endswith(".htm") for url in urls)
    assert all(url.rsplit("-", 1)[-1][:-4].isdigit() for url in urls)
    assert len(urls) == len(set(urls))


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "tuoitre_article.html").read_text(encoding="utf-8")
    adapter = TuoiTreAdapter()
    url = (
        "https://tuoitre.vn/chien-su-day-gia-hoa-nhap-khau-dip-8-3-tang-manh-20260307075519174.htm"
    )

    article = adapter.parse_article(url, html)

    assert article.source == "tuoitre"
    assert article.url == url
    assert article.title == "Chiến sự đẩy giá hoa nhập khẩu dịp 8-3 tăng mạnh"
    assert article.published_at == "2026-03-07T08:01:53+07:00"
    assert article.category == "Kinh doanh"
    assert "Xung đột ở khu vực Trung Đông" in article.content_text
    assert article.content_html is not None


def test_discover_next_page_url_builds_timeline_url_from_zone_id():
    html = (FIXTURES_DIR / "tuoitre_list.html").read_text(encoding="utf-8")
    adapter = TuoiTreAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://tuoitre.vn/kinh-doanh.htm",
    )

    assert next_url == "https://tuoitre.vn/timeline/11/trang-1.htm"
