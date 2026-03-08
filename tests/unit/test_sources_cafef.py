from pathlib import Path

import pytest

from app.sources import SkipArticleError
from app.sources.cafef import CAFEF_SECTION_CONFIGS, CafeFAdapter, cafef_timelinelist_url

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_default_sections_cover_cafef_rollout():
    adapter = CafeFAdapter()

    assert [section.name for section in adapter.sections] == [
        "thi-truong-chung-khoan",
        "bat-dong-san",
        "doanh-nghiep",
        "tai-chinh-ngan-hang",
        "tai-chinh-quoc-te",
        "vi-mo-dau-tu",
        "thi-truong",
        "song",
    ]


def test_cafef_section_zone_mapping_is_explicit_and_complete():
    assert [(item.name, item.zone_id) for item in CAFEF_SECTION_CONFIGS] == [
        ("thi-truong-chung-khoan", "18831"),
        ("bat-dong-san", "18835"),
        ("doanh-nghiep", "18836"),
        ("tai-chinh-ngan-hang", "18834"),
        ("tai-chinh-quoc-te", "18832"),
        ("vi-mo-dau-tu", "18833"),
        ("thi-truong", "18839"),
        ("song", "188114"),
    ]


def test_parse_list_page_extracts_article_urls():
    html = (FIXTURES_DIR / "cafef_list.html").read_text(encoding="utf-8")
    adapter = CafeFAdapter()

    urls = adapter.parse_list_page(html, base_url="https://cafef.vn/thi-truong-chung-khoan.chn")

    assert urls
    assert urls[0].startswith("https://cafef.vn/")
    assert all(url.endswith(".chn") for url in urls)
    assert "https://cafef.vn/thi-truong-chung-khoan.chn" not in urls


def test_parse_article_extracts_normalized_fields():
    html = (FIXTURES_DIR / "cafef_article.html").read_text(encoding="utf-8")
    adapter = CafeFAdapter()
    url = "https://cafef.vn/thoi-diem-nang-hang-can-ke-mbs-diem-ten-30-co-phieu-viet-nam-co-kha-nang-lot-ro-chi-so-ftse-18826030622484142.chn"

    article = adapter.parse_article(url, html)

    assert article.source == "cafef"
    assert article.url == url
    assert article.title.startswith("Thời điểm nâng hạng cận kề")
    assert article.category == "Thị trường chứng khoán"
    assert article.published_at == "2026-03-07T00:01:00"
    assert "FTSE" in article.content_text
    assert article.content_html is not None


def test_parse_article_keeps_relevant_bat_dong_san_story():
    html = (FIXTURES_DIR / "cafef_article_batdongsan.html").read_text(encoding="utf-8")
    adapter = CafeFAdapter()
    url = "https://cafef.vn/tang-ho-tro-giam-ap-luc-tai-chinh-khi-chuyen-muc-dich-su-dung-dat-188260307071033787.chn"

    article = adapter.parse_article(url, html)

    assert article.category == "Bất động sản"
    assert article.published_at == "2026-03-07T09:40:00"
    assert "tiền sử dụng đất" in article.content_text.lower()


def test_parse_article_rejects_irrelevant_bat_dong_san_story():
    adapter = CafeFAdapter()
    html = (FIXTURES_DIR / "cafef_article_batdongsan.html").read_text(encoding="utf-8")
    html = html.replace(
        "Tăng hỗ trợ, giảm áp lực tài chính khi chuyển mục đích sử dụng đất",
        "Mẫu nhà đẹp với không gian sống và nội thất tinh tế",
    ).replace(
        "Bộ Tài chính cho biết các quy định mới về tiền sử dụng đất đã được điều chỉnh theo hướng tháo gỡ vướng mắc, giảm nghĩa vụ tài chính trong một số trường hợp chuyển mục đích sang đất ở, đồng thời duy trì chính sách miễn, giảm và ghi nợ đối với nhóm đối tượng yếu thế theo quy định của Luật Đất đai năm 2024.",
        "Bài viết chia sẻ kinh nghiệm trang trí phòng khách, thiết kế phòng ngủ và nội thất cho không gian sống hiện đại.",
    )

    with pytest.raises(SkipArticleError):
        adapter.parse_article("https://cafef.vn/bai-viet-demo-188260307071033787.chn", html)


def test_discover_next_page_url_builds_timelinelist_url_from_explicit_mapping():
    html = (FIXTURES_DIR / "cafef_list.html").read_text(encoding="utf-8")
    adapter = CafeFAdapter()

    next_url = adapter.discover_next_page_url(
        html,
        section=adapter.sections[0],
        current_url="https://cafef.vn/thi-truong-chung-khoan.chn",
    )

    assert next_url == "https://cafef.vn/timelinelist/18831/2.chn"


def test_discover_next_page_url_falls_back_to_zone_id_in_markup():
    adapter = CafeFAdapter()
    html = "<div data-cd-key='siteid188:newsinzone:zone18831'></div>"

    next_url = adapter.discover_next_page_url(
        html,
        section=type("Section", (), {"name": "unknown", "url": "https://cafef.vn/unknown.chn"})(),
        current_url="https://cafef.vn/unknown.chn",
    )

    assert next_url == "https://cafef.vn/timelinelist/18831/2.chn"


def test_timelinelist_url_generation_uses_mapped_zone_id():
    adapter = CafeFAdapter()

    assert (
        adapter.timelinelist_url(section=adapter.sections[3], page_number=17)
        == "https://cafef.vn/timelinelist/18834/17.chn"
    )


def test_cafef_timelinelist_url_helper_uses_zone_id():
    assert cafef_timelinelist_url("18831", 9) == "https://cafef.vn/timelinelist/18831/9.chn"


def test_parse_article_respects_runtime_html_storage_flags(monkeypatch):
    html = (FIXTURES_DIR / "cafef_article.html").read_text(encoding="utf-8")
    adapter = CafeFAdapter()
    url = "https://cafef.vn/runtime-flags-18826030622484142.chn"

    monkeypatch.setenv("STORE_CONTENT_HTML", "0")
    monkeypatch.setenv("STORE_RAW_HTML", "0")
    article = adapter.parse_article(url, html)

    assert article.content_html is None
    assert article.raw_html is None


def test_parse_list_page_extracts_urls_from_timelinelist_markup_without_container():
    adapter = CafeFAdapter()
    html = """
    <div class='tlitem'>
      <h3><a href='/bai-viet-a-188260306154928257.chn'>A</a></h3>
      <div class='tlitem-flex'>
        <a class='avatar' href='/bai-viet-a-188260306154928257.chn'>avatar</a>
      </div>
    </div>
    <div class='tlitem'>
      <h3><a href='/bai-viet-b-188260306154928258.chn'>B</a></h3>
    </div>
    """

    urls = adapter.parse_list_page(html, base_url="https://cafef.vn/timelinelist/18831/2.chn")

    assert urls == [
        "https://cafef.vn/bai-viet-a-188260306154928257.chn",
        "https://cafef.vn/bai-viet-b-188260306154928258.chn",
    ]
