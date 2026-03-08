import sqlite3

from app.ingest.crawl_cafef_timelinelist_raw import _extract_rows, _insert_rows, _parse_zone_targets


def test_parse_zone_targets_supports_ranges_and_single_pages():
    targets = _parse_zone_targets("18839:8-10,18835:2")

    assert [(item.zone_id, item.page_from, item.page_to) for item in targets] == [
        ("18839", 8, 10),
        ("18835", 2, 2),
    ]


def test_extract_rows_reads_timelinelist_items():
    html = """
    <div class='tlitem box-category-item' data-id='188260306154928257'>
      <h3><a href='/bai-viet-a-188260306154928257.chn'>Bài A</a></h3>
      <div class='tlitem-flex'>
        <img src='https://img/a.jpg' />
        <span class='time time-ago' title='2026-03-06T15:50:00'>2026-03-06T15:50:00</span>
        <p class='sapo box-category-sapo'>Tóm tắt A</p>
      </div>
    </div>
    <div class='tlitem box-category-item' data-id='188260306154928258'>
      <h3><a href='/bai-viet-b-188260306154928258.chn'>Bài B</a></h3>
      <div class='tlitem-flex'>
        <p class='time' data-time='2026-03-06T16:00:00'>2026-03-06T16:00:00</p>
      </div>
    </div>
    """

    rows = _extract_rows(
        html,
        page_url="https://cafef.vn/timelinelist/18839/8.chn",
        zone_id="18839",
        page_number=8,
    )

    assert len(rows) == 2
    assert rows[0][0] == "18839"
    assert rows[0][1] == 8
    assert rows[0][5] == "https://cafef.vn/bai-viet-a-188260306154928257.chn"
    assert rows[0][6] == "Bài A"
    assert rows[0][7] == "2026-03-06T15:50:00"
    assert rows[0][8] == "Tóm tắt A"


def test_insert_rows_is_idempotent():
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        create table cafef_timelinelist_raw (
          id integer primary key autoincrement,
          zone_id text not null,
          page_number integer not null,
          page_url text not null,
          item_rank integer not null,
          article_id text,
          article_url text not null,
          title text,
          published_at_raw text,
          summary_text text,
          image_url text,
          raw_item_html text,
          collected_at text not null default (datetime('now')),
          unique(zone_id, page_number, item_rank, article_url)
        )
        """
    )
    rows = [
        (
            "18839",
            8,
            "https://cafef.vn/timelinelist/18839/8.chn",
            1,
            "188260306154928257",
            "https://cafef.vn/bai-viet-a-188260306154928257.chn",
            "Bài A",
            "2026-03-06T15:50:00",
            "Tóm tắt A",
            "https://img/a.jpg",
            "<div></div>",
        )
    ]

    assert _insert_rows(con, rows) == 1
    assert _insert_rows(con, rows) == 0
