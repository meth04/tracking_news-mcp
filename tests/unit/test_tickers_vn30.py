from app.tickers.vn30 import extract_vn30_tickers


def test_extract_vn30_tickers_is_deduplicated_and_ordered():
    text = "Cổ phiếu VIC dẫn dắt, HPG bứt phá. VIC vẫn tăng, VCB hưởng lợi."
    assert extract_vn30_tickers(text) == ["VIC", "HPG", "VCB"]
