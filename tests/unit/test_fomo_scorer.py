import json

from app.fomo.scorer import score_fomo


def test_score_fomo_returns_polarized_positive_score():
    score, explain_json = score_fomo(
        "VIC tăng sốc lập đỉnh",
        "Cổ phiếu VIC bùng nổ, tăng sốc và lập đỉnh mới với thanh khoản kỷ lục.",
        ["VIC"],
    )

    explain = json.loads(explain_json)
    assert 0.8 <= score <= 1.0
    assert explain["final"] == score
    assert explain["signals"]
    assert explain["ticker_boost"] > 1


def test_score_fomo_returns_polarized_negative_score():
    score, explain_json = score_fomo(
        "Cổ phiếu lao dốc",
        "Nhà đầu tư bán tháo vì rủi ro phá sản và cổ phiếu giảm sàn.",
        [],
    )

    explain = json.loads(explain_json)
    assert -1.0 <= score <= -0.8
    assert explain["final"] == score
