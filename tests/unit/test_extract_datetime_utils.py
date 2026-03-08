import pytest

from app.extract.datetime_utils import (
    MissingPublishedAtError,
    normalize_published_at,
    published_date_from_iso,
)
from app.extract.normalize import normalize_text


def test_normalize_published_at_keeps_vn_timezone():
    value = normalize_published_at("2026-03-07T00:05:00+07:00")
    assert value == "2026-03-07T00:05:00+07:00"
    assert published_date_from_iso(value) == "2026-03-07"


def test_normalize_published_at_rejects_missing_value():
    with pytest.raises(MissingPublishedAtError):
        normalize_published_at(None)


def test_normalize_text_collapses_whitespace():
    text = "  VIC  tăng mạnh\n\n\tHPG bứt phá  "
    assert normalize_text(text) == "VIC tăng mạnh\nHPG bứt phá"
