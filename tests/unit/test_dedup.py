from app.dedup.hashers import (
    compute_content_sha256,
    compute_simhash64,
    compute_simhash_bucket,
    hamming_distance,
)


def test_dedup_hashers_match_for_identical_content():
    text = "VIC tăng mạnh trong phiên hôm nay"
    assert compute_content_sha256(text) == compute_content_sha256(text)

    simhash = compute_simhash64(text)
    assert simhash == compute_simhash64(text)
    assert compute_simhash_bucket(simhash) == compute_simhash_bucket(simhash)
    assert hamming_distance(simhash, simhash) == 0
