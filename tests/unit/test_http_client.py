import httpx

from app.extract.http_client import build_client, fetch_html


def test_fetch_html_uses_client_and_returns_text():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text="<html>ok</html>"))
    with build_client(transport=transport) as client:
        assert fetch_html("https://example.com", client=client) == "<html>ok</html>"


def test_fetch_html_accepts_custom_rate_limit_seconds():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text="<html>ok</html>"))
    with build_client(transport=transport) as client:
        assert (
            fetch_html("https://example.com", client=client, rate_limit_seconds=0.0)
            == "<html>ok</html>"
        )
