"""PageFetcher._fetch_http: one-shot retry after an HTTP/2 stream reset (curl 92).

laundromat-broker.com resets every request whose UA says Chrome < 137, so the
retry swaps the impersonation target (RESET_FALLBACK_IMPERSONATE), not the
HTTP version. These tests mock the session; no network.
"""
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402


class FakeCurlError(Exception):
    def __init__(self, msg, code):
        super().__init__(msg)
        self.code = code


H2_RESET = ("Failed to perform, curl: (92) HTTP/2 stream 1 reset by server "
            "(error 0x1 PROTOCOL_ERROR). See https://curl.se/libcurl/c/libcurl-errors.html "
            "first for more details.")
TIMEOUT = "Failed to perform, curl: (28) Operation timed out after 25000 milliseconds"


class Resp:
    def __init__(self, status=200, text="<html>ok</html>"):
        self.status_code = status
        self.text = text


@pytest.fixture
def fetcher(monkeypatch):
    monkeypatch.setattr(v6, "HAS_CURL_CFFI", True)
    monkeypatch.setattr(v6.time, "sleep", lambda *_: None)
    f = v6.PageFetcher()
    f.session = mock.Mock()
    # _rewarm replaces self.session; keep the mock in place instead.
    monkeypatch.setattr(f, "_rewarm", lambda *a, **k: None)
    return f


def _imps(session):
    return [c.kwargs.get("impersonate") for c in session.get.call_args_list]


def test_reset_then_success_retries_once_with_fallback(fetcher):
    fetcher.session.get.side_effect = [FakeCurlError(H2_RESET, 92), Resp(200, "BODY")]
    assert fetcher._fetch_http("https://laundromat-broker.com/listings.html") == "BODY"
    assert fetcher.session.get.call_count == 2
    assert _imps(fetcher.session) == [None, fetcher.RESET_FALLBACK_IMPERSONATE]
    assert fetcher.stats == {"h2_reset_fallback": 1, "h2_reset_fallback_ok": 1}


def test_fallback_keeps_proxy_setting(fetcher, monkeypatch):
    monkeypatch.setattr(fetcher, "_proxy_dict", lambda: {"https": "http://p"})
    fetcher.session.get.side_effect = [FakeCurlError(H2_RESET, 92), Resp(200, "BODY")]
    fetcher._fetch_http("https://laundromat-broker.com/listings.html", use_proxy=True)
    proxies = [c.kwargs.get("proxies") for c in fetcher.session.get.call_args_list]
    assert proxies == [{"https": "http://p"}, {"https": "http://p"}]

    fetcher.session.get.reset_mock()
    fetcher._reset_fallback_domains.clear()
    fetcher.session.get.side_effect = [FakeCurlError(H2_RESET, 92), Resp(200, "BODY")]
    fetcher._fetch_http("https://other.example/listings", use_proxy=False)
    proxies = [c.kwargs.get("proxies") for c in fetcher.session.get.call_args_list]
    assert proxies == [None, None]


def test_matches_on_message_without_code(fetcher):
    fetcher.session.get.side_effect = [Exception(H2_RESET), Resp(200, "BODY")]
    assert fetcher._fetch_http("https://x.example/") == "BODY"
    assert _imps(fetcher.session)[1] == fetcher.RESET_FALLBACK_IMPERSONATE


def test_later_pages_of_domain_use_fallback_directly(fetcher):
    fetcher.session.get.side_effect = [FakeCurlError(H2_RESET, 92), Resp(200, "P1"), Resp(200, "P2")]
    fetcher._fetch_http("https://laundromat-broker.com/listings.html")
    assert fetcher._fetch_http("https://laundromat-broker.com/listings.html?p=2") == "P2"
    assert _imps(fetcher.session)[-1] == fetcher.RESET_FALLBACK_IMPERSONATE
    assert fetcher.stats["h2_reset_fallback"] == 1


def test_other_error_is_not_fallback(fetcher):
    fetcher.session.get.side_effect = [FakeCurlError(TIMEOUT, 28), Resp(200, "BODY")]
    assert fetcher._fetch_http("https://x.example/") == "BODY"
    # The existing re-warm retry runs, but never with the fallback fingerprint.
    assert _imps(fetcher.session) == [None, None]
    assert fetcher.stats["h2_reset_fallback"] == 0


def test_other_error_exhausted_propagates_unchanged(fetcher):
    fetcher.session.get.side_effect = FakeCurlError(TIMEOUT, 28)
    with pytest.raises(FakeCurlError) as ei:
        fetcher._fetch_http("https://x.example/")
    assert ei.value.code == 28
    assert fetcher.session.get.call_count == v6.MAX_403_RETRIES + 1
    assert all(i is None for i in _imps(fetcher.session))


def test_reset_twice_propagates_original_failure(fetcher):
    fetcher.session.get.side_effect = FakeCurlError(H2_RESET, 92)
    with pytest.raises(FakeCurlError) as ei:
        fetcher._fetch_http("https://laundromat-broker.com/listings.html")
    assert ei.value.code == 92                      # original shape, not the fallback's
    imps = _imps(fetcher.session)
    assert imps.count(fetcher.RESET_FALLBACK_IMPERSONATE) == 1   # retried ONCE only
    assert len(imps) == v6.MAX_403_RETRIES + 2      # normal retries + the one fallback
    assert fetcher.stats == {"h2_reset_fallback": 1, "h2_reset_fallback_ok": 0}
    assert "laundromat-broker.com" not in fetcher._reset_fallback_domains


def test_reset_then_fallback_different_error_still_raises_reset(fetcher):
    fetcher.session.get.side_effect = (
        [FakeCurlError(H2_RESET, 92), FakeCurlError("curl: (56) Connection closed abruptly", 56)]
        + [FakeCurlError(H2_RESET, 92)] * v6.MAX_403_RETRIES)
    with pytest.raises(FakeCurlError) as ei:
        fetcher._fetch_http("https://laundromat-broker.com/listings.html")
    assert ei.value.code == 92


def test_success_path_untouched(fetcher):
    fetcher.session.get.side_effect = [Resp(200, "BODY")]
    assert fetcher._fetch_http("https://robbinspellegrino.com/marketplace") == "BODY"
    assert fetcher.session.get.call_count == 1
    assert "impersonate" not in fetcher.session.get.call_args.kwargs
    assert fetcher.stats["h2_reset_fallback"] == 0
