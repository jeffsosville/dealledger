"""Behavioral check: render /listing/{n} for an active, an inactive and a relisted listing
against a local stub of the Supabase REST API, and check what the page actually says.

Run: python3 tests/test_listing_page_render.py
Needs Node and `npm ci` done. No network, no real database.
"""
import json
import os
import signal
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
import listing_page_stub as stub  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))
METHODOLOGY = ("DealLedger displays the latest recorded asking price from the broker’s listing. "
               "Historical price changes are not currently tracked.")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(url):
    try:
        with NO_PROXY.open(url, timeout=120) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _json_ld(html):
    m = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S)
    assert m, "no JSON-LD on page"
    return json.loads(m.group(1))


def _graph(ld, kind):
    return next(n for n in ld["@graph"] if n["@type"] == kind)


def _text(html):
    body = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)
    body = re.sub(r"<[^>]+>", " ", body)
    for a, b in (("&rsquo;", "’"), ("&#x27;", "'"), ("&amp;", "&"), ("&quot;", '"')):
        body = body.replace(a, b)
    return re.sub(r"\s+", " ", body)


def check_common(html, n):
    text = _text(html)
    ld = _json_ld(html)
    for bad in ("Times Listed", "PRICE HISTORY", "Reduced ", "Closes (12mo)", "Lifetime Sold",
                "No price change observed"):
        assert bad not in text, f"{n}: page shows {bad!r}"
    assert "SoldOut" not in html, f"{n}: SoldOut in page"
    assert "dateModified" not in json.dumps(ld), f"{n}: dateModified in JSON-LD"
    assert METHODOLOGY in text, f"{n}: methodology copy missing"
    assert "Latest recorded" in text, f"{n}: price stat label missing"
    offer = _graph(ld, "Product")["offers"]
    assert offer["price"] == 425000, f"{n}: offer price {offer.get('price')}"
    assert "price changes" not in json.dumps(_graph(ld, "Dataset")), f"{n}: Dataset claims price changes"
    return text, offer


def main():
    srv = stub.start(0)
    stub_url = f"http://127.0.0.1:{srv.server_address[1]}"
    port = _free_port()
    env = dict(os.environ, NEXT_PUBLIC_SUPABASE_URL=stub_url,
               NEXT_PUBLIC_SUPABASE_ANON_KEY="test-anon-key", NEXT_TELEMETRY_DISABLED="1")
    proc = subprocess.Popen(["npx", "next", "dev", "-p", str(port)], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            start_new_session=True)
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                break
            except OSError:
                time.sleep(1)
        else:
            raise SystemExit("next dev did not start")

        # Active: in stock, broker-direct registry card shown.
        status, html = _get(f"{base}/listing/{stub.ACTIVE}")
        assert status == 200, f"active: HTTP {status}"
        text, offer = check_common(html, "active")
        assert offer.get("availability") == "https://schema.org/InStock", "active: availability"
        assert "Example Broker" in text, "active: registry broker card missing"
        assert "218 " in text or "218<" in html, "active: days shown"

        # Inactive: renders, asserts nothing about availability.
        status, html = _get(f"{base}/listing/{stub.INACTIVE}")
        assert status == 200, f"inactive: HTTP {status}"
        _, offer = check_common(html, "inactive")
        assert "availability" not in offer, f"inactive: availability {offer.get('availability')}"

        # Relisted: renders, floor shown as a minimum, no relist count claimed.
        status, html = _get(f"{base}/listing/{stub.RELISTED}")
        assert status == 200, f"relisted: HTTP {status}"
        text, offer = check_common(html, "relisted")
        assert offer.get("availability") == "https://schema.org/InStock", "relisted: availability"
        assert "227+" in text, "relisted: floor DOM should show as 227+"

        # Unknown number: not found.
        status, _ = _get(f"{base}/listing/9999999")
        assert status == 404, f"unknown: HTTP {status}"

        tables = {t for t, _ in stub.requests_seen}
        for t in ("listing_history", "broker_master", "bizquest_listings"):
            assert t not in tables, f"page queried {t}"
        assert "listings" in tables and "mv_listings_page" in tables
    except BaseException:
        os.killpg(proc.pid, signal.SIGTERM)
        print(proc.communicate(timeout=30)[0][-3000:])
        raise
    finally:
        srv.shutdown()
    os.killpg(proc.pid, signal.SIGTERM)
    proc.wait(timeout=30)
    print("listing page render: ok (active, inactive, relisted, 404)")


if __name__ == "__main__":
    main()
