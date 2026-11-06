"""Local stand-in for the Supabase REST API, serving fixture rows to the listing page.

Used by tests/test_listing_page_render.py. It answers only what pages/listing/[id].tsx
asks for (plus empty answers for anything else), with the anon-visible shape:
broker-direct rows only, and permission errors on the tables anon cannot read.
"""
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ACTIVE, INACTIVE, RELISTED = 9000001, 9000002, 9000003


def _listing(n, *, header, is_active, relisted, url, price=425000):
    return {
        "id": n - 9000000, "listing_number": n, "source": "broker_direct",
        "header": header, "price": price, "cash_flow": 120000,
        "state": "NY", "city": "Syracuse", "category": "business", "url": url,
        "first_seen": "2026-04-02T00:00:00+00:00", "last_seen": "2026-11-05T00:00:00+00:00",
        "estimated_listed_date": None, "days_on_market": None, "listing_views": None,
        "is_active": is_active, "price_reduced": False,
        "quality_tier": "Verified", "quality_score": 90,
        "relisted": relisted, "firm_key": None,
    }


LISTINGS = [
    _listing(ACTIVE, header="Fixture Active Laundromat", is_active=True, relisted=False,
             url="https://examplebroker.test/listing/active"),
    _listing(INACTIVE, header="Fixture Inactive Bakery", is_active=False, relisted=False,
             url="https://examplebroker.test/listing/inactive"),
    _listing(RELISTED, header="Fixture Relisted Car Wash", is_active=True, relisted=True,
             url="https://otherbroker.test/listing/relisted"),
]

MV_LISTINGS_PAGE = [
    {"listing_number": ACTIVE, "estimated_listed_date": "2026-04-02", "dom_days_eff": 218, "dom_basis": "observed"},
    {"listing_number": RELISTED, "estimated_listed_date": "2026-03-24", "dom_days_eff": 227, "dom_basis": "floor"},
]

BROKER_DIRECTORY = [
    {"slug": "examplebroker-test", "firm_name": "Example Broker", "domain": "examplebroker.test"},
]

# Reviewed price changes for the active fixture, newest first (as the page orders them).
PRICE_HISTORY = [
    {"listing_number": ACTIVE, "prev_price": 425000, "prev_last_seen": "2026-06-10",
     "next_price": 395000, "next_first_seen": "2026-06-14", "pct_change": -7.1, "window_days": 4},
    {"listing_number": ACTIVE, "prev_price": 450000, "prev_last_seen": "2025-12-30",
     "next_price": 425000, "next_first_seen": "2026-01-03", "pct_change": -5.6, "window_days": 4},
]

TABLES = {
    "listings": LISTINGS,
    "price_history": PRICE_HISTORY,
    "mv_listings_page": MV_LISTINGS_PAGE,
    "broker_directory": BROKER_DIRECTORY,
}
# Tables the anon key is refused on in production.
DENIED = {"listing_history", "bizquest_listings"}
# price_history errors for this listing number, as if the view were not published yet.
FAIL_FOR = {("price_history", RELISTED)}

requests_seen = []  # (table, query) for assertions


def _match(row, key, op_val):
    op, _, val = op_val.partition(".")
    v = row.get(key)
    if op == "eq":
        return str(v) == val
    if op == "lt":
        return v is not None and float(v) < float(val)
    if op == "not" and val == "is.null":
        return v is not None
    if op == "is" and val == "null":
        return v is None
    return True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _rows(self):
        parsed = urllib.parse.urlparse(self.path)
        table = parsed.path.rsplit("/", 1)[-1]
        q = urllib.parse.parse_qsl(parsed.query)
        requests_seen.append((table, parsed.query))
        if table in DENIED:
            return table, None
        if any(t == table and f"eq.{n}" in parsed.query for t, n in FAIL_FOR):
            return table, None
        rows = TABLES.get(table, [])
        for k, v in q:
            if k in ("select", "order", "limit", "offset"):
                continue
            rows = [r for r in rows if _match(r, k, v)]
        return table, rows

    def _send(self, body, n):
        accept = self.headers.get("Accept", "")
        status = 200
        if body is None:
            status, payload = 401, {"code": "42501", "message": "permission denied"}
        elif "vnd.pgrst.object" in accept:
            if len(body) != 1:
                status, payload = 406, {"code": "PGRST116", "message": "not one row"}
            else:
                payload = body[0]
        else:
            payload = body
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Range", f"0-{max(n - 1, 0)}/{n}" if n else "*/0")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        return data

    def do_GET(self):
        _, rows = self._rows()
        data = self._send(rows, len(rows or []))
        self.wfile.write(data)

    def do_HEAD(self):
        _, rows = self._rows()
        self._send(rows, len(rows or []))

    def do_POST(self):  # RPCs: answer null
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"null")


def start(port=0):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
