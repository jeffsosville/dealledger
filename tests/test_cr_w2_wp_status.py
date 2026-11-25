"""Unit tests for scrapers/cr_w2_wp_status_identity.py (W2, 2026-11-23) and the
specialized-runner / WP-REST pieces of the same job. Each fix has a must-still-pass
case (principle 8)."""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("SCRAPERS", os.path.join(HERE, "..", "scrapers")))

import cr_w2_wp_status_identity as m  # noqa: E402
import run_specialized as rs  # noqa: E402
from specialized_scrapers import wp_json_loads  # noqa: E402


# ── hscbrokers: status in the URL path ──────────────────────────────────────
def test_hsc_path_status():
    u = "https://www.hscbrokers.com/property-listings/%s/some-bar/"
    assert m.url_path_status(u % "active", "hscbrokers.com") == ("active", None)
    assert m.url_path_status(u % "sold", "www.hscbrokers.com") == ("sold", "Sold")
    assert m.url_path_status(u % "pending", "hscbrokers.com") == ("active", "Pending")
    assert m.url_path_status(u % "leased", "hscbrokers.com")[0] == m.DROP


def test_hsc_path_status_must_not_fire_elsewhere():
    # same shape on another broker: untouched. Index and bare status pages carry no status.
    assert m.url_path_status("https://other.com/property-listings/sold/x/", "other.com") is None
    assert m.url_path_status("https://www.hscbrokers.com/property-listings/", "hscbrokers.com") is None
    assert m.url_path_status("https://www.hscbrokers.com/property-listings/sold/", "hscbrokers.com") is None
    assert m.url_path_status("https://www.hscbrokers.com/property-listings/unknown/x/", "hscbrokers.com") is None


def test_hsc_index_card():
    assert m.is_index_card("https://www.hscbrokers.com/property-listings/", "hscbrokers.com")
    assert not m.is_index_card("https://www.hscbrokers.com/property-listings/active/x/", "hscbrokers.com")
    assert not m.is_index_card("https://other.com/", "other.com")


# ── prices: one field, never a guess ────────────────────────────────────────
def test_field_money():
    for txt, want in [("$120,000", 120000), (" 2,099,000", 2099000), ("$600.000", 600000),
                      ("$280000", 280000), ("$1,875,000*", 1875000), ("$175,000 plus inventory", 175000)]:
        assert m.parse_field_money(txt) == want, txt
    for txt in ["A CONVENIR", "Undisclosed", "Contact Agent", "", None, "$100,000 - $200,000", "$900"]:
        assert m.parse_field_money(txt) is None, txt


# ── negociosenflorida ───────────────────────────────────────────────────────
def _card(slug, title, price, status=""):
    return (f'<div class="listing-wrap"><h3 class="listing-title"><a href="https://negociosenflorida.com/listings/{slug}/">'
            f'{title}</a></h3><span class="listing-price">{price}</span>'
            f'<span class="listing-status">{status}</span><span class="listing-property-type">Restaurante</span></div>')


NEG = ('<html><body><div class="wp-listings-shortcode">'
       + _card("west-flagler-670k-ventas", "Restaurante West Flagler - $670K Ventas - Negocios En Florida", "$189,000")
       + _card("coral-gables", "Restaurante Coral Gables", " 2,099,000")
       + _card("la-visa-americana", "La Visa Americana", "")
       + _card("edificio-comercial-en-venta-en-miami-springs", "Edificio Comercial en Venta en Miami Springs", "$900,000")
       + _card("padel", "Club de Padel", "")
       + _card("vendido-uno", "Pizzeria", "$100,000", "Vendido")
       + '</div><div class="sidx-container"><div class="listing-wrap"><h3 class="listing-title">'
         '<a href="https://negociosenflorida.com/idx/123-main-st/">123 Main St New Listing - 2 hours on site</a></h3>'
         '<span class="listing-price">$999,999</span></div></div></body></html>')


def test_negocios_cards_price_from_field_and_no_idx():
    cards, held = m.parse_negocios_cards(NEG)
    by = {c["title"]: c for c in cards}
    assert held == 2                                           # visa + building held, not dropped silently
    assert "Restaurante West Flagler - $670K Ventas" in by     # suffix stripped
    assert by["Restaurante West Flagler - $670K Ventas"]["price"] == 189000   # not the title's $670K
    assert by["Restaurante Coral Gables"]["price"] == 2099000  # no $ sign
    assert by["Club de Padel"]["price"] is None                # no price shown: none, no guess
    assert by["Pizzeria"]["status"] == "sold" and by["Pizzeria"]["status_label"] == "Vendido"
    assert not any("123 Main" in t for t in by)                # IDX/MLS block never read
    assert len(cards) == 4


def test_negocios_handler_ignores_other_domains_and_pages_without_cards():
    h = m.make_negocios_handler(lambda *a, **k: {})
    assert h("https://example.com/x", NEG, "b", None) is None
    assert h("https://negociosenflorida.com/x", "<html></html>", "b", None) is None


# ── arthurberry ─────────────────────────────────────────────────────────────
def _row(i, title, href, price, subtype, ref="", img=""):
    return (f'<tr data-row_id="{i}"><td>{img}Jan 2026</td><td><a href="{href}">{title}</a></td>'
            f'<td>{price}</td><td></td><td>{subtype}<br>{ref}</td><td>Boise, ID</td></tr>')


AB = ('<table>'
      + _row(1, "DOWNTOWN RESTAURANT W/LIQUOR LICENSE", "/wp-content/uploads/a.pdf", "$300,000", "Restaurant", "REF BB1234")
      + _row(2, "MERIDIAN LEGACY LIQUOR LICENSE", "/wp-content/uploads/b.pdf", "$150,000", "Liquor License")
      + _row(3, "EAGLE MIXED-USE PROJECT 27.3 AC", "/wp-content/uploads/c.pdf", "$5,000,000", "Land")
      + _row(4, "OFFICE BUILDING", "/commercial-listings/office/", "$1,000,000", "Office")
      + _row(5, "SOLD GARDEN CENTER", "/wp-content/uploads/d.pdf", "$2,500,000", "Retail",
             img='<img src="/img/sold.png" alt="Sold">')
      + _row(6, "NDA ONE", "/non-disclosure-agreement/", "$10,000", "Retail")
      + _row(7, "NDA TWO", "/non-disclosure-agreement/", "$20,000", "Retail")
      + '</table>')


def test_arthurberry_rows():
    rows, dropped = m.parse_arthurberry_rows(AB)
    titles = {r["title"]: r for r in rows}
    assert dropped == {"liquor-license-only": 1, "land": 1, "commercial-real-estate": 1}
    assert "DOWNTOWN RESTAURANT W/LIQUOR LICENSE" in titles    # restaurant sold WITH a license stays in
    assert titles["DOWNTOWN RESTAURANT W/LIQUOR LICENSE"]["ref"] == "BB1234"
    assert titles["SOLD GARDEN CENTER"]["status"] == "sold"
    assert titles["NDA ONE"]["detail_url"] is None and titles["NDA TWO"]["detail_url"] is None   # shared NDA page is not an identity
    assert titles["DOWNTOWN RESTAURANT W/LIQUOR LICENSE"]["price"] == 300000


# ── synergybb: stated total includes sold ───────────────────────────────────
def test_compared_count():
    ls = [{"status": "active"}] * 3 + [{"status": "sold"}] * 4 + [{"status": "inactive"}]
    assert rs._compared_count("wp_synergybb_com", ls)[0] == 7
    assert "stated total includes sold" in rs._compared_count("wp_synergybb_com", ls)[1]
    assert rs._compared_count("wp_other_com", ls) == (3, "")   # every other broker: active only


# ── practicetransitionsgroup: BOM in front of the REST body ─────────────────
def test_wp_json_loads_bom():
    assert wp_json_loads(b'\xef\xbb\xbf\xef\xbb\xbf[{"a": 1}]') == [{"a": 1}]
    assert wp_json_loads('﻿[1]') == [1]
    assert wp_json_loads(b'[{"a": 1}]') == [{"a": 1}]
    import pytest
    with pytest.raises(ValueError):
        wp_json_loads(b"<html>blocked</html>")                 # other garbage still fails loud


# ── superseded twin (saintlouisgroup + 47 brokers) ──────────────────────────
class _Q:
    def __init__(self, rows):
        self.rows, self.f = rows, []

    def select(self, *_):
        return self

    def in_(self, col, vals):
        self.f.append((col, set(vals)))
        return self

    def eq(self, col, val):
        self.f.append((col, {val}))
        return self

    def execute(self):
        class R:
            pass
        r = R()
        r.data = [x for x in self.rows if all(x.get(c) in v for c, v in self.f)]
        return r


class _Client:
    def __init__(self, rows):
        self.rows = rows

    def table(self, _):
        return _Q(self.rows)


def _db(*specs):
    return [{"id": i, "url": u, "broker_domain": d, "status": s} for i, u, d, s in specs]


def test_superseded_row_is_sent_under_its_active_twin():
    db = _db(("new1", "https://b.com/x/", "b.com", "superseded"), ("old1", "https://b.com/x/", "b.com", "active"))
    rows, st = m.remap_superseded_ids(_Client(db), [{"id": "new1", "url": "https://b.com/x/", "broker_domain": "b.com"}],
                                      log=lambda *_: None)
    assert rows[0]["id"] == "old1" and st["remapped"] == 1


def test_remap_must_not_touch_normal_ambiguous_or_twinless_rows():
    db = _db(("a", "https://b.com/a/", "b.com", "active"),
             ("s", "https://b.com/s/", "b.com", "superseded"),                       # no active twin
             ("t", "https://b.com/t/", "b.com", "superseded"),
             ("t1", "https://b.com/t/", "b.com", "active"), ("t2", "https://b.com/t/", "b.com", "active"),   # ambiguous
             ("u", "https://b.com/u/", "b.com", "superseded"),
             ("u1", "https://c.com/u/", "c.com", "active"))                          # same url, other broker
    rows = [{"id": i, "url": f"https://b.com/{i}/", "broker_domain": "b.com"} for i in "astu"]
    out, st = m.remap_superseded_ids(_Client(db), rows, log=lambda *_: None)
    assert [r["id"] for r in out] == list("astu")
    assert st["remapped"] == 0 and st["no_active_twin"] == 2 and st["ambiguous"] == 1


def test_remap_never_raises():
    class Boom:
        def table(self, _):
            raise RuntimeError("down")
    msgs = []
    rows = [{"id": "x"}]
    out, st = m.remap_superseded_ids(Boom(), rows, log=msgs.append)
    assert out == rows and st["lookup_failed"] == 1 and "failed" in msgs[0]
    assert m.remap_superseded_ids(None, rows)[0] == rows
