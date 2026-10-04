"""Offline checks for the discovery gate in agents/discover_backlog.py.
Run: python3 -m pytest tests/test_discovery_gate.py  (no network, no Supabase)."""
import os, sys, types
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))
import discover_backlog as db


class _R:
    def __init__(self, text, url):
        self.text, self.url, self.status_code = text, url, 200


def _ev(html, final="https://acme.com/listings"):
    dv = types.SimpleNamespace(get_page=lambda u, timeout=15: _R(html, final))
    return db.page_evidence(dv, "https://acme.com/listings", "acme.com")


BROKER = " ".join(f"<div>Established HVAC company. Asking $9{i}0,000 Cash flow $2{i}0,000 "
                  f"SDE. Business for sale listing</div>" for i in range(8))
REALTOR = " ".join(f"<div>{i}12 Oak St $45{i},000 3 beds 2 baths 1,800 sq ft MLS #{i} "
                   f"for sale listing</div>" for i in range(8))
CRE = " ".join(f"<div>Retail space for lease $2{i}/SF NNN, 4.{i} acres zoned commercial, "
               f"office for sale $1,{i}00,000 listing</div>" for i in range(8))
RESTAURANT_WITH_RE = " ".join(f"<div>Restaurant with real estate, 2,500 sq ft, asking $5{i}0,000, "
                              f"cash flow $1{i}0,000, business for sale listing</div>" for i in range(8))


def test_broker_auto_promotes():
    assert db.gate_failures(_ev(BROKER), 5, None, None) == []


def test_business_with_real_estate_still_promotes():
    assert db.gate_failures(_ev(RESTAURANT_WITH_RE), 5, None, None) == []


def test_realtor_is_proposed():
    assert any("real estate" in f for f in db.gate_failures(_ev(REALTOR), 5, None, None))


def test_cre_is_proposed():
    assert any("real estate" in f for f in db.gate_failures(_ev(CRE), 5, None, None))


def test_off_domain_is_proposed():
    fails = db.gate_failures(_ev(BROKER, "https://other.com/x"), 5, None, None)
    assert any("leaves the domain" in f for f in fails)


def test_parked_is_proposed():
    fails = db.gate_failures(_ev("<h1>Coming soon</h1> listing for sale $1 $2 $3"), 5, None, None)
    assert any("parked" in f for f in fails)


def test_few_prices_is_proposed():
    fails = db.gate_failures(_ev("listing for sale asking $100,000"), 0, None, None)
    assert any("prices" in f for f in fails)


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print(f"discovery gate: {len(tests)} checks passed")
