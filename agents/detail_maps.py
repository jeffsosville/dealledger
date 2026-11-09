#!/usr/bin/env python3
"""
Detail-page field maps — Claude writes them once per domain, Python runs them forever.

    python3 agents/detail_maps.py submit              # fetch samples, send one Batch API job
    python3 agents/detail_maps.py collect             # pull results, validate, write maps
    python3 agents/detail_maps.py backfill --domain eatz-associates.com --limit 25          # dry run
    python3 agents/detail_maps.py backfill --domain eatz-associates.com --limit 25 --write  # real
    python3 agents/detail_maps.py submit --targets repair                                  # only REPAIR_TARGETS
    python3 agents/detail_maps.py backfill --domain kensingtoncompany.com --repair         # fix wrong values (dry run)

Maps land in data/detail_maps/<domain>.json. V6 calls apply_detail_map() before its regex.
Only fills EMPTY fields. Never overwrites a value already in listings_direct —
except `backfill --repair`, which replaces revenue / cash_flow on rows whose stored
financials are impossible (revenue = asking price, cash flow = asking price or >= revenue).
--repair never writes asking_price: a corrected price would read as a price change, so a
differing price is printed for Jeff instead.
"""
import argparse, json, os, re, sys, time
from pathlib import Path

import anthropic
from bs4 import BeautifulSoup
from supabase import create_client

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
from dealledger_scraper_v6 import (PageFetcher, _needs_proxy, parse_all_prices,  # noqa: E402
                                   _sane_financials, US_STATES)

MAP_DIR = ROOT / "data" / "detail_maps"
SAMPLE_DIR = MAP_DIR / "samples"
BATCH_FILE = MAP_DIR / "_batch.json"
MODEL = "claude-sonnet-4-6"
SAMPLES_PER_DOMAIN = 3

# Top gap domains WITHOUT a dedicated scraper. The specialized ones (tworld, execbb,
# hedgestone, wesellrestaurants, sunbeltnetwork, linkbusiness, vestedbb, fcbb,
# routesforsale) get fixed inside their own scrapers, not here.
TARGETS = [
    "eatz-associates.com", "trufortebusinessgroup.com", "restaurantrealty.com",
    "www.companysellers.com", "mergerscorp.com", "strategicdc.com", "www.lbaweb.com",
    "vikingmergers.com", "quietlight.com", "salehgroup.com", "www.websiteclosers.com",
    "www.reputedbrokerage.com", "listings.routeconsultant.com",
    "corbettrestaurantgroup.com", "sellingrestaurants.com",
]
# quietlight.com and websiteclosers.com removed 2026-11-09: digital/ecommerce is out of scope.

# Domains whose STORED financials are impossible (integrity check 2026-11-09). Samples are
# drawn from the corrupt rows so the map learns the real labels.
REPAIR_TARGETS = [
    "commercialroutesforsale.com", "americanbusinessbrokers.com",   # majority-corrupt
    "kensingtoncompany.com", "eastcoastbusinessbrokers.com", "gottesman-company.com",
    "calhouncompanies.com", "nashbb.com", "regalconsultants.com", "oibmn.com",
]
MONEY = ("asking_price", "revenue", "cash_flow")
TEXT = ("city", "state")
FIELDS = MONEY + TEXT

FIELD_SPEC = {
    "type": "object", "additionalProperties": False,
    "required": ["method", "css", "label", "not_disclosed"],
    "properties": {
        "method": {"type": "string", "enum": ["css", "label", "none"]},
        "css": {"type": ["string", "null"]},
        "label": {"type": ["string", "null"]},
        "not_disclosed": {"type": "boolean"},
    },
}
SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["fields", "notes"],
    "properties": {
        "fields": {"type": "object", "additionalProperties": False,
                   "required": list(FIELDS), "properties": {f: FIELD_SPEC for f in FIELDS}},
        "notes": {"type": "string"},
    },
}

PROMPT = """These are {n} detail pages from one business broker's website, each for a different business for sale.

For each field, tell me how to find it on EVERY page of this site, not just these:
- asking_price, revenue (gross sales), cash_flow (SDE / owner benefit / discretionary earnings), city, state

Per field choose one method:
- "label": the value sits next to a text label. Give the label exactly as written (e.g. "Gross Revenue:"). Preferred when labels are consistent.
- "css": a CSS selector (BeautifulSoup/soupsieve syntax) that returns only that value. Use stable classes or ids, never nth-child chains.
- "none": the site does not show this field.

Set not_disclosed=true only if the field is absent from ALL pages because the broker doesn't publish it.
Do not confuse asking price with revenue, or EBITDA/net income with cash flow unless the site uses it as cash flow.

{pages}"""


def sb():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def slim(html, limit=40_000):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "svg", "noscript", "iframe", "header", "footer", "nav"]):
        t.decompose()
    for t in soup.find_all(True):
        t.attrs = {k: v for k, v in t.attrs.items() if k in ("class", "id")}
    return str(soup)[:limit]


def cid(domain):
    return re.sub(r"[^a-zA-Z0-9_-]", "_", domain)[:64]


def _impossible(r):
    f = lambda v: float(v) if v not in (None, "") else None
    p, cf, rv = f(r.get("asking_price")), f(r.get("cash_flow")), f(r.get("revenue"))
    return bool((rv and p and rv == p) or (cf and p and cf == p)
                or (cf and rv and rv > 0 and cf >= rv))


def bad_urls(db, domain, n):
    rows = (db.table("listings_direct").select("url,asking_price,cash_flow,revenue")
            .eq("broker_domain", domain).eq("status", "active").eq("public", True)
            .limit(500).execute().data)
    return [r["url"] for r in rows if r.get("url") and _impossible(r)][:n * 2]


def gap_urls(db, domain, n):
    rows = (db.table("listings_direct").select("url")
            .eq("broker_domain", domain).eq("status", "active").eq("public", True)
            .or_("revenue.is.null,cash_flow.is.null").limit(n * 4).execute().data)
    return [r["url"] for r in rows if r.get("url")][:n * 2]


# ---------------------------------------------------------------- submit
def submit(which="all"):
    db, fetcher, requests = sb(), PageFetcher(), []
    targets = {"all": TARGETS + REPAIR_TARGETS, "gaps": TARGETS, "repair": REPAIR_TARGETS}[which]
    for domain in targets:
        pages, out = [], SAMPLE_DIR / domain
        out.mkdir(parents=True, exist_ok=True)
        pick = bad_urls if domain in REPAIR_TARGETS else gap_urls
        for url in pick(db, domain, SAMPLES_PER_DOMAIN):
            if len(pages) == SAMPLES_PER_DOMAIN:
                break
            try:
                html, _ = fetcher.fetch(url, use_proxy=_needs_proxy(domain))
                (out / f"{len(pages)}.html").write_text(html)
                pages.append(f'<page url="{url}">\n{slim(html)}\n</page>')
            except Exception as e:
                print(f"  {domain}: fetch failed {url} ({e})")
            time.sleep(1)
        if len(pages) < 2:
            print(f"✗ {domain}: only {len(pages)} pages — skipped")
            continue
        requests.append({"custom_id": cid(domain), "params": {
            "model": MODEL, "max_tokens": 2000,
            "output_config": {"format": {"type": "json_schema", "schema": SCHEMA}},
            "messages": [{"role": "user", "content":
                          PROMPT.format(n=len(pages), pages="\n\n".join(pages))}],
        }})
        print(f"✓ {domain}: {len(pages)} samples")
    fetcher.close()
    if not requests:
        sys.exit("nothing to submit")
    batch = anthropic.Anthropic().messages.batches.create(requests=requests)
    BATCH_FILE.write_text(json.dumps({"id": batch.id, "domains": {cid(d): d for d in targets}}))
    print(f"\nBatch {batch.id} submitted ({len(requests)} domains). Run `collect` once it ends (<24h).")


# ---------------------------------------------------------------- extraction
def _money(text):
    vals = [v for v in parse_all_prices(text or "") if v >= 1_000]
    return vals[0] if vals else None


def _by_label(soup, label):
    pat = re.compile(re.escape(label.rstrip(": ").strip()), re.I)
    for s in soup.find_all(string=pat):
        node = s.parent
        after = node.get_text(" ", strip=True)
        m = pat.search(after)
        tail = after[m.end():] if m else ""
        if tail.strip(" :-"):
            return tail.strip(" :-")
        sib = node.find_next_sibling() or (node.parent and node.parent.find_next_sibling())
        if sib:
            return sib.get_text(" ", strip=True)
    return None


def apply_detail_map(domain, html):
    """Return {field: value} for fields the map can find on this page. Used by V6 and backfill."""
    path = MAP_DIR / f"{domain}.json"
    if not path.exists():
        return {}
    m = json.loads(path.read_text())
    if m.get("status") != "validated":
        return {}
    soup, out = BeautifulSoup(html, "html.parser"), {}
    for f, spec in m["fields"].items():
        raw = None
        try:
            if spec["method"] == "css" and spec.get("css"):
                el = soup.select_one(spec["css"])
                raw = el.get_text(" ", strip=True) if el else None
            elif spec["method"] == "label" and spec.get("label"):
                raw = _by_label(soup, spec["label"])
        except Exception:
            raw = None
        if not raw:
            continue
        if f in MONEY:
            out[f] = _money(raw)
        elif f == "state":
            st = re.search(r"\b([A-Z]{2})\b", raw)
            out[f] = st.group(1) if st and st.group(1) in US_STATES else None
        else:
            out[f] = raw[:80]
    out = {k: v for k, v in out.items() if v}
    if any(k in out for k in MONEY):
        price, cf, rev = _sane_financials(
            out.get("asking_price"), out.get("cash_flow"), out.get("revenue"))
        for k, v in (("asking_price", price), ("cash_flow", cf), ("revenue", rev)):
            if v:
                out[k] = v
            else:
                out.pop(k, None)
    return out


# ---------------------------------------------------------------- collect
def collect():
    meta = json.loads(BATCH_FILE.read_text())
    client = anthropic.Anthropic()
    b = client.messages.batches.retrieve(meta["id"])
    if b.processing_status != "ended":
        sys.exit(f"Batch still {b.processing_status}")
    for r in client.messages.batches.results(meta["id"]):
        domain = meta["domains"].get(r.custom_id, r.custom_id)
        if r.result.type != "succeeded":
            print(f"✗ {domain}: {r.result.type}")
            continue
        text = "".join(c.text for c in r.result.message.content if c.type == "text")
        try:
            m = json.loads(text)
        except json.JSONDecodeError:
            print(f"✗ {domain}: unparseable")
            continue
        m.update(domain=domain, model=MODEL, status="validated",
                 generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        path = MAP_DIR / f"{domain}.json"
        path.write_text(json.dumps(m, indent=2))
        # Validate against the saved samples: each non-"none" field must hit on ≥2 of them.
        samples = sorted((SAMPLE_DIR / domain).glob("*.html"))
        hits = {f: 0 for f in FIELDS}
        for s in samples:
            for f in apply_detail_map(domain, s.read_text()):
                hits[f] += 1
        needed = [f for f, spec in m["fields"].items() if spec["method"] != "none"]
        weak = [f for f in needed if hits[f] < min(2, len(samples))]
        if weak:
            m["status"] = "needs_review"
            m["weak_fields"] = weak
            path.write_text(json.dumps(m, indent=2))
        nd = [f for f, s in m["fields"].items() if s["not_disclosed"]]
        print(f"{'✓' if not weak else '~'} {domain}: hits {hits}"
              + (f"  weak={weak}" if weak else "") + (f"  not_disclosed={nd}" if nd else ""))


# ---------------------------------------------------------------- backfill
def backfill(domain, limit, write, repair=False):
    db, fetcher = sb(), PageFetcher()
    q = (db.table("listings_direct").select("id,url," + ",".join(FIELDS))
         .eq("broker_domain", domain).eq("status", "active").eq("public", True))
    if repair:
        rows = [r for r in q.limit(1000).execute().data if _impossible(r)][:limit]
    else:
        rows = q.or_("revenue.is.null,cash_flow.is.null,state.is.null").limit(limit).execute().data
    filled, price_diffs = 0, []
    for r in rows:
        try:
            html, _ = fetcher.fetch(r["url"], use_proxy=_needs_proxy(domain))
        except Exception as e:
            print(f"  fetch failed {r['url']} ({e})")
            continue
        found = apply_detail_map(domain, html)
        if repair:
            if found.get("asking_price") and r.get("asking_price") and found["asking_price"] != float(r["asking_price"]):
                price_diffs.append((r["id"], r["url"], r["asking_price"], found["asking_price"]))
            patch = {k: v for k, v in found.items()
                     if k in ("revenue", "cash_flow") and v
                     and v != (float(r[k]) if r.get(k) not in (None, "") else None)}
            patch.update({k: v for k, v in found.items() if k in TEXT and not r.get(k)})
        else:
            patch = {k: v for k, v in found.items() if not r.get(k)}  # empty fields only
        if patch:
            filled += 1
            print(f"  {r['id']}: {patch}")
            if write:
                db.table("listings_direct").update(patch).eq("id", r["id"]).execute()
        time.sleep(1)
    fetcher.close()
    print(f"\n{domain}: {filled}/{len(rows)} listings {'repaired' if repair else 'gained fields'}"
          + ("" if write else "  (dry run — add --write)"))
    if price_diffs:
        print(f"\nAsking price on the page differs from the stored price on {len(price_diffs)} rows "
              "(NOT written — needs Jeff):")
        for pid, url, old, new in price_diffs:
            print(f"  {pid}  stored {old}  page {new}  {url}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["submit", "collect", "backfill"])
    p.add_argument("--domain")
    p.add_argument("--limit", type=int, default=25)
    p.add_argument("--write", action="store_true")
    p.add_argument("--repair", action="store_true")
    p.add_argument("--targets", choices=["all", "gaps", "repair"], default="all")
    a = p.parse_args()
    MAP_DIR.mkdir(parents=True, exist_ok=True)
    if a.cmd == "submit":
        submit(a.targets)
    elif a.cmd == "collect":
        collect()
    else:
        if not a.domain:
            sys.exit("--domain required")
        backfill(a.domain, a.limit, a.write, a.repair)
