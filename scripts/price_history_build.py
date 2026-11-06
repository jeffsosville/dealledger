#!/usr/bin/env python3
"""
price_history_build.py — build the historical price-history load files from the
committed snapshot archive (data/snapshots/YYYY-MM-DD/listings*.csv|json).

Read-only with respect to the repo and the database. Writes three CSV files to
--out:

  source_files.csv     one row per snapshot file: id, path, the git commit that
                       first added it, which version was read, row count.
  price_evidence.csv   one row per snapshot row (eligible AND excluded): file id,
                       row number, date, id and price as recorded, and the
                       exclusion reason if any. Nothing dropped.
  listing_identity.csv identity URL -> current listings_direct id, link basis and
                       identity status (ok / ambiguous).
Outputs go to the private price-history tables via price_history_load.py.
  price_changes.csv    candidate price changes between consecutive dated
                       observations of one listing, with the screening result.

Deterministic: same inputs -> byte-identical outputs (sorted, no timestamps).

Inputs besides the repo, either:
  --from-db  read listings_direct statuses and broker_block with SUPABASE_URL /
             SUPABASE_SERVICE_KEY (read-only), or
  --status   JSON {listings_direct.id: [status_code, published]}
             (a=active i=inactive s=sold u=superseded p=parse_error q=quarantined*)
  --blocked  JSON list of blocked broker domains.

Usage (from the repo root, on a machine with full git history):
  python3 scripts/price_history_build.py --from-db --reviews data/price_history_reviews.csv --out /tmp/ph

Methodology: claude/price-history-evidence-2026-11-06.md and
claude/price-history-validation-2026-11-06.md in the DealLedger project.
"""
import argparse
import collections
import csv
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import date

csv.field_size_limit(10**9)
RULES_VERSION = "2026-11-06.v6"
# v6 tiers. hard: extraction/identity failure, never reviewed or published.
# review: plausible but unusual, needs a human verdict. pass: no flags (still reviewed).
# "listing has no public page" is a publication restriction only (the public view
# requires a registry page); it never changes the tier.
HARD_REASONS = {
    "url is an index page", "not a broker page (platform or file host)", "placeholder or missing title",
    "placeholder or implausible value", "price swapped with a revenue or cash-flow figure",
    "price returns to an earlier value (flip-flop)", "price contradicts an asking figure in the title",
    "price equals a revenue or profit figure in the title", "ratio outside 0.2-5x",
    "one-day odd prior price with a large move", "odd values on both sides with a large move",
    "specific titles disagree", "state changed", "city changed",
    "price shared with 10+ other listings of this domain that day"}
REVIEW_REASONS = {
    "change over 60% (needs external evidence)", "price over 15x cash flow", "price over 10x revenue",
    "price equals revenue or cash flow on the same row", "price shared with other listings of this domain that day",
    "more than 3 changes on one listing", "cash flow changed more than 2x",
    "price and cash flow scaled by the same factor", "change under 0.5%"}
PUBLICATION_REASONS = {"listing has no public page"}

# Files rewritten after their first commit: read the version as first committed.
REWRITTEN = {
    "data/snapshots/2026-02-23/listings.json",
    "data/snapshots/2026-03-05/listings.json",
    "data/snapshots/2026-09-02/listings.csv",
    "data/snapshots/2026-09-02/listings.json",
}
MARKETPLACES = ("bizbuysell.com", "bizquest.com", "businessesforsale.com", "businessbroker.net",
                "bizben.com", "loopnet.com", "dealstream.com", "flippa.com")
NOT_BROKER_PAGE = ("dealrelations.com", "wsimg.com", "godaddysites.com", "wixsite.com",
                   "squarespace.com", "leadconnectorhq.com", "google.com", "dropbox.com", ".pdf")
PLACEHOLDER_PRICES = {1, 9999999, 99999999, 100000000, 123456789, 999999999, 1000000000}
GENERIC_TITLE = re.compile(
    r"^(more details|view details|read more|details|businesses? for sale|listings?|search|results?|"
    r"login|home|available businesses|current listings|our listings|active in escrow|under contract|"
    r"sold|pending)\b|for sale:?$", re.I)
INDEX_PATH = re.compile(r"/(status|search|listings?|businesses-for-sale|business-listings|"
                        r"current-listings|our-listings)/?$|/status/|/search/?\?|/category/", re.I)
MONEY = re.compile(r"\$\s?(\d[\d,]*(?:\.\d+)?)\s*([kKmM])?")
CUR_ID = re.compile(r"^([0-9a-f]{16}|spec:.+)$")


def identity_url(u):
    """norm_listing_url() without dropping the query string (it distinguishes listings).
    Must match private.identity_url() in sql/2026-11-06_price_history.sql."""
    u = re.sub(r"^\s+|\s+$", "", u or "").split("#")[0]
    u = re.sub(r"^https?://(www\.)?", "", u, flags=re.I)
    path, _, query = u.partition("?")
    path = re.sub(r"/+$", "", path).lower()
    return f"{path}?{query.lower()}" if query else path


def host(u):
    m = re.match(r"^https?://([^/:?#]+)", (u or "").strip().lower())
    return re.sub(r"^www\.", "", m.group(1)) if m else ""


def num(v):
    v = ("" if v is None else str(v)).strip().replace("$", "").replace(",", "")
    try:
        return float(v) if v else None
    except ValueError:
        return None


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def first_commits():
    out, cur = {}, None
    for line in git("log", "--reverse", "--format=@@%H", "--name-status", "--", "data/snapshots").splitlines():
        if line.startswith("@@"):
            cur = line[2:]
            continue
        p = line.split("\t")
        if len(p) >= 2 and p[0].startswith("A") and p[-1] not in out:
            out[p[-1]] = cur
    return out


SKIPPED = []


def snapshot_rows(first):
    for d in sorted(x for x in os.listdir("data/snapshots") if re.match(r"^\d{4}-\d{2}-\d{2}$", x)):
        files = sorted(glob.glob(f"data/snapshots/{d}/listings*.csv")) or \
                sorted(glob.glob(f"data/snapshots/{d}/listings*.json"))
        for path in files:
            # Only the committed archive is evidence. A file git has never seen has no
            # verifiable date, and an uncommitted local edit is not what was observed
            # that day, so the working tree is never read. Files rewritten on a later
            # day are read as first committed; all others as committed at HEAD
            # (same-day re-runs commit the day's final snapshot).
            sha = first.get(path, "")
            if not sha:
                SKIPPED.append(path)
                continue
            if path in REWRITTEN:
                text, version = git("show", f"{sha}:{path}"), "first_commit"
            else:
                text, version = git("show", f"HEAD:{path}"), "head"
            if path.endswith(".csv"):
                rows = csv.DictReader(text.splitlines(True))
            else:
                j = json.loads(text)
                rows = j if isinstance(j, list) else j.get("listings", [])
            for n, r in enumerate(rows):
                url = r.get("url") or r.get("source_url") or ""
                desc = (r.get("description") or "").strip().lower()
                yield {
                    "snapshot_date": d, "source_file": path, "source_commit": sha, "file_version": version,
                    "row_index": n, "listing_id": r.get("id") or "", "url": url,
                    "identity_url": identity_url(url),
                    "domain": re.sub(r"^www\.", "", (r.get("broker_domain") or host(url)).lower()),
                    "asking_price": num(r.get("asking_price")), "cash_flow": num(r.get("cash_flow")),
                    "revenue": num(r.get("revenue")), "city": (r.get("city") or "").strip(),
                    "state": (r.get("state") or "").strip().upper(), "title": (r.get("title") or "").strip(),
                    "desc_hash": hashlib.md5(desc[:500].encode()).hexdigest()[:12] if desc else "",
                }


def title_tokens(t):
    t = re.split(r"\s[|\-–—]\s", t or "")[0].lower()
    return set(re.findall(r"[a-z0-9]+", t)) - {"for", "sale", "the", "a", "and", "of", "in", "business",
                                               "with", "available", "resale"}


def money_in(t):
    for m in MONEY.finditer(t or ""):
        v = float(m.group(1).replace(",", ""))
        s = (m.group(2) or "").lower()
        v *= 1000 if s == "k" else 1e6 if s == "m" else 1
        yield v, (t[max(0, m.start() - 25):m.end() + 25]).lower()


def fetch_from_db():
    import requests
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY")
    if not (url and key):
        sys.exit("--from-db needs SUPABASE_URL and SUPABASE_SERVICE_KEY")
    h = {"apikey": key, "Authorization": f"Bearer {key}"}
    code = {"active": "a", "inactive": "i", "sold": "s", "superseded": "u", "parse_error": "p"}
    status, offset = {}, 0
    while True:
        r = requests.get(f"{url}/rest/v1/listings_direct", headers={**h, "Range": f"{offset}-{offset + 9999}"},
                         params={"select": "id,status,published", "order": "id"}, timeout=120)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for x in rows:
            status[x["id"]] = [code.get(x["status"], "q"), bool(x["published"])]
        offset += len(rows)          # the API may cap page size below what was asked
    if len(status) < 1000:
        sys.exit(f"--from-db read only {len(status)} listings; refusing to build")
    r = requests.get(f"{url}/rest/v1/broker_block", headers=h, params={"select": "broker_domain"}, timeout=120)
    r.raise_for_status()
    blocked = {re.sub(r"^www\.", "", (x["broker_domain"] or "").lower()) for x in r.json()}
    return status, blocked


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from-db", action="store_true")
    ap.add_argument("--status")
    ap.add_argument("--blocked")
    ap.add_argument("--reviews", help="CSV of hand reviews: listing_id,next_first_seen,verdict,reviewer,ref")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    if a.from_db:
        status, blocked = fetch_from_db()
    elif a.status and a.blocked:
        status, blocked = json.load(open(a.status)), set(json.load(open(a.blocked)))
    else:
        sys.exit("give --from-db, or both --status and --blocked")
    blocked.discard("")
    os.makedirs(a.out, exist_ok=True)

    first = first_commits()
    rows = list(snapshot_rows(first))
    paths = sorted({r["source_file"] for r in rows})
    file_id = {p: n for n, p in enumerate(paths, 1)}
    file_rows = collections.Counter(r["source_file"] for r in rows)
    file_ver = {r["source_file"]: r["file_version"] for r in rows}
    with open(os.path.join(a.out, "source_files.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["file_id", "path", "first_commit", "file_version", "snapshot_date", "row_count"])
        for p in paths:
            w.writerow([file_id[p], p, first.get(p, ""), file_ver[p], p.split("/")[2], file_rows[p]])
    for r in rows:
        r["file_id"] = file_id[r["source_file"]]
    by_url = collections.defaultdict(list)
    for r in rows:
        if r["identity_url"] and r["listing_id"]:
            by_url[r["identity_url"]].append(r)

    # ---- identity ----
    identity = {}
    for u, rs in by_url.items():
        ids = collections.defaultdict(set)
        for r in rs:
            ids[r["listing_id"]].add(r["snapshot_date"])
        per_day = collections.Counter(d for ds in ids.values() for d in ds)
        latest = max(ids, key=lambda i: (max(ids[i]), i))
        if len(ids) > 1 and any(c > 1 for c in per_day.values()):
            identity[u] = (latest, "ambiguous", "several ids on the same day (shared or index page)", len(ids))
        elif not CUR_ID.match(latest):
            identity[u] = (latest, "ambiguous", "only old-format ids; never seen by the current crawler", len(ids))
        elif latest not in status:
            identity[u] = (latest, "ambiguous", "current id not in listings_direct", len(ids))
        else:
            identity[u] = (latest, "ok", "same id" if len(ids) == 1 else "sequential id change", len(ids))

    # ---- evidence: eligibility per row ----
    def exclusion(r):
        if not r["url"] or not r["identity_url"]:
            return "no or invalid url"
        h = host(r["url"])
        if any(h == m or h.endswith("." + m) for m in MARKETPLACES):
            return "marketplace url"
        if h in blocked or r["domain"] in blocked:
            return "blocked domain"
        ident = identity.get(r["identity_url"])
        if not ident:
            return "no listing id"
        if ident[1] != "ok":
            return "identity: " + ident[2]
        st = status[ident[0]][0]
        if st == "p":
            return "listing now parse_error"
        if st == "q":
            return "listing now quarantined"
        if r["asking_price"] is None:
            return "no price"
        if r["asking_price"] < 1000:
            return "price under $1,000"
        return ""

    for r in rows:
        r["exclusion"] = exclusion(r)

    # same-day conflicts for one listing
    day_prices = collections.defaultdict(set)
    for r in rows:
        if not r["exclusion"]:
            day_prices[(r["identity_url"], r["snapshot_date"])].add(r["asking_price"])
    for r in rows:
        if not r["exclusion"] and len(day_prices[(r["identity_url"], r["snapshot_date"])]) > 1:
            r["exclusion"] = "same-day price conflict"

    # ---- write evidence ----
    ev_cols = ["file_id", "row_index", "snapshot_date", "listing_id", "identity_url", "asking_price", "cash_flow",
               "revenue", "title", "exclusion"]
    with open(os.path.join(a.out, "price_evidence.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=ev_cols, extrasaction="ignore")
        w.writeheader()
        for r in sorted(rows, key=lambda r: (r["snapshot_date"], r["source_file"], r["row_index"])):
            w.writerow(r)

    with open(os.path.join(a.out, "listing_identity.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["identity_url", "listing_id", "identity_status", "link_basis", "ids_seen"])
        for u in sorted(identity):
            lid, st, basis, n = identity[u]
            w.writerow([u, lid, st, basis, n])

    # ---- series and changes ----
    title_spread = collections.Counter()
    seen = set()
    for r in rows:
        k = (r["domain"], r["title"].lower(), r["identity_url"])
        if r["title"] and k not in seen:
            seen.add(k)
            title_spread[(r["domain"], r["title"].lower())] += 1
    shared_price = collections.Counter()
    seen = set()
    for r in rows:
        if r["asking_price"] is None:
            continue
        k = (r["domain"], r["snapshot_date"], r["asking_price"], r["identity_url"])
        if k not in seen:
            seen.add(k)
            shared_price[(r["domain"], r["snapshot_date"], r["asking_price"])] += 1

    def placeholder_title(dom, t):
        tl = (t or "").strip().lower()
        return (not tl) or len(tl) < 6 or bool(GENERIC_TITLE.search(tl)) or title_spread[(dom, tl)] >= 3

    series = collections.defaultdict(dict)
    for r in sorted(rows, key=lambda r: (r["snapshot_date"], r["source_file"], r["row_index"])):
        if not r["exclusion"]:
            series[r["identity_url"]].setdefault(r["snapshot_date"], r)

    reviews = {}
    if a.reviews:
        for rv in csv.DictReader(open(a.reviews)):
            reviews[(rv["listing_id"], rv["next_first_seen"])] = rv

    changes = []
    for u in sorted(series):
        days = sorted(series[u].items())
        runs = []
        for d, r in days:
            if runs and runs[-1]["price"] == r["asking_price"]:
                runs[-1]["last"] = (d, r)
                runs[-1]["n"] += 1
            else:
                runs.append({"price": r["asking_price"], "first": (d, r), "last": (d, r), "n": 1})
        prices = [x["price"] for x in runs]
        flip = len(prices) != len(set(prices))
        lid = identity[u][0]
        for j in range(1, len(runs)):
            a_, b_ = runs[j - 1], runs[j]
            (d0, r0), (d1, r1) = a_["last"], b_["first"]
            p0, p1 = a_["price"], b_["price"]
            pct = round((p1 - p0) / p0 * 100, 1)
            dom = u.split("/")[0]
            why, ident_why = [], []
            ratio = p1 / p0
            if ratio < 0.2 or ratio > 5:
                why.append("ratio outside 0.2-5x")
            if p0 in PLACEHOLDER_PRICES or p1 in PLACEHOLDER_PRICES or max(p0, p1) >= 1e8:
                why.append("placeholder or implausible value")
            if flip:
                why.append("price returns to an earlier value (flip-flop)")
            if len(runs) - 1 > 3:
                why.append("more than 3 changes on one listing")
            if abs(pct) < 0.5:
                why.append("change under 0.5%")
            if abs(pct) > 60:
                why.append("change over 60% (needs external evidence)")
            for p, r in ((p0, r0), (p1, r1)):
                if (r["revenue"] and abs(p - r["revenue"]) < 1) or (r["cash_flow"] and abs(p - r["cash_flow"]) < 1):
                    why.append("price equals revenue or cash flow on the same row")
                if shared_price[(dom, r["snapshot_date"], p)] >= 3 or \
                        (shared_price[(dom, r["snapshot_date"], p)] >= 2 and p % 1000):
                    why.append("price shared with other listings of this domain that day")
                if shared_price[(dom, r["snapshot_date"], p)] - 1 >= 10:
                    why.append("price shared with 10+ other listings of this domain that day")
                if r["cash_flow"] and r["cash_flow"] > 0 and p / r["cash_flow"] > 15:
                    why.append("price over 15x cash flow")
                if r["revenue"] and 0 < r["revenue"] < 1e9 and p / r["revenue"] > 10:
                    why.append("price over 10x revenue")
                for v, ctx in money_in(r["title"]):
                    if re.search(r"revenue|sales|profit|net|cash flow|income|ebitda|gross|buildout|monthly|/mo", ctx):
                        if v >= 10000 and abs(v - p) < 1:
                            why.append("price equals a revenue or profit figure in the title")
                    elif re.search(r"ask|price|listed|for \$|only", ctx) and abs(v - p) > max(1, 0.02 * p):
                        why.append("price contradicts an asking figure in the title")
            if (r0["revenue"] and abs(p1 - r0["revenue"]) < 1) or (r0["cash_flow"] and abs(p1 - r0["cash_flow"]) < 1) \
                    or (r1["revenue"] and abs(p0 - r1["revenue"]) < 1) or (r1["cash_flow"] and abs(p0 - r1["cash_flow"]) < 1):
                why.append("price swapped with a revenue or cash-flow figure")
            if r0["cash_flow"] and r1["cash_flow"] and r0["cash_flow"] != r1["cash_flow"] and \
                    abs(r1["cash_flow"] / r0["cash_flow"] - p1 / p0) < 0.005:
                why.append("price and cash flow scaled by the same factor")
            if a_["n"] == 1 and p0 % 1000 and abs(pct) > 25:
                why.append("one-day odd prior price with a large move")
            if p0 % 1000 and p1 % 1000 and abs(pct) > 30:
                why.append("odd values on both sides with a large move")
            if any(x in u for x in NOT_BROKER_PAGE):
                why.append("not a broker page (platform or file host)")
            if INDEX_PATH.search("/" + u.split("/", 1)[-1]) or "/" not in u:
                why.append("url is an index page")
            ph0, ph1 = placeholder_title(dom, r0["title"]), placeholder_title(dom, r1["title"])
            if ph0 or ph1:
                why.append("placeholder or missing title")
            # identity checks between the two sides
            if not (ph0 or ph1):
                t0, t1 = title_tokens(r0["title"]), title_tokens(r1["title"])
                slug = set(re.findall(r"[a-z0-9]+", u.split("/", 1)[-1])) - {"listing", "listings", "business",
                                                                              "businesses", "for", "sale", "html",
                                                                              "php", "aspx"}
                matches_slug = lambda t: len(t & slug) >= 2 or (t and len(t & slug) / len(t) >= 0.5)
                if t0 and t1 and len(t0 & t1) / len(t0 | t1) < 0.34 and not (matches_slug(t0) and matches_slug(t1)):
                    ident_why.append("specific titles disagree")
            if r0["state"] and r1["state"] and r0["state"] != r1["state"]:
                ident_why.append("state changed")
            if r0["city"] and r1["city"] and r0["city"].lower() != r1["city"].lower():
                ident_why.append("city changed")
            if r0["cash_flow"] and r1["cash_flow"] and not (0.5 <= r1["cash_flow"] / r0["cash_flow"] <= 2):
                ident_why.append("cash flow changed more than 2x")
            st, pub = status[lid]
            if not pub and st == "a":
                ident_why.append("listing has no public page")
            reasons = set(why + ident_why)
            unknown = reasons - HARD_REASONS - REVIEW_REASONS - PUBLICATION_REASONS
            assert not unknown, unknown
            auto = "hard" if reasons & HARD_REASONS else ("review" if reasons & REVIEW_REASONS else "pass")
            rv = reviews.get((lid, d1), {})
            changes.append({
                "listing_id": lid, "identity_url": u,
                "prev_price": p0, "prev_first_seen": a_["first"][0], "prev_last_seen": d0, "prev_days_seen": a_["n"],
                "next_price": p1, "next_first_seen": d1, "next_last_seen": b_["last"][0], "next_days_seen": b_["n"],
                "pct_change": pct, "window_days": (date.fromisoformat(d1) - date.fromisoformat(d0)).days,
                "prev_listing_id_seen": r0["listing_id"], "next_listing_id_seen": r1["listing_id"],
                "prev_source": f"{r0['source_file']}@{r0['source_commit'][:12]}#{r0['row_index']}",
                "next_source": f"{r1['source_file']}@{r1['source_commit'][:12]}#{r1['row_index']}",
                "screen_result": auto, "screen_reasons": "; ".join(sorted(set(why + ident_why))),
                "rules_version": RULES_VERSION,
                "review_status": rv.get("verdict", "pending") if auto != "hard" else "not_eligible",
                "reviewer": rv.get("reviewer", ""), "review_ref": rv.get("ref", ""),
            })
    cols = list(changes[0]) if changes else []
    with open(os.path.join(a.out, "price_changes.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(changes)

    ex = collections.Counter(r["exclusion"] or "eligible" for r in rows)
    res = collections.Counter(c["screen_result"] for c in changes)
    rev = collections.Counter(c["review_status"] for c in changes)
    print(f"skipped {len(SKIPPED)} snapshot file(s) not in git history" + (": " + ", ".join(SKIPPED[:5]) if SKIPPED else ""))
    print(f"rules {RULES_VERSION}: evidence rows {len(rows):,}  identities {len(identity):,}  changes {len(changes):,}")
    for k, v in ex.most_common():
        print(f"  {v:>8,}  {k}")
    print("  screen:", dict(res), " review:", dict(rev))


if __name__ == "__main__":
    sys.exit(main())
