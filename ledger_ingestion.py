"""
DealLedger — Canonical Ingestion Layer
Every listing from every source flows through ledger_upsert().
"""

import csv, hashlib, json, os, re
from datetime import datetime, timezone
from urllib.parse import urlparse

LEDGER_FILE  = "data/ledger.jsonl"
LATEST_FILE  = "data/latest.json"
CHANGES_FILE = "data/changes.jsonl"

US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
    'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
    'VA','WA','WV','WI','WY','DC'
}

JUNK_TITLES = {
    'about','businesses for sale','accessibility statement','free sign up',
    'opt-out preferences','quick links','privacy policy','terms of service',
    'contact us','home','sitemap','cookie policy','sign in','login','register',
    'search results','page not found','recently sold','in contract',
    'in contract - sale pending','sale pending','how to sell your business',
    'the buying process'
}

JUNK_URL_PATTERNS = [
    '/privacy-policy', '/terms-of-service', '/about-us', '/contact-us',
    '/sitemap', '/cookie-consent', '/office-locations', '/sign-in', '/login',
    '/business-broker-office', '/broker-profile', 'cookieyes.com',
]


# ── 1. CANONICALIZE ──────────────────────────────────────────────────────────

def canonicalize(raw: dict, source: str = "") -> dict | None:
    title = (raw.get('title') or raw.get('name') or '').strip()
    if not title or title.lower() in JUNK_TITLES or len(title) < 5:
        return None

    source_url = (
        raw.get('source_url') or raw.get('listing_url') or
        raw.get('url') or raw.get('link') or ''
    ).strip()

    if not _is_valid_url(source_url):
        return None

    broker_name = (
        raw.get('broker_name') or raw.get('broker') or
        raw.get('company') or source or ''
    ).strip()

    asking_price = _clean_price(raw.get('asking_price') or raw.get('price') or '')
    revenue      = _clean_price(raw.get('revenue') or raw.get('gross_revenue') or '')
    cash_flow    = _clean_price(raw.get('cash_flow') or raw.get('sde') or raw.get('ebitda') or '')
    state        = _clean_state(raw.get('state') or raw.get('location') or '')
    city         = (raw.get('city') or '').strip().title()
    business_type = (raw.get('business_type') or raw.get('category') or raw.get('vertical') or '').strip().lower()

    return {
        'title':         title,
        'source_url':    source_url,
        'broker_name':   broker_name,
        'asking_price':  asking_price,
        'revenue':       revenue,
        'cash_flow':     cash_flow,
        'state':         state,
        'city':          city,
        'business_type': business_type,
        'status':        raw.get('status', 'active'),
        'raw_source':    source,
    }


def _is_valid_url(url: str) -> bool:
    if not url or not url.startswith('http'):
        return False
    url_lower = url.lower()
    if any(p in url_lower for p in JUNK_URL_PATTERNS):
        return False
    path = urlparse(url).path.rstrip('/')
    return len(path) >= 5


def _clean_price(val) -> str:
    if not val: return ''
    s = str(val).strip().upper().replace(',','').replace('$','').replace(' ','')
    if not s or s in ('N/A','NONE','NULL','-','—','0','0.0'): return ''
    try:
        if s.endswith('M'): return str(int(float(s[:-1]) * 1_000_000))
        if s.endswith('K'): return str(int(float(s[:-1]) * 1_000))
        v = int(float(s))
        return str(v) if v > 0 else ''
    except:
        return ''


def _clean_state(val) -> str:
    if not val: return ''
    s = str(val).strip().upper()
    if s in US_STATES: return s
    m = re.search(r'\b([A-Z]{2})\b', s)
    if m and m.group(1) in US_STATES: return m.group(1)
    return ''


# ── 2. FINGERPRINT + ID ──────────────────────────────────────────────────────

def fingerprint(listing: dict) -> str:
    key = f"{listing['title']}|{listing['asking_price']}|{listing['broker_name']}"
    return hashlib.sha1(key.encode()).hexdigest()


def listing_id(listing: dict) -> str:
    url = listing.get('source_url') or listing.get('title', '')
    return hashlib.md5(url.encode()).hexdigest()


# ── 3. LEDGER UPSERT ─────────────────────────────────────────────────────────

def ledger_upsert(raw_listings: list, source: str = "") -> dict:
    now   = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    existing = _load_ledger()
    stats    = {'inserted': 0, 'updated': 0, 'unchanged': 0, 'rejected': 0}
    changes  = []

    for raw in raw_listings:
        canonical = canonicalize(raw, source)
        if canonical is None:
            stats['rejected'] += 1
            continue

        lid = listing_id(canonical)
        fp  = fingerprint(canonical)
        canonical['id']          = lid
        canonical['fingerprint'] = fp

        if lid in existing:
            prev = existing[lid]
            prev['last_seen'] = now

            if prev.get('fingerprint') != fp:
                # Detect what changed
                changed_field = None
                old_val = new_val = None
                if prev.get('asking_price') != canonical['asking_price']:
                    changed_field = 'asking_price'
                    old_val = prev.get('asking_price')
                    new_val = canonical['asking_price']
                elif prev.get('title') != canonical['title']:
                    changed_field = 'title'
                    old_val = prev.get('title')
                    new_val = canonical['title']

                if changed_field:
                    changes.append({
                        'listing_id':   lid,
                        'changed_at':   now,
                        'field':        changed_field,
                        'old_value':    old_val,
                        'new_value':    new_val,
                        'broker_name':  prev.get('broker_name'),
                        'source_url':   prev.get('source_url'),
                    })

                prev['fingerprint']   = fp
                prev['asking_price']  = canonical['asking_price']
                prev['title']         = canonical['title']
                prev['status']        = 'active'
                stats['updated'] += 1
            else:
                stats['unchanged'] += 1

            existing[lid] = prev
        else:
            canonical['first_seen'] = now
            canonical['last_seen']  = now
            existing[lid]           = canonical
            stats['inserted'] += 1

    _save_ledger(existing)
    if changes:
        _append_changes(changes)
    _build_latest(existing, today)

    print(f"[ledger_upsert] source={source or 'unknown'} "
          f"inserted={stats['inserted']} updated={stats['updated']} "
          f"unchanged={stats['unchanged']} rejected={stats['rejected']}")
    return stats


# ── 4. DISAPPEARANCE DETECTION ───────────────────────────────────────────────

def mark_disappeared(source: str, seen_ids: set) -> list:
    now      = datetime.now(timezone.utc).isoformat()
    existing = _load_ledger()
    disappeared = []

    for lid, listing in existing.items():
        if listing.get('raw_source') == source and lid not in seen_ids:
            if listing.get('status') == 'active':
                listing['status']    = 'disappeared'
                listing['last_seen'] = now
                disappeared.append(lid)
                existing[lid] = listing

    if disappeared:
        _save_ledger(existing)
        print(f"[mark_disappeared] source={source} disappeared={len(disappeared)}")

    return disappeared


# ── 5. SNAPSHOT DIFF ─────────────────────────────────────────────────────────

def build_daily_diff(today: str, yesterday: str) -> dict:
    """Compare today vs yesterday snapshot and return diff stats."""
    def load_snapshot(date):
        path = f"data/snapshots/{date}/listings.json"
        if not os.path.exists(path): return {}
        with open(path) as f:
            listings = json.load(f)
        return {l['id']: l for l in listings}

    today_map     = load_snapshot(today)
    yesterday_map = load_snapshot(yesterday)

    new_ids         = set(today_map) - set(yesterday_map)
    removed_ids     = set(yesterday_map) - set(today_map)
    relisted_ids    = {lid for lid in new_ids if yesterday_map.get(lid, {}).get('status') == 'disappeared'}

    diff = {
        'date':          today,
        'new':           len(new_ids),
        'removed':       len(removed_ids),
        'relisted':      len(relisted_ids),
        'total_active':  len(today_map),
    }

    os.makedirs(f'data/snapshots/{today}', exist_ok=True)
    with open(f'data/snapshots/{today}/diff.json', 'w') as f:
        json.dump(diff, f, indent=2)

    print(f"[build_daily_diff] new={diff['new']} removed={diff['removed']} relisted={diff['relisted']}")
    return diff


# ── 6. FILE I/O ──────────────────────────────────────────────────────────────

def _load_ledger() -> dict:
    if not os.path.exists(LEDGER_FILE): return {}
    ledger = {}
    with open(LEDGER_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    l = json.loads(line)
                    ledger[l['id']] = l
                except: pass
    return ledger


def _save_ledger(ledger: dict):
    os.makedirs('data', exist_ok=True)
    with open(LEDGER_FILE, 'w') as f:
        for listing in ledger.values():
            f.write(json.dumps(listing, default=str) + '\n')


def _append_changes(changes: list):
    os.makedirs('data', exist_ok=True)
    with open(CHANGES_FILE, 'a') as f:
        for change in changes:
            f.write(json.dumps(change, default=str) + '\n')


def _build_latest(ledger: dict, today: str):
    active = [l for l in ledger.values() if l.get('status') != 'disappeared']
    active.sort(key=lambda x: x.get('first_seen', ''), reverse=True)

    os.makedirs('data', exist_ok=True)
    with open(LATEST_FILE, 'w') as f:
        json.dump(active, f, default=str)

    os.makedirs(f'data/snapshots/{today}', exist_ok=True)
    with open(f'data/snapshots/{today}/listings.json', 'w') as f:
        json.dump(active, f, default=str)

    print(f"[build_latest] {len(active)} active listings → {LATEST_FILE}")


# ── 7. SEED FROM EXISTING DATA ───────────────────────────────────────────────

def seed_from_latest():
    """One-time: seed the ledger from existing latest.json."""
    if not os.path.exists(LATEST_FILE):
        print("No latest.json found")
        return

    with open(LATEST_FILE) as f:
        listings = json.load(f)

    print(f"Seeding ledger from {len(listings)} existing listings...")
    stats = ledger_upsert(listings, source="seed")
    print(f"Seed complete: {stats}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'seed':
        seed_from_latest()
    else:
        # Test
        test = [
            {'title': 'Pizza Restaurant NYC', 'source_url': 'https://example.com/listings/pizza-nyc-123', 'broker_name': 'Test Broker', 'asking_price': '$500,000', 'state': 'NY'},
            {'title': 'Car Wash Florida', 'source_url': 'https://example.com/listings/carwash-fl-456', 'broker_name': 'Test Broker', 'asking_price': '1.2M', 'state': 'FL'},
            {'title': 'About', 'source_url': 'https://example.com/about', 'broker_name': 'Test Broker'},
            {'title': 'Office Deli Houston', 'source_url': 'https://linkbusiness.com/businesses-for-sale/HT00164/Office-Deli-In-SW-Houston', 'broker_name': 'Link Business', 'asking_price': '150000', 'state': 'TX'},
        ]
        stats = ledger_upsert(test, source="test")
        print("\nStats:", stats)
        print("\nLedger:")
        ledger = _load_ledger()
        for l in ledger.values():
            print(f"  {l['id'][:8]}... {l['title']} | {l['asking_price']} | {l['state']} | {l['status']}")
