"""
listing_status — one broker-status policy for every writer (2026-11-12).

Why this exists
---------------
Before this module the same broker label meant different things depending on
which code path read it (SUMMARY_job2.md decision 6):

    "Under Contract"  WP-REST (W4) -> inactive   businessusa (W11) -> active
    "Sale Pending"    WP-REST (W4) -> inactive   ninbb (W12)       -> active
    "Coming Soon!"    WP-REST (W4) -> inactive   (emitted as a row)

Two code paths writing one concept must write it the same way (principle 15).
Jeff's decisions 5 and 6 (2026-11-11 23:50 UTC) set the policy:

  * sold       — the broker says the deal closed (SOLD, Sold!, SOLD FOR ...).
  * active + label — Under Contract / Pending / Sale Pending / Under LOI /
                 Has Accepted Offer / Accepted Offer / In Contract /
                 Off Market: still on
                 the broker's list, so still active; the broker's own words
                 are carried verbatim as broker_status_text.
  * DROP       — Coming Soon (not a listing yet) and For Lease (out of scope):
                 the row is not emitted at all. Callers count and log drops.
  * inactive   — Not Available / Withdrawn / Withdraw / Expired / On Hold.
                 ("Withdraw" is lbaweb.com's card value, 2 cards on 12 Nov.)

Per-domain overrides cover a broker whose vocabulary differs
(reputedbrokerage.com: "Expired" means sold, decision 7).

Everything here is pure: no network, no DB, no mutation of arguments.
Callers import it defensively and keep their local mapping as the fallback:

    try:
        from listing_status import classify_status, apply_status, DROP
    except Exception:
        classify_status = None
"""
from __future__ import annotations

import re
from types import MappingProxyType
from typing import Optional, Tuple

DROP = "DROP"   # sentinel status: the caller must not emit the row

# Most final wins when two signals disagree (a 'Sold' badge beats a
# 'For Sale' taxonomy term; Coming Soon beats "not on the index").
STATUS_RANK = MappingProxyType({"active": 0, "inactive": 1, DROP: 2, "sold": 3})

# ── the constant set (normalised phrases: lower case, single spaces) ────────
SOLD_PHRASES = frozenset({
    "sold", "sold for", "just sold", "recently sold", "just closed", "closed",
    "sold out", "soldout", "sold closed", "sold settled", "deal closed",
})
UNDER_CONTRACT_PHRASES = frozenset({
    "under contract", "undercontract", "contract pending", "pending",
    "sale pending", "pending sale", "under loi", "loi", "loi accepted",
    "under offer", "accepted offer", "has accepted offer", "offer accepted",
    "under agreement", "off market", "offmarket",
    # "In Contract": wesellrestaurants.com's feed word (bstatuslist, 124 of
    # 1,417 records on 11 Nov). Added 2026-11-12 so the feed needs no local
    # alias (principle 15: one vocabulary).
    "in contract",
})
COMING_SOON_PHRASES = frozenset({"coming soon", "coming soon listing", "comingsoon"})
OUT_OF_SCOPE_PHRASES = frozenset({"for lease", "for rent", "lease", "leased",
                                  "for lease only"})
INACTIVE_PHRASES = frozenset({
    "not available", "no longer available", "withdrawn", "withdraw", "expired", "inactive",
    "on hold", "hold", "paused", "temporarily unavailable",
})
ACTIVE_PHRASES = frozenset({
    "for sale", "active", "available", "current", "new listing", "new",
    "just listed", "reduced", "price reduced", "now available", "listing",
    "business for sale", "businesses for sale", "no banner",
})
# WordPress taxonomy / acf values only — never matched against display text
# ("past" in a sentence is not a status).
TAXONOMY_ONLY = MappingProxyType({"past": "sold"})

# class -> (status, keep_label)
_CLASSES = (
    (SOLD_PHRASES, "sold", True),
    (UNDER_CONTRACT_PHRASES, "active", True),
    (COMING_SOON_PHRASES, DROP, True),
    (OUT_OF_SCOPE_PHRASES, DROP, True),
    (INACTIVE_PHRASES, "inactive", True),
    (ACTIVE_PHRASES, "active", False),
)
_KEEP_LABEL = {"sold": True, "inactive": True, DROP: True, "active": False}

# Per-domain vocabulary that differs from the table (normalised phrase -> status).
DOMAIN_OVERRIDES = MappingProxyType({
    "reputedbrokerage.com": MappingProxyType({"expired": "sold"}),
})

MAX_LABEL_LEN = 60
_SOLD_PREFIX = re.compile(r"^sold (?:for|at)\b")
_TAX_PREFIX = re.compile(
    r"^(?:(?:listing[_-]?)?(?:status|availability|category)|property[_-]status|"
    r"listing[_-]type|portfolio[_-]type)[-_](.+)$", re.I)
_SLUG_SOLD = re.compile(r"^sold[-_]|[-_]sold(?:[-_]\d+)?$")


def normalize_status_text(text) -> str:
    """'SOLD!' -> 'sold'; '(Off-Market)' -> 'off market'; 'Under_Contract:' -> 'under contract'."""
    if text is None or isinstance(text, bool):
        return ""
    t = str(text).replace("\xa0", " ").lower()
    t = re.sub(r"[!:*()\[\]{}\"'`.,;]+", " ", t)
    t = re.sub(r"[\s\-_/|]+", " ", t)
    return t.strip()


def _label(text) -> Optional[str]:
    t = re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()
    return t or None


def _domain_key(domain) -> str:
    d = (domain or "").strip().lower()
    d = re.sub(r"^https?://", "", d).split("/")[0]
    return d[4:] if d.startswith("www.") else d


def _status_of(norm: str, domain=None) -> Optional[str]:
    if not norm:
        return None
    over = DOMAIN_OVERRIDES.get(_domain_key(domain))
    if over and norm in over:
        return over[norm]
    for phrases, status, _keep in _CLASSES:
        if norm in phrases:
            return status
    if _SOLD_PREFIX.match(norm):
        return "sold"
    return None


def lookup_status(text, domain=None) -> Optional[Tuple[str, Optional[str]]]:
    """(status, label) for a phrase in the set; None when it is not.

    status is 'sold' | 'active' | 'inactive' | DROP. label is the caller's
    text (whitespace collapsed) when the class keeps one, else None.
    """
    raw = "" if text is None or isinstance(text, bool) else str(text)
    if len(raw.strip()) > MAX_LABEL_LEN:
        return None
    norm = normalize_status_text(raw)
    status = _status_of(norm, domain)
    if status is None:
        return None
    keep = norm not in ACTIVE_PHRASES or status != "active"
    return status, (_label(raw) if keep else None)


def classify_status(text, domain=None) -> Tuple[str, Optional[str]]:
    """Like lookup_status, but an unknown or empty phrase is ('active', None)."""
    return lookup_status(text, domain) or ("active", None)


def classify_class_term(term, domain=None) -> Optional[Tuple[str, Optional[str]]]:
    """A WordPress class_list term or taxonomy/acf value:
    'status-sold' / 'property_status-for-sale' / 'listing_status-past' / 'sold'.
    None when the term carries no status from the set."""
    t = (term or "").strip() if isinstance(term, str) else ""
    if not t:
        return None
    m = _TAX_PREFIX.match(t)
    value = m.group(1) if m else t
    norm = normalize_status_text(value)
    if norm in TAXONOMY_ONLY:
        return TAXONOMY_ONLY[norm], t
    hit = lookup_status(value, domain)
    if hit is None:
        return None
    return hit[0], (t if hit[1] is not None else None)


def classify_slug(slug) -> Optional[Tuple[str, str]]:
    """'sold-hvac-company-1234' / 'hvac-company-sold' -> ('sold', slug)."""
    s = (slug or "").strip().lower() if isinstance(slug, str) else ""
    return ("sold", slug.strip()) if s and _SLUG_SOLD.search(s) else None


def is_drop(status) -> bool:
    return status == DROP


def more_final(a, b) -> str:
    """The more final of two statuses (unknown values rank as active)."""
    return a if STATUS_RANK.get(a, 0) >= STATUS_RANK.get(b, 0) else b


def apply_status(row: dict, text, domain=None):
    """Return a NEW row with the status `text` implies, or DROP.

    - unknown phrase: the row is returned (copied) unchanged, status defaulting
      to 'active' if it had none — never a guess;
    - Coming Soon / For Lease: DROP (do not emit; count and log);
    - otherwise status is set when it is at least as final as the row's
      current status (a 'sold' row is not reopened by a later 'Pending'
      label), and broker_status_text is set to the verbatim label when the
      class keeps one.
    """
    out = dict(row or {})
    out.setdefault("status", "active")
    hit = lookup_status(text, domain or out.get("broker_domain"))
    if hit is None:
        return out
    status, label = hit
    if status == DROP:
        return DROP
    if STATUS_RANK[status] >= STATUS_RANK.get(out["status"], 0):
        out["status"] = status
        if label is not None:
            out["broker_status_text"] = label
    return out


def drop_summary(labels) -> str:
    """'3 dropped (Coming Soon!: 2, For Lease: 1)' for a caller's log line."""
    counts: dict = {}
    for lab in labels:
        k = lab or "?"
        counts[k] = counts.get(k, 0) + 1
    if not counts:
        return "0 dropped"
    return f"{sum(counts.values())} dropped (" + ", ".join(
        f"{k}: {v}" for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))) + ")"


__all__ = [
    "DROP", "STATUS_RANK", "SOLD_PHRASES", "UNDER_CONTRACT_PHRASES",
    "COMING_SOON_PHRASES", "OUT_OF_SCOPE_PHRASES", "INACTIVE_PHRASES",
    "ACTIVE_PHRASES", "TAXONOMY_ONLY", "DOMAIN_OVERRIDES", "normalize_status_text",
    "lookup_status", "classify_status", "classify_class_term", "classify_slug",
    "is_drop", "more_final", "apply_status", "drop_summary",
]
