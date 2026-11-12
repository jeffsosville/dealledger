"""
price_disclosure.py — one rule for "the broker explicitly withholds the price".

Owner decision 2026-11-11: a listing whose broker says "Market Price", "Call for
price", "$40k Down" (and no total) ... IS published, shown as "Price not
disclosed". A listing that simply has no price we could find is NOT: a plain
null price is still a junk signal. So the flag needs positive evidence — the
broker's own words — and those words are stored verbatim (broker_price_text).

Both writers (dealledger_scraper_v6.SupabaseWriter.upsert and
run_specialized.upsert_listings) call disclosure_fields() on every row, so the
two paths write the same two columns the same way (principle 15). Scrapers put
the broker's raw price string on the row under `broker_price_text` (preferred)
or `price_text`; nothing here reads `description` (principle 5).

Three outcomes per row, and the writer sends the columns only in the first two:
  priced           -> price_not_disclosed=False, broker_price_text=None
                      (positive evidence the price IS disclosed: clears a stale flag)
  explicit phrase  -> price_not_disclosed=True,  broker_price_text=<verbatim>
  neither          -> nothing sent; a new row gets the column default (false), an
                      existing row keeps what it has (principle 14: no evidence,
                      no overwrite)

Column-not-there-yet: the migration (claude/staging/2026-11-11/
w14-price_not_disclosed.sql) may land after this code. PostgREST rejects an
unknown column with PGRST204; the writers detect it with
is_missing_disclosure_column(), log once, and retry without the two fields.
"""
from __future__ import annotations

import re
from typing import Optional

# The writer plumbing (uniform batches, column-missing retry) is shared with
# every optional column group; group_by_keys stays importable from here.
from optional_columns import OptionalColumns, group_by_keys  # noqa: F401

DISCLOSURE_KEYS = ("price_not_disclosed", "broker_price_text")
MAX_TEXT = 120

# The broker's own words for "we won't print the price". Matched only against a
# short price-field string (<= MAX_TEXT chars) that contains no total amount.
# Deliberately NOT in the list: "N/A", "-", "0", blank — those are an empty
# template field, not a statement by the broker.
_WITHHELD_ALT = r"""(?:
        market\s+price
      | market\s+to\s+determine
      | (?:price\s+)?(?:not|non)[\s-]?disclosed
      | undisclosed
      | unlisted
      | confidential
      | (?:please\s+)?call\s+(?:for|about)\s+(?:price|pricing|details|info)
      | call\s+(?:the\s+)?(?:broker|agent|office|us)
      | price\s+on\s+(?:call|request|application)
      | (?:contact|ask|email|inquire)\s+(?:us\s+|the\s+|our\s+)?(?:broker\s+|agent\s+|office\s+)?(?:for|about)\s+(?:price|pricing|details)
      | (?:available\s+)?(?:up)?on\s+request
      | inquire
      | negotiable
      | to\s+be\s+determined
      | tbd
      | make\s+(?:an\s+)?offer
      | best\s+offer
      | submit\s+(?:an\s+)?offer
      | poa
    )"""
_WITHHELD = re.compile(r"\b" + _WITHHELD_ALT + r"\b", re.I | re.X)
# The same phrase at the START of a labelled value ("$TBD" included).
_WITHHELD_LEAD = re.compile(r"\$?\s*(?:price\s+)?" + _WITHHELD_ALT + r"\b", re.I | re.X)

_MONEY = re.compile(r"\$\s?\d[\d,]*(?:\.\d+)?\s*(?:[kKmM]{1,2}\b|million\b|thousand\b)?|\b\d{1,3}(?:,\d{3})+\b")
# "$40k Down", "$150,000 down payment" — only a down payment, no total price.
_DOWN = re.compile(r"\$\s?\d[\d,]*(?:\.\d+)?\s*(?:[kKmM]{1,2}|million|thousand)?\s+down\b", re.I)

# Labelled price on free card text: "Price: Market Price", "Asking Price - Call for price".
_LABEL = re.compile(
    r"\b(?:asking(?:\s+price)?|list(?:ing)?\s+price|sale\s+price|price)\s*[:\-–]\s*(\S[^|\n\r•·]{1,60})",
    re.I,
)


def _clean(text) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def withheld_price_phrase(text) -> Optional[str]:
    """
    If `text` (a price field as the broker printed it) explicitly withholds the
    price, return it verbatim (whitespace collapsed, <= MAX_TEXT chars); else None.

    A string carrying a real total ("$450,000 negotiable", "$1.2M / call for
    terms") is a price, not a withheld one -> None. A lone down payment
    ("$40k Down") with no other amount is withheld -> returned.
    """
    t = _clean(text)
    if not t or len(t) > MAX_TEXT:
        return None
    amounts = _MONEY.findall(t)
    downs = _DOWN.findall(t)
    if downs and len(amounts) == len(downs):
        return t
    if amounts:
        return None
    if _WITHHELD.search(t):
        return t
    return None


def find_labelled_withheld_price(card_text) -> Optional[str]:
    """
    Search free card text for a LABELLED withheld price ("Price: Market Price",
    "Asking Price: $50k Down"). Only the start of the labelled value is judged,
    never the surrounding description, so "vending machines priced on request"
    in prose cannot set the flag, and "Asking price: Confidential Type 47 ABC
    license ..." stores "Asking price: Confidential", not the rest of the card.
    Returns the verbatim label + phrase, or None.
    """
    for m in _LABEL.finditer(_clean(card_text)):
        value = m.group(1)
        hit = _DOWN.match(value) or _WITHHELD_LEAD.match(value)
        if hit:
            label = m.group(0)[: m.start(1) - m.start(0)]
            return _clean(label + hit.group(0))[:MAX_TEXT]
    return None


def _positive(v) -> bool:
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False


def disclosure_fields(row: dict) -> Optional[dict]:
    """
    The two columns to send for this scraped row, or None to send neither.

    `row` is a scraper dict (V6: asking_price; specialized format_listing:
    price). A scraper's own `price_not_disclosed=True` WITHOUT a phrase is not
    enough on its own (W5's ERE scraper sets it for any missing price) — the
    phrase is the evidence, and the owner wants it stored verbatim.
    """
    if _positive(row.get("asking_price")) or _positive(row.get("price")):
        return {"price_not_disclosed": False, "broker_price_text": None}
    for key in ("broker_price_text", "price_text"):
        phrase = withheld_price_phrase(row.get(key))
        if phrase:
            return {"price_not_disclosed": True, "broker_price_text": phrase[:MAX_TEXT]}
    return None


def flagged_without_phrase(row: dict) -> bool:
    """A scraper set price_not_disclosed=True but carried no phrase we accept.
    Writers count these and log them; nothing is written for them."""
    return bool(row.get("price_not_disclosed")) and disclosure_fields(row) is None


def is_missing_disclosure_column(err) -> bool:
    """True when a PostgREST error says price_not_disclosed / broker_price_text
    is not a column of listings_direct (migration not applied yet).
    PostgREST: 400 {"code":"PGRST204","message":"Could not find the
    'broker_price_text' column of 'listings_direct' in the schema cache"}.
    Postgres (direct): 42703 column "price_not_disclosed" ... does not exist.
    Same test as every optional column group (optional_columns.OptionalColumns)."""
    return OptionalColumns(DISCLOSURE_KEYS).matches(err)


def strip_disclosure(rows: list[dict]) -> int:
    """Remove the two keys in place; returns how many rows carried them."""
    return OptionalColumns(DISCLOSURE_KEYS).strip(rows)


class DisclosureColumns(OptionalColumns):
    """Per-process memory of 'the columns are not there yet', so the writer
    retries without them once, logs once, and stops sending them — but still
    counts what it could not write (principle 17: never a silent drop).
    Since j3 (2026-11-12) this is one instance of the shared optional-column
    mechanism (optional_columns.py); broker_status_text uses the same one."""

    def __init__(self):
        super().__init__(DISCLOSURE_KEYS, "w14 migration")
        self.flag_no_phrase = 0  # scraper flagged it, but no phrase to store
