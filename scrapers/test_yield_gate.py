#!/usr/bin/env python3
"""
test_yield_gate.py — principle 8 check for title_repetition_gate().

A filter's false-reject rate is invisible in a way its false-accept rate is
not. This asserts the gate passes real brokers with genuine template
repetition BEFORE trusting it to reject anything. Ratios below are taken
directly from a live pull against listings_direct on 2026-09-08 (see the
comment above TITLE_REPETITION_THRESHOLD in dealledger_scraper_v6.py).

Usage:
    python3 scrapers/test_yield_gate.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from dealledger_scraper_v6 import title_repetition_gate, TITLE_REPETITION_THRESHOLD


def _cards(n_rows, n_titles):
    """n_rows cards spread as evenly as possible over n_titles distinct titles."""
    return [{"title": f"Title {i % n_titles}"} for i in range(n_rows)]


MUST_PASS = [
    ("execbb.com",                 2125, 395),
    ("restaurantrealty.com",        843, 136),
    ("www.calhouncompanies.com",    870, 154),
    ("corbettrestaurantgroup.com", 1111, 160),
]

MUST_REJECT = [
    ("www.aria.net",                357,  10),
    ("www.sagbrokerage.com",         96,   2),
    ("businessesforsale.nebba.com", 254,   5),
    # pavilionservices.com's own exact rows/title figure wasn't isolated (it's
    # excluded via BLOCKLIST_DOMAINS + export_discovered_ok.py's
    # KNOWN_BAD_DOMAINS before it would ever reach this gate) — 1,201 rows
    # across a small placeholder-category title set is comfortably in the
    # same shape as the three domains above, all >30 rows/title.
]


def run():
    failures = []

    for name, rows, titles in MUST_PASS:
        verdict, reason = title_repetition_gate(_cards(rows, titles))
        ratio = rows / titles
        status = "PASS" if verdict == "ok" else "FAIL"
        print(f"[{status}] must-pass   {name:<28} {rows:>5} rows / {titles:>4} titles "
              f"= {ratio:5.1f}  -> {verdict} ({reason})")
        if verdict != "ok":
            failures.append(name)

    for name, rows, titles in MUST_REJECT:
        verdict, reason = title_repetition_gate(_cards(rows, titles))
        ratio = rows / titles
        status = "PASS" if verdict == "reject" else "FAIL"
        print(f"[{status}] must-reject {name:<28} {rows:>5} rows / {titles:>4} titles "
              f"= {ratio:5.1f}  -> {verdict} ({reason})")
        if verdict != "reject":
            failures.append(name)

    print(f"\nthreshold = {TITLE_REPETITION_THRESHOLD}")
    if failures:
        print(f"FAILED: {failures}")
        return 1
    print("All cases passed.")
    return 0


if __name__ == "__main__":
    sys.exit(run())
