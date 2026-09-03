#!/usr/bin/env python3
"""One-off inspector for NO_PATTERN brokers — fetch with the same fetcher the
scraper uses, then print candidate repeated-container selectors so a human
(or Claude) can pick the real listing card selector by eye."""
import sys, json
from collections import Counter
from bs4 import BeautifulSoup

sys.path.insert(0, "scrapers")
from dealledger_scraper_v6 import PageFetcher, is_listing_element, best_card_title, has_detail_link

def inspect(url):
    fetcher = PageFetcher()
    try:
        html, method = fetcher.fetch(url, use_proxy=False)
    except Exception as e:
        print(f"FETCH FAILED (no proxy): {e}")
        try:
            html, method = fetcher.fetch(url, use_proxy=True)
        except Exception as e2:
            print(f"FETCH FAILED (proxy): {e2}")
            fetcher.close()
            return None, None
    fetcher.close()
    print(f"fetched via {method}, {len(html):,} bytes")
    soup = BeautifulSoup(html, "html.parser")

    # Count class-signature repeats among div/li/article at depth-agnostic level
    sig_counter = Counter()
    sig_examples = {}
    for tag in soup.find_all(["div", "li", "article", "a", "section"]):
        classes = tag.get("class")
        if not classes:
            continue
        sig = tag.name + "." + ".".join(classes)
        sig_counter[sig] += 1
        if sig not in sig_examples:
            sig_examples[sig] = tag

    print("\nTop repeated class signatures (count >= 3):")
    for sig, cnt in sig_counter.most_common(25):
        if cnt < 3:
            continue
        el = sig_examples[sig]
        text = el.get_text(" ", strip=True)[:80]
        is_l = is_listing_element(el, url)
        print(f"  {cnt:4d}  {sig:60.60s}  listing={is_l}  text={text!r}")

    return html, soup

if __name__ == "__main__":
    inspect(sys.argv[1])
