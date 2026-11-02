"""Submit dealledger.org URLs to IndexNow (Bing, Yandex, Seznam, Naver...).

Bing's index feeds ChatGPT search and Copilot, so pushing new and changed
pages here gets them discoverable within hours instead of waiting for a crawl.

Reads the live sitemap index, collects URLs from the chosen child sitemaps,
and POSTs them in batches of 10,000 (the protocol limit).

    python3 scripts/indexnow.py                 # browse pages only (default)
    python3 scripts/indexnow.py --all           # every sitemap
    python3 scripts/indexnow.py --dry-run       # list counts, submit nothing

The key file public/<KEY>.txt must be live on the site before submitting.
"""
import argparse
import re
import sys
import urllib.request
import json

HOST = "dealledger.org"
KEY = "a544450d32e427f18160d68c7f5f2c7c"
KEY_LOCATION = f"https://{HOST}/{KEY}.txt"
ENDPOINT = "https://api.indexnow.org/indexnow"
BATCH = 10_000


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "DealLedger-IndexNow/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8")


def locs(xml):
    return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="submit every child sitemap, not just browse pages")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    children = locs(get(f"https://{HOST}/sitemap.xml"))
    if not args.all:
        children = [c for c in children if "sitemap-browse" in c or "sitemap-static" in c]

    urls = []
    for child in children:
        found = [u for u in locs(get(child)) if u.startswith(f"https://{HOST}/")]
        print(f"[+] {child}: {len(found)} urls")
        urls.extend(found)
    urls = list(dict.fromkeys(urls))
    print(f"[+] {len(urls)} unique urls")

    if not urls:
        sys.exit("no urls found; refusing to submit an empty list")

    if get(KEY_LOCATION).strip() != KEY:
        sys.exit(f"key file {KEY_LOCATION} is not live yet; deploy first")

    if args.dry_run:
        return

    for i in range(0, len(urls), BATCH):
        body = json.dumps({"host": HOST, "key": KEY, "keyLocation": KEY_LOCATION,
                           "urlList": urls[i:i + BATCH]}).encode()
        req = urllib.request.Request(ENDPOINT, data=body, method="POST",
                                     headers={"Content-Type": "application/json; charset=utf-8"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                print(f"[+] batch {i // BATCH + 1}: HTTP {r.status}")
        except urllib.error.HTTPError as e:
            # 200/202 = accepted; 422 = urls don't match host/key; 429 = too many
            sys.exit(f"batch {i // BATCH + 1}: HTTP {e.code} {e.read().decode()[:200]}")


if __name__ == "__main__":
    main()
