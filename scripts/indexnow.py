"""Submit dealledger.org URLs to IndexNow (Bing, Yandex, Seznam, Naver...).

Bing's index feeds ChatGPT search and Copilot, so pushing new and changed
pages here gets them discoverable within hours instead of waiting for a crawl.

IndexNow is for pages that changed, not the whole site: Bing flags repeated
full-site submissions ("Avoid IndexNow Batch Mode"). The sitemaps already
tell crawlers about everything else.

    python3 scripts/indexnow.py --changed            # daily: only what changed (CI default)
    python3 scripts/indexnow.py --changed --browse   # same, plus browse/static pages (weekly)
    python3 scripts/indexnow.py --changed --dry-run  # list what would go, submit nothing
    python3 scripts/indexnow.py --all                # every URL in every sitemap (one-off only)

--changed compares the sitemap files in public/ against the same files at
--base (default: $GITHUB_SHA, i.e. the commit the nightly sitemap job checked
out before regenerating). It submits:
  - listings that are new, or whose <lastmod> moved (lastmod = when the
    listing itself changed: price, status, fields)
  - listings and broker pages that dropped out (so Bing recrawls and drops them)
  - broker pages that are new
Broker <lastmod> is "last seen" and moves every day, so it is ignored.

The key file public/<KEY>.txt must be live on the site before submitting.
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

HOST = "dealledger.org"
KEY = "a544450d32e427f18160d68c7f5f2c7c"
KEY_LOCATION = f"https://{HOST}/{KEY}.txt"
ENDPOINT = "https://api.indexnow.org/indexnow"
BATCH = 10_000
PUBLIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "public")

URL_RE = re.compile(r"<url>(.*?)</url>", re.S)
LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
MOD_RE = re.compile(r"<lastmod>\s*([^<\s]+)\s*</lastmod>")


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "DealLedger-IndexNow/1.1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8")


def locs(xml):
    return LOC_RE.findall(xml)


def entries(xml):
    """{loc: lastmod} for a <urlset>."""
    out = {}
    for block in URL_RE.findall(xml or ""):
        loc = LOC_RE.search(block)
        if loc:
            mod = MOD_RE.search(block)
            out[loc.group(1)] = mod.group(1) if mod else ""
    return out


def git_show(base, relpath):
    try:
        return subprocess.run(["git", "show", f"{base}:{relpath}"], capture_output=True,
                              text=True, check=True).stdout
    except subprocess.CalledProcessError:
        return ""


def git_ls(base, pattern):
    try:
        out = subprocess.run(["git", "ls-tree", "--name-only", base, "public/"],
                             capture_output=True, text=True, check=True).stdout.split()
    except subprocess.CalledProcessError:
        return []
    return [p for p in out if re.fullmatch(pattern, p)]


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def changed_urls(base):
    """Diff public/sitemap-*.xml (working tree) against the same files at `base`."""
    # Listings may be split across sitemap-listings-N.xml; pool them.
    old_l, new_l = {}, {}
    for p in git_ls(base, r"public/sitemap-listings-\d+\.xml"):
        old_l.update(entries(git_show(base, p)))
    for p in glob.glob(os.path.join(PUBLIC, "sitemap-listings-*.xml")):
        new_l.update(entries(read(p)))

    if not old_l:
        sys.exit(f"no listing sitemaps found at {base}; refusing to treat every listing as new")

    new_or_moved = [u for u, m in new_l.items() if old_l.get(u) != m]
    gone = [u for u in old_l if u not in new_l]

    # Brokers: only arrivals and departures (lastmod is "last seen", always today).
    old_b = set(entries(git_show(base, "public/sitemap-brokers.xml")))
    bp = os.path.join(PUBLIC, "sitemap-brokers.xml")
    new_b = set(entries(read(bp))) if os.path.exists(bp) else set()
    b_new = sorted(new_b - old_b) if old_b else []
    b_gone = sorted(old_b - new_b)

    print(f"[+] listings: {len(new_or_moved)} new/changed, {len(gone)} gone "
          f"(of {len(new_l)} now, {len(old_l)} at {base[:7]})")
    print(f"[+] brokers:  {len(b_new)} new, {len(b_gone)} gone")
    if len(new_or_moved) > 0.5 * max(len(new_l), 1):
        # A wholesale lastmod rewrite would turn this back into batch mode.
        sys.exit(f"{len(new_or_moved)} of {len(new_l)} listings 'changed' - looks like a "
                 "regeneration artifact, not real changes; refusing to submit")
    return new_or_moved + gone + b_new + b_gone


def sitemap_urls(include_all):
    children = locs(get(f"https://{HOST}/sitemap.xml"))
    if not include_all:
        children = [c for c in children if "sitemap-browse" in c or "sitemap-static" in c]
    urls = []
    for child in children:
        found = [u for u in locs(get(child)) if u.startswith(f"https://{HOST}/")]
        print(f"[+] {child}: {len(found)} urls")
        urls.extend(found)
    return urls


def submit(urls):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--changed", action="store_true", help="only URLs that changed since --base")
    ap.add_argument("--base", default=os.environ.get("GITHUB_SHA", "HEAD~1"),
                    help="git commit to diff sitemaps against (default $GITHUB_SHA, else HEAD~1)")
    ap.add_argument("--browse", action="store_true", help="with --changed: also send browse/static pages")
    ap.add_argument("--all", action="store_true", help="every URL in every sitemap (one-off only)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.changed:
        urls = changed_urls(args.base)
        if args.browse:
            urls += sitemap_urls(include_all=False)
    else:
        urls = sitemap_urls(include_all=args.all)

    urls = [u for u in dict.fromkeys(urls) if u.startswith(f"https://{HOST}/")]
    print(f"[+] {len(urls)} unique urls")

    if not urls:
        print("[=] nothing changed; nothing to submit")
        return

    if args.dry_run:
        for u in urls[:20]:
            print("    ", u)
        return

    if get(KEY_LOCATION).strip() != KEY:
        sys.exit(f"key file {KEY_LOCATION} is not live yet; deploy first")

    submit(urls)


if __name__ == "__main__":
    main()
