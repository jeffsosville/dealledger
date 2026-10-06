#!/usr/bin/env python3
"""check_credentials.py — fail fast, and loudly, when a key or password is dead.

Step 1 of every nightly job. On Oct 6 2026 we found the proxy password and the
Anthropic key had both been dead for days and nothing said so: proxied crawls
quietly came back short. This turns that into a red X and an email.

    python3 scripts/check_credentials.py --need supabase,proxy
    python3 scripts/check_credentials.py --need supabase,proxy,anthropic

Reads the same env vars as the scrapers: SUPABASE_URL, SUPABASE_SERVICE_KEY,
PROXY_USER/PROXY_PASS/PROXY_HOST or PROXY_URL, ANTHROPIC_API_KEY.
Prints nothing secret.
"""
import argparse, os, sys
from urllib.parse import urlparse, unquote
import requests

def _proxy_url():
    user, pw = os.environ.get("PROXY_USER", ""), os.environ.get("PROXY_PASS", "")
    host = os.environ.get("PROXY_HOST", "gw.dataimpulse.com:823")
    if not (user and pw):
        raw = os.environ.get("PROXY_URL", "")
        if raw:
            u = urlparse(raw)
            user, pw = unquote(u.username or ""), unquote(u.password or "")
            host = f"{u.hostname}:{u.port}" if u.port else (u.hostname or host)
    user = user.split("__")[0]
    return f"http://{user}:{pw}@{host}" if user and pw else None

def check_supabase():
    url, key = os.environ.get("SUPABASE_URL", "").rstrip("/"), os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not (url and key):
        return "SUPABASE_URL / SUPABASE_SERVICE_KEY not set"
    r = requests.get(f"{url}/rest/v1/listings_direct?select=id&limit=1",
                     headers={"apikey": key, "Authorization": f"Bearer {key}"}, timeout=20)
    return None if r.status_code == 200 else f"Supabase HTTP {r.status_code}: {r.text[:120]}"

def check_proxy():
    p = _proxy_url()
    if not p:
        return "no proxy credentials (PROXY_USER/PROXY_PASS or PROXY_URL)"
    try:
        r = requests.get("https://api.ipify.org", proxies={"http": p, "https": p}, timeout=25)
    except requests.exceptions.ProxyError as e:
        return "proxy refused the login (407 / NO_USER) — password changed? " + type(e).__name__
    except Exception as e:
        return f"proxy error: {type(e).__name__}"
    return None if r.status_code == 200 else f"proxy HTTP {r.status_code}"

def check_anthropic():
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return "ANTHROPIC_API_KEY not set"
    r = requests.get("https://api.anthropic.com/v1/models",
                     headers={"x-api-key": key, "anthropic-version": "2023-06-01"}, timeout=20)
    return None if r.status_code == 200 else f"Anthropic HTTP {r.status_code} (key invalid or expired?)"

CHECKS = {"supabase": check_supabase, "proxy": check_proxy, "anthropic": check_anthropic}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--need", default="supabase,proxy")
    names = [n.strip() for n in ap.parse_args().need.split(",") if n.strip()]
    bad = 0
    for n in names:
        err = CHECKS[n]()
        if err:
            bad += 1
            print(f"::error title=Credential check failed: {n}::{err}")
            print(f"  FAIL  {n}: {err}")
        else:
            print(f"  ok    {n}")
    sys.exit(1 if bad else 0)
