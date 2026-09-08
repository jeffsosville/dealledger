#!/bin/bash
# RETIRED 2026-09-08 — do not re-add the cron entry for this script.
#
# At --top-n 250, a single run took 24-56h (logs/v6_2026090{5,6,7}.log
# last-write times). The crontab fired it daily at 08:30 EDT regardless of
# whether the previous run had finished, so multiple v6 processes were
# writing to production listings_direct concurrently for days — no
# concurrency guard existed here, unlike .github/workflows/broker_scrape.yml
# (see its `concurrency:` block). This is very likely the mechanism behind
# "listings is being written during measurement" in CLAUDE.md, and is the
# same root cause as the sagbrokerage.com junk burst (its 12:30 UTC / 08:30
# EDT timing matches this cron exactly — it came through the normal
# brokers_clean.csv rotation, not a bypass).
#
# The actual crontab line (`30 8 * * * ... run_v6_daily.sh`) has been
# removed from `crontab -l` on this machine (2026-09-08). This guard exists
# in case that entry, or any other trigger, ever gets re-added by mistake.
#
# Coverage consequence, not yet resolved: this script covered 250
# brokers/day; data/brokers_clean.csv holds 1,705, so the surviving
# .github/workflows/broker_scrape.yml (--top-n 100, one run/day, 285-minute
# guard inside a 350-minute job cap) is an ~17-day full rotation, not a
# daily one. Reaching parity needs sharding across parallel jobs or more
# than one run/day — a 250-broker pass alone needs 24h+, which the
# workflow's time budget cannot fit in a single run. That's a separate
# decision, not fixed by raising --top-n on the existing job.
echo "run_v6_daily.sh is retired — see the comment at the top of this file. Not running." >&2
exit 1

cd /Users/jeffsosville/dealledger-repo || exit 1
set -a
source .env
set +a
mkdir -p logs
LOG="logs/v6_$(date +%Y%m%d).log"
PY=/Library/Frameworks/Python.framework/Versions/3.13/bin/python3
echo "=== v6 run started $(date) ===" >> "$LOG"
$PY -c "
from curl_cffi import requests
import os, random, sys
u=os.environ.get('PROXY_USER',''); p=os.environ.get('PROXY_PASS','')
h=os.environ.get('PROXY_HOST','gw.dataimpulse.com:823')
if not (u and p):
    print('PROXY WARNING: creds missing'); sys.exit(0)
sid=f'hc{random.randint(1000,9999)}'
proxy=f'http://{u}__cr.us;sessid.{sid}:{p}@{h}'
try:
    r=requests.get('https://api.ipify.org', impersonate='chrome131', proxies={'http':proxy,'https':proxy}, timeout=20)
    print(f'PROXY HEALTH: {r.status_code} (exit IP {r.text.strip()})')
except Exception as e:
    print(f'PROXY HEALTH: FAILED — {e}')
" >> "$LOG" 2>&1
$PY -u scrapers/dealledger_scraper_v6.py --brokers data/brokers_clean.csv --stale-first --top-n 250 >> "$LOG" 2>&1
echo "=== v6 run finished $(date) exit=$? ===" >> "$LOG"
