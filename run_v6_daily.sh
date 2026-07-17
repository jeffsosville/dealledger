#!/bin/bash
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
$PY scrapers/dealledger_scraper_v6.py --brokers data/brokers_clean.csv --stale-first --top-n 250 >> "$LOG" 2>&1
echo "=== v6 run finished $(date) exit=$? ===" >> "$LOG"
