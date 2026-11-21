#!/bin/bash
cd "$(dirname "$0")" || exit 1
set -a
source "$HOME/.dealledger.env"
set +a
mkdir -p logs
LOG="logs/specialized_$(date +%Y%m%d).log"
PY=/Library/Frameworks/Python.framework/Versions/3.13/bin/python3
echo "=== specialized run started $(date) ===" >> "$LOG"
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
# Mac runs only the brokers that fail from GitHub (data/mac_only_brokers.txt). FULL=1 runs everything.
MAC_ONLY=$(grep -v '^\s*#' data/mac_only_brokers.txt | awk '{print $1}' | grep -v '^$' | paste -sd, -)
if [ "${FULL:-0}" = "1" ] || [ -z "$MAC_ONLY" ]; then
  $PY -u scrapers/run_specialized.py >> "$LOG" 2>&1
else
  echo "Mac-only brokers: $MAC_ONLY" >> "$LOG"
  $PY -u scrapers/run_specialized.py --brokers "$MAC_ONLY" >> "$LOG" 2>&1
fi
echo "=== specialized run finished $(date) exit=$? ===" >> "$LOG"
