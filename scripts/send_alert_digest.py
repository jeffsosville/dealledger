#!/usr/bin/env python3
"""
Weekly new-listing digest for alert_subscriptions (sql/2026-11-13_alert_subscriptions.sql).

For each confirmed, not-unsubscribed alert: pull public listings matching its
state / vertical whose evidence-based listed date is after last_sent_at, email
them, then advance last_sent_at. Alerts with nothing new get no email (and
last_sent_at stays put, so nothing is skipped).

"New" uses mv_seo_listings_v2.estimated_listed_date with dom_basis in
('observed', 'window') only - never 'floor' / 'in_db', which are database-entry
dates, not listing dates.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, RESEND_API_KEY.
     --dry-run prints each email instead of sending and does not update rows.
"""
import argparse
import html
import os
import re
import sys
from datetime import datetime, timezone

import requests

SITE = "https://dealledger.org"
FROM = "DealLedger <pulse@dealledger.org>"
MAX_ROWS = 25
NEW_BASES = "(observed,window)"

SB_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SB_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
RESEND = os.environ.get("RESEND_API_KEY", "")

# Mirrors lib/seo.ts STATES / TYPES labels for subject lines and links.
STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado",
    "CT": "Connecticut", "DE": "Delaware", "DC": "Washington, DC", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky",
    "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota",
    "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


def sb(path, params=None, method="GET", body=None):
    r = requests.request(
        method, f"{SB_URL}/rest/v1/{path}", params=params, json=body, timeout=60,
        headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": "application/json"},
    )
    r.raise_for_status()
    return r.json() if r.text else None


def type_labels():
    """Vertical -> (label, slug) parsed from lib/seo.ts so the two never drift."""
    src = open(os.path.join(os.path.dirname(__file__), "..", "lib", "seo.ts")).read()
    out = {}
    for m in re.finditer(r"^\s+(\w+):\s*\{\s*slug:\s*'([^']+)',\s*label:\s*'([^']+)'", src, re.M):
        out[m.group(1)] = (m.group(3), m.group(2))
    return out


def state_slug(code):
    return "washington-dc" if code == "DC" else re.sub(r"[^a-z]+", "-", STATES[code].lower())



def scope(sub, types):
    st, v = sub["state_code"], sub["vertical"]
    what = types[v][0] if v else "Businesses"
    label = f"{what} for sale in {STATES[st]}" if st else f"{what} for sale nationwide"
    if st and v:
        path = f"/businesses-for-sale/{state_slug(st)}/{types[v][1]}"
    elif st:
        path = f"/businesses-for-sale/{state_slug(st)}"
    else:
        path = f"/businesses-for-sale/type/{types[v][1]}"
    return label, path


def money(n):
    if not n or n <= 0:
        return "—"
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${round(n / 1_000)}K"
    return f"${round(n)}"


def new_listings(sub):
    since = sub["last_sent_at"][:10]
    params = {
        "select": "listing_number,title,city,state_code,price,cash_flow,estimated_listed_date",
        "estimated_listed_date": f"gt.{since}",
        "dom_basis": f"in.{NEW_BASES}",
        "order": "estimated_listed_date.desc",
        "limit": str(MAX_ROWS + 1),
    }
    if sub["state_code"]:
        params["state_code"] = f"eq.{sub['state_code']}"
    if sub["vertical"]:
        params["vertical"] = f"eq.{sub['vertical']}"
    return sb("mv_seo_listings_v2", params)


def render(sub, label, path, rows):
    more = len(rows) > MAX_ROWS
    rows = rows[:MAX_ROWS]
    unsub = f"{SITE}/api/alerts/unsubscribe?t={sub['token']}"
    items = "".join(
        f"""<tr><td style="padding:12px 0;border-top:1px solid #d4cfc4;">
<a href="{SITE}/listing/{r['listing_number']}" style="color:#0c4a6e;">{html.escape((r['title'] or 'Business for sale')[:110])}</a><br>
<span style="font-size:13px;color:#57534e;">{html.escape(', '.join(x for x in [r.get('city'), r.get('state_code')] if x) or '—')}
 · asking {money(r.get('price'))} · cash flow {money(r.get('cash_flow'))}</span></td></tr>"""
        for r in rows
    )
    n = len(rows)
    body = f"""<!doctype html><html><body style="margin:0;background:#fafaf9;">
<div style="max-width:560px;margin:0 auto;padding:32px 24px;font-family:-apple-system,system-ui,sans-serif;font-size:15px;line-height:1.55;color:#1c1917;">
<div style="font-family:Menlo,monospace;font-size:13px;font-weight:600;margin-bottom:24px;">DealLedger</div>
<p><strong>{n}{'+' if more else ''} new</strong> for <a href="{SITE}{path}" style="color:#0c4a6e;">{html.escape(label)}</a> since your last update.</p>
<table style="width:100%;border-collapse:collapse;">{items}</table>
{f'<p><a href="{SITE}{path}" style="color:#0c4a6e;">See all on DealLedger</a></p>' if more else ''}
<p style="margin-top:32px;padding-top:16px;border-top:1px solid #d4cfc4;font-size:12px;color:#78716c;">
You asked for alerts on this page. <a href="{unsub}" style="color:#78716c;">Unsubscribe</a><br>
DealLedger · 10 Academy St, Skaneateles, NY 13152</p></div></body></html>"""
    subject = f"{n}{'+' if more else ''} new: {label}"
    return subject, body, unsub


def send(to, subject, body, unsub):
    r = requests.post(
        "https://api.resend.com/emails", timeout=30,
        headers={"Authorization": f"Bearer {RESEND}", "Content-Type": "application/json"},
        json={
            "from": FROM, "to": [to], "subject": subject, "html": body,
            "headers": {"List-Unsubscribe": f"<{unsub}>", "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"},
        },
    )
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not (SB_URL and SB_KEY):
        sys.exit("SUPABASE_URL / SUPABASE_SERVICE_KEY not set")
    dry = a.dry_run or not RESEND

    types = type_labels()
    subs = sb("alert_subscriptions", {
        "select": "id,email,state_code,vertical,token,last_sent_at",
        "confirmed_at": "not.is.null", "unsubscribed_at": "is.null",
    })
    sent = skipped = failed = 0
    for sub in subs:
        if (sub["state_code"] and sub["state_code"] not in STATES) or (sub["vertical"] and sub["vertical"] not in types):
            failed += 1
            print(f"bad scope {sub['id']}")
            continue
        rows = new_listings(sub)
        if not rows:
            skipped += 1
            continue
        label, path = scope(sub, types)
        subject, body, unsub = render(sub, label, path, rows)
        if dry:
            print(f"--- to {sub['email']}: {subject}")
            continue
        try:
            send(sub["email"], subject, body, unsub)
            sb(f"alert_subscriptions?id=eq.{sub['id']}", method="PATCH",
               body={"last_sent_at": datetime.now(timezone.utc).isoformat()})
            sent += 1
        except Exception as e:  # one bad address must not stop the run
            failed += 1
            print(f"send failed {sub['id']}: {e}")
    print(f"alerts: {len(subs)} active, {sent} sent, {skipped} nothing new, {failed} failed{' (dry run)' if dry else ''}")


if __name__ == "__main__":
    main()
