#!/usr/bin/env python3
"""
DealLedger Snapshot Orchestrator
Run after scraping to normalize, dedupe, hash, and publish.

Usage:
    python3 ingestion/run_snapshot.py
    python3 ingestion/run_snapshot.py --scrape --top-n 200
"""
import argparse, csv, hashlib, json, os, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SNAPS = DATA / "snapshots"

def today(): return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""): h.update(chunk)
    return h.hexdigest()

def find_listings(d):
    files = sorted(Path(d).glob("listings*.json"), key=lambda f: f.stat().st_size, reverse=True)
    return files[0] if files else None

JUNK = {"businesses for sale","accessibility statement","free sign up","opt-out preferences",
    "quick links","privacy policy","terms of service","contact us","cookie policy",
    "site map","login","sign in","register","subscribe","newsletter","about us","home","search","none","untitled"}

def normalize(raw_path):
    with open(raw_path) as f: data = json.load(f)
    if isinstance(data, dict) and "listings" in data:
        listings, meta = data["listings"], data.get("metadata", {})
    elif isinstance(data, list):
        listings, meta = data, {}
    else:
        return [], {}
    out = []
    for l in listings:
        title = (l.get("title") or "").strip()
        if title.lower() in JUNK or len(title) < 4: continue
        out.append({
            "id": l.get("id") or hashlib.md5((l.get("source_url","")+title).encode()).hexdigest(),
            "source_url": l.get("source_url", ""),
            "broker_name": l.get("broker_name"),
            "broker_url": l.get("broker_url"),
            "title": title,
            "asking_price": l.get("asking_price") or l.get("price"),
            "revenue": l.get("revenue"),
            "cash_flow": l.get("cash_flow"),
            "city": l.get("city"), "state": l.get("state"),
            "country": l.get("country", "US"),
            "business_type": l.get("business_type"),
            "vertical": l.get("vertical"),
            "first_seen": l.get("first_seen") or l.get("scraped_at"),
            "status": l.get("status", "active"),
        })
    return out, meta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scrape", action="store_true")
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--snapshot-dir")
    args = parser.parse_args()

    print("=" * 60)
    print("DEALLEDGER SNAPSHOT ORCHESTRATOR")
    print("=" * 60)

    if args.scrape:
        print("\n[1/5] Running scraper...")
        subprocess.run([sys.executable, str(ROOT/"scrapers/dealledger_scraper_v4.py"),
            "--brokers", str(DATA/"brokers.csv"), "--top-n", str(args.top_n)], cwd=str(ROOT))

    snap_dir = args.snapshot_dir or str(SNAPS / today())
    print(f"\n[2/5] Looking for data in {snap_dir}")
    raw = find_listings(snap_dir)
    if not raw:
        print("  No listings found!"); sys.exit(1)
    print(f"  Found: {raw.name} ({raw.stat().st_size:,} bytes)")

    print("\n[3/5] Normalizing...")
    listings, meta = normalize(raw)
    print(f"  {len(listings)} listings after cleanup")

    print(f"\n[4/5] Writing snapshot...")
    os.makedirs(snap_dir, exist_ok=True)
    lpath = os.path.join(snap_dir, "listings.json")
    json.dump(listings, open(lpath, "w"), indent=2, default=str)

    wp = sum(1 for l in listings if l.get("asking_price"))
    wc = sum(1 for l in listings if l.get("cash_flow"))
    states = len(set(l.get("state") for l in listings if l.get("state")))
    brokers = len(set(l.get("broker_name") for l in listings if l.get("broker_name")))

    summary = {"snapshot_date": os.path.basename(snap_dir), "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0.0", "total_listings": len(listings), "with_price": wp,
        "with_cash_flow": wc, "states": states, "brokers": brokers}
    spath = os.path.join(snap_dir, "summary.json")
    json.dump(summary, open(spath, "w"), indent=2)

    with open(os.path.join(snap_dir, "sha256.txt"), "w") as f:
        for name in ["listings.json", "summary.json"]:
            p = os.path.join(snap_dir, name)
            if os.path.exists(p): f.write(f"{sha256(p)}  {name}\n")

    print(f"  {len(listings)} listings | {wp} with price | {states} states | {brokers} brokers")

    print("\n[5/5] Updating latest + manifest...")
    json.dump(listings, open(DATA/"latest.json", "w"), indent=2, default=str)
    fields = ["id","source_url","broker_name","title","asking_price","revenue","cash_flow","city","state","vertical","business_type","first_seen","status"]
    with open(DATA/"latest.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(listings)

    mpath = SNAPS / "manifest.json"
    manifest = json.load(open(mpath)) if mpath.exists() else []
    manifest = [m for m in manifest if m.get("snapshot_date") != summary["snapshot_date"]]
    manifest.append({"snapshot_date": summary["snapshot_date"], "path": f"data/snapshots/{summary['snapshot_date']}/",
        "record_count": len(listings), "generated_at": summary["generated_at"]})
    manifest.sort(key=lambda m: m["snapshot_date"])
    json.dump(manifest, open(mpath, "w"), indent=2)

    print(f"\n{'=' * 60}")
    print(f"SNAPSHOT COMPLETE: {len(listings)} listings")
    print(f"{'=' * 60}")

if __name__ == "__main__": main()
