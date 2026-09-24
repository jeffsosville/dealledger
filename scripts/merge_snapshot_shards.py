#!/usr/bin/env python3
"""
Merge the per-shard V6 snapshots into the one daily snapshot the rest of the
repo expects (data/snapshots/<date>/{listings.json,listings.csv,failures.json,
embed_brokers.json,summary.json}).

    python3 scripts/merge_snapshot_shards.py shards/ data/snapshots

`shards/` holds one directory per downloaded artifact, each shaped like
data/snapshots (i.e. shards/snapshot-shard-0/<date>/listings.json ...).
A shard that died before _save_results() simply contributes nothing; the
merged summary records how many shards were present.
"""
import json
import os
import sys
from collections import Counter, defaultdict

import pandas as pd

SUM_KEYS = ("brokers_attempted", "brokers_success", "brokers_failed",
            "total_listings", "listings_with_price", "listings_with_cashflow",
            "listings_with_state", "detail_pages_fetched", "patterns_learned",
            "seconds_total")
COUNTER_KEYS = ("verticals", "failure_types")


def _load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def main(shards_root: str, out_root: str) -> int:
    by_date = defaultdict(list)          # date -> [shard snapshot dirs]
    for shard in sorted(os.listdir(shards_root)):
        sdir = os.path.join(shards_root, shard)
        if not os.path.isdir(sdir):
            continue
        for date in sorted(os.listdir(sdir)):
            if os.path.isdir(os.path.join(sdir, date)):
                by_date[date].append((shard, os.path.join(sdir, date)))

    if not by_date:
        print("⚠️  no shard snapshots found — nothing to merge")
        return 0

    for date, parts in sorted(by_date.items()):
        listings, failures, embeds, summaries = [], [], [], []
        for shard, d in parts:
            listings += _load(os.path.join(d, "listings.json"), [])
            failures += _load(os.path.join(d, "failures.json"), [])
            embeds   += _load(os.path.join(d, "embed_brokers.json"), [])
            s = _load(os.path.join(d, "summary.json"), None)
            if s:
                s["_shard"] = shard
                summaries.append(s)

        merged = {}
        for s in summaries:
            for k, v in s.items():
                if k in SUM_KEYS:
                    merged[k] = merged.get(k, 0) + (v or 0)
                elif k in COUNTER_KEYS:
                    merged[k] = dict(Counter(merged.get(k, {})) + Counter(v or {}))
                elif k not in merged:
                    merged[k] = v
        if summaries:
            merged["started"] = min(s.get("started", "") for s in summaries)
            merged["completed"] = max(s.get("completed", "") for s in summaries)
            merged["patterns_cached"] = max(s.get("patterns_cached", 0) for s in summaries)
            merged["interrupted"] = any(s.get("interrupted") for s in summaries)
            slow = [t for s in summaries for t in (s.get("slowest_brokers") or [])]
            merged["slowest_brokers"] = sorted(slow, reverse=True)[:15]
        merged.pop("_shard", None)
        merged["total_listings"] = len(listings)
        merged["shards"] = sorted(s["_shard"] for s in summaries)

        out = os.path.join(out_root, date)
        os.makedirs(out, exist_ok=True)
        with open(os.path.join(out, "listings.json"), "w") as f:
            json.dump(listings, f, indent=2, default=str)
        if listings:
            pd.DataFrame(listings).to_csv(os.path.join(out, "listings.csv"), index=False)
        with open(os.path.join(out, "failures.json"), "w") as f:
            json.dump(failures, f, indent=2, default=str)
        with open(os.path.join(out, "embed_brokers.json"), "w") as f:
            json.dump(embeds, f, indent=2, default=str)
        with open(os.path.join(out, "summary.json"), "w") as f:
            json.dump(merged, f, indent=2, default=str)
        print(f"📁 {out}: {len(parts)} shard(s), {len(listings)} listings, "
              f"{len(failures)} failures, {merged.get('brokers_attempted', 0)} brokers")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))
