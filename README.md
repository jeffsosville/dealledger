# DealLedger

An open, verifiable record of businesses for sale.

---

## The Problem

There is no MLS for business sales. No standards. No audit trail. No accountability.

Brokers can list fake businesses. Marketplaces can inflate their counts. Listings can disappear and reappear with no record. Prices change with no history. No one knows what's real.

This is the [Market for Lemons](https://en.wikipedia.org/wiki/The_Market_for_Lemons). When buyers can't distinguish honest sellers from dishonest ones, trust collapses.

DealLedger is a public deed registry for private market activity — a memory layer for markets that are designed to forget.

---

## What This Is

Every listing is:
- **Source-linked** — traceable to its original broker website
- **Timestamped** — first seen, last seen, every change recorded
- **Diffable** — additions, removals, price changes, relists visible
- **Verifiable** — methodology is public, data is reproducible

If a listing can't be traced to a verifiable source, it doesn't exist in the ledger.

---

## How the data is collected

Broker-direct only. DealLedger does not ingest marketplace aggregators; the
BizBuySell feed was retired in September 2026 and its rows are kept as
inactive history.

1. **Registry.** Every known broker website is a row in `broker_sources`
   (Supabase), moving through `1_needs_discovery → 3_crawlable → 4_producing`.
2. **Discovery** (`agents/discover_backlog.py`, daily) finds each broker's
   listings page and marks the verified ones crawlable.
3. **Nightly crawl** (`scrapers/dealledger_scraper_v6.py`, 4 parallel shards,
   400 brokers/day) reads crawlable and producing brokers, stalest first.
4. **Specialized scrapers** (`scrapers/run_specialized.py`) cover the large
   franchise networks and WordPress-REST sites daily.
5. Listings land in `listings_direct` with first-seen/last-seen dates, are
   filtered for junk, and are bridged to the public site. A daily snapshot is
   committed to `data/snapshots/<date>/`.

Operational detail — schedules, stages, monitors — is in [CLAUDE.md](CLAUDE.md).

---

## Quick Start

```bash
git clone https://github.com/jeffsosville/dealledger
cd dealledger
pip install -r requirements.txt

# Test the generic crawler on ONE broker (never writes to production
# unless you pass --write)
python3 scrapers/dealledger_scraper_v6.py --broker https://example-broker.com/listings

# Run the specialized scrapers without writing
python3 scrapers/run_specialized.py --dry-run --brokers transworld
```

---

## Project Structure

```
dealledger/
├── scrapers/
│   ├── dealledger_scraper_v6.py   # generic broker crawler (nightly, sharded)
│   ├── run_specialized.py         # franchise + WordPress-REST scrapers
│   ├── crawl_run_log.py           # per-broker crawl_run logging
│   ├── first_seen_monitor.py      # first_seen invariant check
│   ├── freshness_monitor.py       # staleness alerts
│   └── brokers/                   # per-broker scraper template
├── agents/
│   ├── discover_backlog.py        # daily broker discovery
│   └── discovery_v2.py            # listings-page finder
├── scripts/
│   ├── export_brokers_csv.py      # broker list from broker_sources
│   └── merge_snapshot_shards.py   # merge nightly shard snapshots
├── sql/                           # applied database migrations
├── data/
│   ├── snapshots/                 # daily snapshots (CC0)
│   └── wp_rest_brokers.csv        # WordPress-REST broker registry
├── pages/                         # dealledger.org (Next.js)
├── .github/workflows/             # scheduled jobs
└── docs/
```

---

## How to Contribute

We need help adding brokers. There are hundreds of business brokers with websites — each one is a potential data source.

**To add a broker:**

1. Check the [wanted brokers list](docs/WANTED_BROKERS.md)
2. Most brokers need no code: send us the website and discovery will find
   its listings page (see [Adding a Broker](docs/adding-a-broker.md))
3. For a site the generic crawler can't read, write a specialized scraper
   from `scrapers/brokers/_template.py` and submit a PR

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for full details.

---

## Documentation

- [METHODOLOGY.md](docs/METHODOLOGY.md) — How we classify and verify listings
- [SCHEMA.md](docs/SCHEMA.md) — Data schema specification
- [CONTRIBUTING.md](docs/CONTRIBUTING.md) — How to contribute
- [Adding a Broker](docs/adding-a-broker.md) — Step-by-step scraper guide

---

## Data Access

**Snapshots (CSV)**
```bash
curl -O https://data.dealledger.org/snapshots/latest.csv
```

**API** (coming soon)
```bash
curl https://api.dealledger.org/listings?vertical=cleaning&state=TX
```

---

## License

- **Code**: MIT License
- **Data**: CC0 1.0 (public domain) — see [DATA_LICENSE.md](DATA_LICENSE.md)

You may use, fork, and build upon this work. No attribution required for data.

---

## Links

- Website: [dealledger.org](https://dealledger.org)
- Data: [data.dealledger.org](https://data.dealledger.org)
- Docs: [docs.dealledger.org](https://docs.dealledger.org)

---

*"We don't take sides. We publish what we observe."*
