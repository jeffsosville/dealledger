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

## Quick Start

```bash
# Clone the repo
git clone https://github.com/jeffsosville/dealledger
cd dealledger

# Install dependencies
pip install -r requirements.txt

# Run a single broker scraper
python -m scrapers.brokers.transworld

# Run all scrapers
python -m scrapers.run_all

# Generate a snapshot
python -m ledger.snapshot
```

Or with Docker:

```bash
docker-compose up
```

---

## Project Structure

```
dealledger/
├── scrapers/
│   ├── base.py              # Base scraper class (all brokers inherit this)
│   ├── run_all.py           # Run all broker scrapers
│   └── brokers/
│       ├── __init__.py
│       ├── transworld.py    # Example: Transworld Business Advisors
│       ├── sunbelt.py       # Example: Sunbelt Business Brokers
│       └── ...              # One file per broker
├── ingestion/
│   ├── normalize.py         # Normalize raw scraper output to schema
│   ├── dedupe.py            # Detect duplicates across sources
│   ├── hash.py              # Generate content hashes for change detection
│   └── validate.py          # Validate against schema
├── ledger/
│   ├── append.py            # Append new records to ledger
│   ├── diff.py              # Generate diffs between snapshots
│   └── snapshot.py          # Export ledger to CSV/JSON
├── api/                     # Optional: REST API for querying
├── data/
│   └── snapshots/           # Published snapshots (CSV)
├── tests/                   # Tests for scrapers and ingestion
├── docs/
│   ├── METHODOLOGY.md       # How we classify and verify
│   ├── SCHEMA.md            # Data schema specification
│   ├── CONTRIBUTING.md      # How to contribute
│   └── adding-a-broker.md   # Step-by-step guide for new scrapers
├── requirements.txt
├── docker-compose.yml
└── LICENSE
```

---

## How to Contribute

We need help adding brokers. There are hundreds of business brokers with websites — each one is a potential data source.

**To add a broker:**

1. Check the [wanted brokers list](docs/WANTED_BROKERS.md)
2. Read the [adding a broker guide](docs/adding-a-broker.md)
3. Copy the template: `scrapers/brokers/_template.py`
4. Write your scraper
5. Add tests
6. Submit a PR

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
- **Data**: CC-BY 4.0

You may use, fork, and build upon this work. Attribution required for data.

---

## Links

- Website: [dealledger.org](https://dealledger.org)
- Data: [data.dealledger.org](https://data.dealledger.org)
- Docs: [docs.dealledger.org](https://docs.dealledger.org)

---

*"We don't take sides. We publish what we observe."*
