---
license: cc0-1.0
pretty_name: DealLedger — US businesses for sale (broker-direct)
language: [en]
tags: [small-business, business-brokerage, mergers-and-acquisitions, open-data, tabular]
size_categories: [10K<n<100K]
configs:
  - config_name: listings
    data_files: listings.csv
  - config_name: left_observation
    data_files: listings_left_observation.csv
---

# DealLedger — US businesses for sale, straight from the brokers

Every small business for sale in America, in one open list. This dataset is a dated snapshot of
[DealLedger](https://dealledger.org): listings published directly on US business brokers' own
websites, recorded daily.

- `listings.csv` / `listings.json` — every published listing in the snapshot: asking price, cash flow,
  revenue, location, business type, broker, source URL, first-observed date and listing-age evidence basis.
- `listings_left_observation.csv` — listings that left observation, with the evidence (404, redirect,
  broker label, or absence from three complete crawls). Leaving observation is not evidence of a sale.
- `meta.json` — snapshot date and coverage. `verification.json` — the checks this snapshot passed
  against the live database before release.
- `DATA_DICTIONARY.md` — every field.

Snapshot date: see `meta.json`. Current data: https://dealledger.org and
https://github.com/jeffsosville/dealledger/tree/main/data/public (updated daily).

**License:** CC0 1.0 — public domain, no attribution required. If you use it in research, please cite
the Zenodo DOI for the snapshot you used (see `CITATION.cff` in the GitHub repo).
