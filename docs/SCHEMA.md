# DealLedger Schema

Version: 1.0.0

This document defines the canonical schema for DealLedger records.

---

## Core Listing Record

Every listing in the ledger has these fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique DealLedger identifier (generated) |
| `source_url` | string | yes | Original listing URL on broker site |
| `source_id` | string | no | Broker's internal listing ID (if available) |
| `broker_id` | string | yes | DealLedger broker identifier |
| `broker_name` | string | yes | Broker company name |
| `first_seen` | datetime | yes | UTC timestamp of first observation |
| `last_seen` | datetime | yes | UTC timestamp of most recent observation |
| `status` | enum | yes | Current status (see below) |

---

## Status Values

| Status | Description |
|--------|-------------|
| `active` | Listing currently visible on source |
| `removed` | Listing no longer visible (404 or delisted) |
| `sold` | Marked as sold by broker |
| `pending` | Marked as pending/under contract |
| `relisted` | Previously removed, now active again |

---

## Business Details

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `title` | string | yes | Listing title/headline |
| `description` | string | no | Full listing description |
| `asking_price` | integer | no | Asking price in USD (null if "Contact for Price") |
| `price_hidden` | boolean | yes | True if price not disclosed |
| `vertical` | string | yes | Industry vertical (see verticals list) |
| `category` | string | no | Sub-category within vertical |

---

## Location

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `city` | string | no | City name |
| `state` | string | no | State/province code (e.g., "TX") |
| `country` | string | yes | Country code (e.g., "US") |
| `zip` | string | no | Postal code |
| `region` | string | no | Broker-defined region (e.g., "Dallas-Fort Worth") |
| `location_hidden` | boolean | yes | True if specific location not disclosed |

---

## Financials

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `revenue` | integer | no | Annual gross revenue in USD |
| `cash_flow` | integer | no | Annual cash flow / SDE in USD |
| `ebitda` | integer | no | EBITDA in USD (if different from cash_flow) |
| `inventory` | integer | no | Inventory value included in sale |
| `ffe` | integer | no | Furniture, fixtures, equipment value |
| `real_estate` | boolean | no | True if real estate included |
| `real_estate_value` | integer | no | Real estate value if included |
| `year_established` | integer | no | Year business was established |
| `employees` | integer | no | Number of employees |

---

## Deal Terms

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `seller_financing` | boolean | no | Seller financing available |
| `sba_prequalified` | boolean | no | SBA loan pre-qualified |
| `franchise` | boolean | no | Is this a franchise |
| `franchise_name` | string | no | Franchise brand name |
| `home_based` | boolean | no | Home-based business |
| `relocatable` | boolean | no | Business can be relocated |
| `absentee_owner` | boolean | no | Absentee/semi-absentee ownership possible |

---

## Metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `scraped_at` | datetime | yes | UTC timestamp of this scrape |
| `content_hash` | string | yes | SHA-256 hash of normalized content |
| `confidence` | float | yes | Verification confidence score (0.0-1.0) |
| `flags` | array | no | List of warning flags (see below) |

---

## Warning Flags

| Flag | Description |
|------|-------------|
| `price_drop_50` | Price dropped >50% since first seen |
| `relist_detected` | Same/similar listing appeared after removal |
| `stale_90` | Not updated by broker in 90+ days |
| `duplicate_suspected` | Possible duplicate of another listing |
| `source_404` | Source URL returned 404 on last check |

---

## History Record

Price and status changes are tracked separately:

| Field | Type | Description |
|-------|------|-------------|
| `listing_id` | string | Reference to listing |
| `timestamp` | datetime | When change was observed |
| `field` | string | Field that changed |
| `old_value` | any | Previous value |
| `new_value` | any | New value |

---

## Broker Record

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique DealLedger broker identifier |
| `name` | string | yes | Company name |
| `website` | string | yes | Primary website URL |
| `headquarters_state` | string | no | HQ state code |
| `coverage_states` | array | no | States where broker operates |
| `verticals` | array | no | Industry verticals covered |
| `active_listings` | integer | yes | Current active listing count |
| `total_observed` | integer | yes | Total listings ever observed |
| `first_seen` | datetime | yes | First observation date |
| `last_scraped` | datetime | yes | Most recent successful scrape |

---

## Verticals

Current supported verticals:

| Code | Description |
|------|-------------|
| `cleaning` | Cleaning services (janitorial, residential, commercial) |
| `laundromat` | Laundromats and coin laundry |
| `vending` | Vending machine routes and operations |
| `hvac` | HVAC and mechanical services |
| `landscaping` | Landscaping and lawn care |
| `pool` | Pool service and maintenance |
| `pest` | Pest control |
| `plumbing` | Plumbing services |
| `electrical` | Electrical services |
| `automotive` | Auto repair, car wash, etc. |
| `restaurant` | Restaurants and food service |
| `retail` | Retail stores |
| `ecommerce` | E-commerce and online businesses |
| `manufacturing` | Manufacturing and production |
| `distribution` | Wholesale and distribution |
| `professional` | Professional services (accounting, consulting, etc.) |
| `healthcare` | Healthcare and medical services |
| `other` | Other / uncategorized |

---

## Example Record (JSON)

```json
{
  "id": "dl_a1b2c3d4",
  "source_url": "https://tworld.com/listings/cleaning-business-dallas-12345",
  "source_id": "12345",
  "broker_id": "br_transworld",
  "broker_name": "Transworld Business Advisors",
  "first_seen": "2026-01-15T08:30:00Z",
  "last_seen": "2026-01-22T14:15:00Z",
  "status": "active",
  "title": "Profitable Commercial Cleaning Business - Dallas",
  "description": "Well-established commercial cleaning company...",
  "asking_price": 450000,
  "price_hidden": false,
  "vertical": "cleaning",
  "category": "commercial",
  "city": "Dallas",
  "state": "TX",
  "country": "US",
  "zip": null,
  "region": "Dallas-Fort Worth",
  "location_hidden": false,
  "revenue": 620000,
  "cash_flow": 185000,
  "ebitda": null,
  "inventory": 15000,
  "ffe": 45000,
  "real_estate": false,
  "real_estate_value": null,
  "year_established": 2018,
  "employees": 12,
  "seller_financing": true,
  "sba_prequalified": true,
  "franchise": false,
  "franchise_name": null,
  "home_based": false,
  "relocatable": false,
  "absentee_owner": true,
  "scraped_at": "2026-01-22T14:15:00Z",
  "content_hash": "sha256:a1b2c3d4e5f6...",
  "confidence": 0.95,
  "flags": []
}
```

---

## CSV Export Format

Snapshots are exported as CSV with flattened fields. Arrays are pipe-delimited.

```csv
id,source_url,broker_id,broker_name,first_seen,last_seen,status,title,asking_price,...
dl_a1b2c3d4,https://tworld.com/...,br_transworld,Transworld Business Advisors,2026-01-15T08:30:00Z,2026-01-22T14:15:00Z,active,Profitable Commercial Cleaning Business - Dallas,450000,...
```

---

## Schema Versioning

Schema changes follow semver:
- **Major**: Breaking changes (field removed, type changed)
- **Minor**: Additions (new optional fields)
- **Patch**: Clarifications (no structural changes)

Current version: **1.0.0**
