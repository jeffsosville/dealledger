// GET /api/v1 — API index: what's here and how to call it.
import { route } from '../../../lib/intelligence/http';
import { SITE } from '../../../lib/intelligence';

export default route('index', async () => ({
  body: {
    name: 'DealLedger API',
    description:
      'Read-only, public-domain (CC0) data on US businesses for sale, observed directly on broker websites: listings, ' +
      'observed listing age with evidence, reviewed asking-price changes and relist signals. No key needed.',
    docs: `${SITE}/developers`,
    openapi: `${SITE}/openapi.json`,
    mcp: `${SITE}/api/mcp`,
    endpoints: {
      'GET /api/v1/listings': 'Search public listings. Filters: state, type, q, broker, min_price, max_price, min_cash_flow, min_age_days, relisted. sort, limit (≤100), offset. format=csv for up to 5,000 rows.',
      'GET /api/v1/listings/{listing_number}': 'Full history of one listing: price, observed age with evidence, price changes, relist signal.',
      'GET /api/v1/price-changes': 'Reviewed asking-price changes. Filters: state, type, direction (down|up|any), min_pct, since. limit (≤100), offset. format=csv.',
      'GET /api/v1/reference': 'Valid state codes and business types.',
    },
    rules: [
      'Observed age is a minimum from DealLedger crawls, not verified days on market.',
      'Price changes are reported as a date window; the exact day of the change is unknown.',
      'A relist signal is a heuristic match, never confirmed.',
      'Results are paged: follow next_offset until it is null to get every match.',
    ],
  },
}));
