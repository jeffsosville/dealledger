// GET /api/v1/listings/{listing_number} — everything DealLedger has observed about one listing.
import { route } from '../../../../lib/intelligence/http';
import { getListing } from '../../../../lib/intelligence';

export default route('listing', async (req) => ({ body: await getListing(req.query.id) }));
