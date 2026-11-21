// GET /api/v1/reference — valid state codes and business types.
import { route } from '../../../lib/intelligence/http';
import { referenceData } from '../../../lib/intelligence';

export default route('reference', async () => ({ body: referenceData() }));
