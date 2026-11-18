// pages/listings/[dl].tsx
//
// /listings/dl-07712603 → /listing/{listing_number}
//
// Listing pages show a "DL-" record id (listings.id, zero-padded to 8). People and
// AI answers turn that into /listings/dl-<id>; resolve it to the real listing page.
// Anything unresolvable goes to the browse hub instead of a 404.

import type { GetServerSideProps } from 'next';
import { getSupabase } from '../../lib/supabase';

export const getServerSideProps: GetServerSideProps = async ({ params }) => {
  const raw = String(params?.dl || '');
  const m = raw.match(/^dl-?0*(\d{1,10})$/i);
  if (m) {
    const { data } = await getSupabase()
      .from('listings')
      .select('listing_number')
      .eq('id', Number(m[1]))
      .maybeSingle();
    if (data?.listing_number) {
      return { redirect: { destination: `/listing/${data.listing_number}`, permanent: true } };
    }
  }
  return { redirect: { destination: '/businesses-for-sale', permanent: false } };
};

export default function ListingsRedirect() {
  return null;
}
