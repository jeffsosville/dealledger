// 404 — most misses are listings that sold or came down. Offer the next one instead of a dead end.
import type { GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../components/SeoPage';
import { alertPickerOptions } from '../lib/alerts';

type Props = { picker: Awaited<ReturnType<typeof alertPickerOptions>> };

export const getStaticProps: GetStaticProps<Props> = async () => ({
  props: { picker: await alertPickerOptions() },
  revalidate: 60 * 60 * 24,
});

export default function NotFound({ picker }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <SeoPage
      path="/404"
      noindex
      noCsv
      title="Page not found — DealLedger"
      h1="That page isn’t here"
      eyebrow="404"
      description="This page could not be found on DealLedger."
      intro="If you were looking for a listing, it has probably sold or been taken down by the broker. Tell us what you’re looking for and we’ll email you the next one."
      stats={null}
      rows={[]}
      alert={{ label: 'Businesses for sale', heading: 'Get the next one by email', picker }}
      crumbs={[{ href: '/', label: 'DealLedger' }, { href: '/404', label: 'Not found' }]}
      groups={[{ title: 'Keep looking', links: [
        { href: '/businesses-for-sale', label: 'Businesses for sale by state and type' },
        { href: '/', label: 'DealLedger home' },
      ] }]}
    />
  );
}
