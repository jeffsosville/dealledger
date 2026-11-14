// /alerts — standalone signup for new-listing email alerts: pick a type and a state.
import type { GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../components/SeoPage';
import { alertPickerOptions } from '../lib/alerts';

type Props = { picker: Awaited<ReturnType<typeof alertPickerOptions>> };

export const getStaticProps: GetStaticProps<Props> = async () => ({
  props: { picker: await alertPickerOptions() },
  revalidate: 60 * 60 * 24,
});

export default function AlertsPage({ picker }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <SeoPage
      path="/alerts"
      title="New Business-for-Sale Listing Alerts — DealLedger"
      h1="Get new businesses for sale by email"
      eyebrow="ALERTS"
      description="A free weekly email of new businesses for sale in the type and state you choose, read directly from broker websites, with how long each has really been listed."
      intro="Pick a type of business and a state. Once a week, DealLedger emails you the new listings it found on broker websites, often before they reach the big marketplaces, with the real listing age for each. Free, no account, one-click unsubscribe."
      stats={null}
      rows={[]}
      alert={{ label: 'Businesses for sale', heading: 'Choose what to watch', picker }}
      crumbs={[{ href: '/', label: 'DealLedger' }, { href: '/alerts', label: 'Alerts' }]}
      groups={[{ title: 'Or browse first', links: [{ href: '/businesses-for-sale', label: 'All businesses for sale by state and type' }] }]}
    />
  );
}
