-- 2026-11-02 dl_lookup_evidence(q): dl_lookup plus the evidence behind it.
--
-- For BizBuySell / BizQuest listing numbers, dl_lookup's date is interpolated
-- between Internet Archive anchors. That interpolated date is a good estimate,
-- but the date that is actually PROVEN is the bound_date of the next anchor
-- above the listing number: that higher-numbered listing was captured by
-- then, and numbers are assigned in order, so this one existed by then too.
--
-- This adds:
--   provable_listed_by / provable_days  the conservative, defensible figures
--   anchor_below / anchor_above         the anchors either side (number, site,
--                                       earliest capture, bound date)
--   archive_url                         Wayback history of the URL itself, to
--                                       check whether the ad was up continuously
--
-- A listing number dates when the listing record was created. It does not
-- prove the ad was advertised continuously since then; a reactivated listing
-- keeps its old number. archive_url is how you check continuity.
--
-- Used by scripts/listing_age_batch.py. Reads only DealLedger's own tables.
-- APPLIED to production on 2026-11-02.

create or replace function public.dl_lookup_evidence(q text)
returns jsonb language plpgsql security definer set search_path = public as $$
declare
  base   jsonb := public.dl_lookup(q);
  ln     bigint := nullif(base->>'listing_number', '')::bigint;
  below  record;
  above  record;
  proven date;
  raw    text := btrim(coalesce(q, ''));
begin
  if base->>'kind' <> 'estimate' or ln is null then
    return base || jsonb_build_object('archive_url',
      case when raw ~* '^https?://' then 'https://web.archive.org/web/2*/' || raw end);
  end if;

  select listing_number, site, earliest_capture, bound_date into below
  from wayback_anchors where listing_number <= ln order by listing_number desc limit 1;
  select listing_number, site, earliest_capture, bound_date into above
  from wayback_anchors where listing_number > ln order by listing_number asc limit 1;

  proven := above.bound_date;
  if proven is null then
    -- Past the last Wayback anchor: fall back to the next calibration anchor.
    select anchor_date into proven from dom_anchors where listing_number > ln order by listing_number asc limit 1;
  end if;

  return base || jsonb_build_object(
    'provable_listed_by', proven,
    'provable_days', case when proven is not null then current_date - proven end,
    'anchor_below', case when below.listing_number is not null then jsonb_build_object(
        'listing_number', below.listing_number, 'site', below.site,
        'earliest_capture', below.earliest_capture, 'bound_date', below.bound_date) end,
    'anchor_above', case when above.listing_number is not null then jsonb_build_object(
        'listing_number', above.listing_number, 'site', above.site,
        'earliest_capture', above.earliest_capture, 'bound_date', above.bound_date) end,
    'archive_url', case when raw ~* '^https?://' then 'https://web.archive.org/web/2*/' || raw end
  );
end $$;

grant execute on function public.dl_lookup_evidence(text) to anon, authenticated;
