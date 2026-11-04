-- 2026-11-04: the insert guard logged ~275 "url already quarantined" rows a
-- night. BEFORE INSERT fires on every upsert, including ones that resolve to
-- an UPDATE of a row that is already quarantined, so the same URLs were
-- re-logged daily and inflated daily_health's quarantined_24h.
-- Same behaviour; quarantine_log only gets a row when the status really changes.
CREATE OR REPLACE FUNCTION public.listings_direct_insert_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare prior text;
begin
  if coalesce(new.status, 'active') <> 'active' then
    return new;
  end if;

  if exists (select 1 from public.broker_block k
              where regexp_replace(lower(k.broker_domain), '^www\.', '')
                  = regexp_replace(lower(new.broker_domain), '^www\.', '')) then
    new.status := 'quarantined_blocked';
    if not exists (select 1 from public.listings_direct l where l.id = new.id and l.status = new.status) then
      insert into public.quarantine_log(id, prior_status, new_status, reason, title, url, at)
      values (new.id, 'active', new.status, 'insert_guard: blocked domain', new.title, new.url, now());
    end if;
    return new;
  end if;

  select l.status into prior from public.listings_direct l
   where l.url = new.url and l.status like 'quarantined%'
   order by l.updated_at desc nulls last limit 1;
  if prior is not null then
    new.status := prior;
    if not exists (select 1 from public.listings_direct l where l.id = new.id and l.status = new.status) then
      insert into public.quarantine_log(id, prior_status, new_status, reason, title, url, at)
      values (new.id, 'active', new.status, 'insert_guard: url already quarantined', new.title, new.url, now());
    end if;
    return new;
  end if;

  if new.url ~* '(/single-family/|/condominium/|/townhouses?/|/multi-family/|/properties/listing/|/homes-for-sale|/idx/|/mls-?listings?/)' then
    new.status := 'quarantined_residential';
    if not exists (select 1 from public.listings_direct l where l.id = new.id and l.status = new.status) then
      insert into public.quarantine_log(id, prior_status, new_status, reason, title, url, at)
      values (new.id, 'active', new.status, 'insert_guard: property-listing url', new.title, new.url, now());
    end if;
  end if;
  return new;
end $function$;
