-- Stage-2 decisions from stored discovery evidence (no new fetches). 2026-10-13.
-- NOT APPLIED. Run after Jeff approves. Every row gets decided_by='human'.
--
-- A. 101 real-estate sites: domain is a realty/realtor/property name, or the
--    gate counted >=6 residential/CRE terms and at least twice as many as business
--    terms. Hard rule: realtor/IDX/CRE doesn't belong -> 0_blocked + broker_block.
--    Review the list; delete any line that is a real business broker.
begin;
with d(domain) as (select unnest(array[
  'acerealty.net',
  'akberbhamani.atlcommunities.com',
  'alaskaunlimitedrealty.com',
  'allfirstcommercial.net',
  'andrea.brayandcosearch.com',
  'aspectrep.com',
  'avantiway.com',
  'baldwinrealestateenterprises.com',
  'bhhsdrysdale.com',
  'bloomtreerealty.com',
  'bluesharkrealty.com',
  'brcrealestate.com',
  'buckeyeprorealty.com',
  'c21beggins.com',
  'casschies.com',
  'cervera.com',
  'christiesrealestate.com',
  'citypropertyassociates.com',
  'coldwellbankerhomes.com',
  'commercial.saundersrealestate.com',
  'connectionflorida.com',
  'corcoran.com',
  'crossvilleremax.com',
  'cygnusrealestate.com',
  'dalanrealty.com',
  'dnvrrealty.com',
  'dream-realty.com',
  'dreammorerealty.net',
  'duaneburrows.com',
  'dvbrealty.com',
  'excelcres.com',
  'fandbleasing.com',
  'firstcoloradorealty.com',
  'firstintrnatl.com',
  'futurehomerealty.com',
  'futureinsiterealty.com',
  'getthepros.com',
  'gfwillis.com',
  'grandrealtyofamerica.com',
  'hawkinscre.com',
  'horizonhomesrealty.com',
  'infinitiproperties.net',
  'iraspilky.com',
  'jimshaub.com',
  'jmrealtyservicescorp.squarespace.com',
  'jonasmartin.com',
  'joycerealty.com',
  'judgefite.com',
  'keyrealtynevada.com',
  'kidsdepotrealty.com',
  'lacunacre.com',
  'leesabyrnesrealty.com',
  'legacyrealestate.net',
  'level-re.com',
  'mainsourcerealty.com',
  'masiello.com',
  'maxfieldre.com',
  'maxoneproperties.com',
  'maybenrealty.com',
  'mikefullerrealty.com',
  'murphyrealtygrp.com',
  'mywdr.com',
  'nar.realtor',
  'newmichiganrealty.com',
  'nexthome.com',
  'omni-re.com',
  'orlandocommercialrealestate.com',
  'ownrealty.com',
  'paraneejudd.com',
  'paulfaddis.com',
  'procisioncre.com',
  'propertiesct.com',
  'prosellproperty.com.au',
  'pyrealtyco.com',
  'rafaelzuzolo.com',
  'rapidcitycommercial.com',
  'realamericarealty.com',
  'realtyhive.com',
  'recckio.com',
  'resmik.com',
  'robertlilley.com',
  'sangcanhelp.com',
  'scottvarley.com',
  'search.avantiway.com',
  'skyrei.com',
  'sothebysrealty.ca',
  'sturgesproperty.com',
  'swannrealestate.com',
  'teamner.com',
  'telloteam.com',
  'theimateam.com',
  'trelleum.com',
  'tripleoaksrealty.com',
  'tru-south.com',
  'unitedrealestatefortune.com',
  'usrealtyhome.com',
  'vantagepointretail.com',
  'venturegj.com',
  'windermerelaneco.com',
  'winmorrisonrealty.com',
  'yougotlistings.com'
]))
, upd as (
  update broker_sources b set discovery_stage='0_blocked', strategy_status='blocked',
    last_error_type='REJECTED', decided_at=now(), decided_by='human', updated_at=now(),
    decision_reason='stage-2 review 2026-10-13: real estate (discovery gate evidence)'
  from d where regexp_replace(lower(b.domain),'^www\.','')=d.domain
    and b.discovery_stage='2_listings_url_known'
  returning b.domain)
insert into broker_block (broker_domain, reason, updated_at)
select regexp_replace(lower(domain),'^www\.',''), 'stage-2 review 2026-10-13: real estate', now() from upd
on conflict do nothing;

-- B. 3 franchise local-office sites (Murphy agent pages, Sunbelt offices):
--    listings come from the corporate feed; same call as the Oct 12 office decisions.
update broker_sources b set discovery_stage='0_unusable', strategy_status='unusable',
  last_error_type='REJECTED', decided_at=now(), decided_by='human', updated_at=now(),
  decision_reason='stage-2 review 2026-10-13: franchise office, captured at corp level'
where regexp_replace(lower(b.domain),'^www\.','') in ('marwannabulsi-murphy.com', 'michaelmetzger-murphy.com', 'sunbeltorlando.com')
  and b.discovery_stage='2_listings_url_known';
commit;
