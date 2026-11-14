// lib/niches.ts — specific business types carved out of the broad verticals.
//
// Roofing ranks because it is a specific search ("roofing business for sale")
// with a dedicated page holding more listings than the marketplace ads show.
// This repeats that for every specific type with enough listings: pizzerias,
// bars, dental practices, auto repair shops, and so on.
//
// A niche is a title pattern (titles only - descriptions are noise, same rule
// as scrapers/classify_verticals.py), optionally limited to parent verticals.
// A listing can sit in more than one niche ("Pizza & Sports Bar").
//
// Pages: /businesses-for-sale/type/{slug} and /businesses-for-sale/{state}/{slug},
// rendered by the same routes as the verticals. Niche slugs must never collide
// with a TYPES slug in lib/seo.ts.
//
// Data: one cached read of mv_seo_listings_v2 (~21K rows) per server instance,
// matched in memory. No database change needed.

import { getSupabase } from './supabase';
import { Row, Stats, TYPES } from './seo';

export const NICHE_MIN = 10;      // national niche page
export const NICHE_COMBO_MIN = 5; // niche in one state

export type Niche = {
  slug: string;
  label: string;          // "Pizzerias"
  noun: string;           // "pizzerias"
  re: RegExp;             // matched against the title
  not?: RegExp;           // title exclusions
  parents?: string[];     // limit to these verticals (omit = any)
  group: string;          // hub grouping
};

const r = (s: string) => new RegExp(s, 'i');

export const NICHES: Niche[] = [
  // ── Food & drink ─────────────────────────────────────────────────────────
  { slug: 'pizzerias', label: 'Pizzerias', noun: 'pizzerias', re: r('pizz'), group: 'Food & drink' },
  { slug: 'bars-and-taverns', label: 'Bars & Taverns', noun: 'bars, taverns and pubs',
    re: r('\\b(bar|bars|tavern|pub|saloon|nightclub|night club|lounge|taproom|tap room)\\b'),
    not: r('\\b(juice|salad|nail|oxygen|raw|snack|candy|granola|protein|brow|lash|blow ?dry|hot|sushi|candle|beverage|dessert|coffee|espresso|tea|yogurt|ice cream|cereal|mobile|smoothie|poke|taco|build|fruit) bar\\b|bar-?b-?q|\\bbbq\\b|barber|\\bbar (stool|code)'),
    group: 'Food & drink' },
  { slug: 'coffee-shops', label: 'Coffee Shops & Cafés', noun: 'coffee shops and cafés',
    re: r('coffee|\\bcaf[eé]s?\\b|espresso'), group: 'Food & drink' },
  { slug: 'bakeries', label: 'Bakeries', noun: 'bakeries',
    re: r('baker(y|ies)|bake shop|pastr(y|ies)|donut|doughnut|cupcake|patisserie'), group: 'Food & drink' },
  { slug: 'mexican-restaurants', label: 'Mexican Restaurants', noun: 'Mexican restaurants',
    re: r('mexican|taqueria|\\btacos?\\b|cantina|tex-?mex|burrito'), group: 'Food & drink' },
  { slug: 'asian-restaurants', label: 'Asian Restaurants', noun: 'Asian restaurants',
    re: r('chinese|asian|sushi|japanese|\\bthai\\b|vietnamese|\\bpho\\b|ramen|hibachi|korean|teriyaki|poke'),
    parents: ['restaurant', 'retail', 'other'], group: 'Food & drink' },
  { slug: 'italian-restaurants', label: 'Italian Restaurants', noun: 'Italian restaurants',
    re: r('italian|trattoria|ristorante|osteria'), group: 'Food & drink' },
  { slug: 'burger-and-fast-food', label: 'Burger & Fast Food Restaurants', noun: 'burger and fast food restaurants',
    re: r('burger|fast food|quick service|\\bqsr\\b|fast casual|drive-? ?thru|chicken wings?|\\bwings?\\b|fried chicken'),
    group: 'Food & drink' },
  { slug: 'delis-and-sandwich-shops', label: 'Delis & Sandwich Shops', noun: 'delis and sandwich shops',
    re: r('sandwich|\\bdelis?\\b|delicatessen|sub shop|\\bsubs\\b|bagel|hoagie'), group: 'Food & drink' },
  { slug: 'ice-cream-and-dessert', label: 'Ice Cream, Dessert & Smoothie Shops', noun: 'ice cream, dessert and smoothie shops',
    re: r('ice cream|frozen yogurt|froyo|gelato|dessert|smoothie|juice bar|\\bboba\\b|bubble tea|shaved ice|creamery'),
    group: 'Food & drink' },
  { slug: 'breweries-and-wineries', label: 'Breweries, Wineries & Distilleries', noun: 'breweries, wineries and distilleries',
    re: r('brewer(y|ies)|brew ?pub|winer(y|ies)|vineyard|distiller(y|ies)|meadery|cidery'), group: 'Food & drink' },
  { slug: 'food-trucks', label: 'Food Trucks', noun: 'food trucks', re: r('food truck|food trailer'), group: 'Food & drink' },
  { slug: 'catering-companies', label: 'Catering Companies', noun: 'catering companies', re: r('cater'), group: 'Food & drink' },
  { slug: 'food-manufacturing', label: 'Food Manufacturers & Co-Packers', noun: 'food manufacturers and co-packers',
    re: r('food (manufactur|production|processing)|co-?packer|commissary|sauce|spice|snack (brand|manufactur)'),
    group: 'Food & drink' },

  // ── Retail ───────────────────────────────────────────────────────────────
  { slug: 'convenience-stores', label: 'Convenience Stores', noun: 'convenience stores',
    re: r('convenience|c-store|mini ?mart|corner store|bodega|food mart'), group: 'Retail' },
  { slug: 'grocery-stores', label: 'Grocery Stores & Markets', noun: 'grocery stores and markets',
    re: r('grocer|supermarket|food market|meat market|butcher|international market|ethnic market'), group: 'Retail' },
  { slug: 'smoke-and-vape-shops', label: 'Smoke, Vape & CBD Shops', noun: 'smoke, vape and CBD shops',
    re: r('smoke shop|\\bvape|tobacco|\\bcbd\\b|hookah|cigar|head shop|dispensar'), group: 'Retail' },
  { slug: 'liquor-licenses', label: 'Liquor Licenses', noun: 'liquor licenses',
    re: r('liquor licen[cs]e|\\b(c33|type 4[17]|4cop)\\b'),
    not: r('store|shop|\\bmart\\b|market|package|bottle|restaurant|\\bbar\\b|tavern|pub\\b|grill|deli|lounge|business'),
    group: 'Retail' },
  { slug: 'florists', label: 'Florists & Flower Shops', noun: 'florists', re: r('florist|flower shop|floral'), group: 'Retail' },
  { slug: 'jewelry-stores', label: 'Jewelry Stores', noun: 'jewelry stores', re: r('jewel'), group: 'Retail' },
  { slug: 'clothing-boutiques', label: 'Clothing Stores & Boutiques', noun: 'clothing stores and boutiques',
    re: r('boutique|apparel|clothing|fashion|bridal|shoe store'), not: r('hotel|med ?spa'), group: 'Retail' },
  { slug: 'furniture-stores', label: 'Furniture & Mattress Stores', noun: 'furniture and mattress stores',
    re: r('furniture|mattress'), group: 'Retail' },
  { slug: 'thrift-and-consignment', label: 'Thrift & Consignment Stores', noun: 'thrift and consignment stores',
    re: r('thrift|consignment|resale shop|pawn'), group: 'Retail' },
  { slug: 'hardware-and-supply', label: 'Hardware & Supply Stores', noun: 'hardware and supply stores',
    re: r('hardware store|lumber yard|building supply|feed store|farm supply|nursery|garden center'), group: 'Retail' },
  { slug: 'ecommerce-businesses', label: 'Ecommerce & Amazon Businesses', noun: 'ecommerce and Amazon businesses',
    re: r('e-?commerce|amazon|\\bfba\\b|shopify|online (store|retail|retailer|business)|\\bdtc\\b|direct-to-consumer|etsy'),
    group: 'Retail' },

  // ── Health ───────────────────────────────────────────────────────────────
  { slug: 'dental-practices', label: 'Dental Practices', noun: 'dental practices',
    re: r('dental|dentist|orthodont|endodont|periodont|oral surg'), group: 'Health' },
  { slug: 'home-health-and-senior-care', label: 'Home Health & Senior Care Agencies', noun: 'home health and senior care agencies',
    re: r('home health|home ?care|non-medical|senior care|in-home|hospice|caregiv|personal care agency'), group: 'Health' },
  { slug: 'assisted-living', label: 'Assisted Living & Group Homes', noun: 'assisted living facilities and group homes',
    re: r('assisted living|group home|memory care|adult (day|family) (care|home)|residential care|boarding home|personal care home'),
    group: 'Health' },
  { slug: 'physical-therapy-and-chiropractic', label: 'Physical Therapy & Chiropractic Practices', noun: 'physical therapy and chiropractic practices',
    re: r('physical therap|chiropract|rehab(ilitation)? clinic|occupational therap|sports medicine'), group: 'Health' },
  { slug: 'med-spas', label: 'Med Spas & Aesthetics Clinics', noun: 'med spas and aesthetics clinics',
    re: r('med ?spa|medical spa|aesthetic|botox|\\biv (therapy|hydration|bar)|weight loss clinic|laser (clinic|hair)|cosmetic'),
    group: 'Health' },
  { slug: 'medical-practices', label: 'Medical Practices & Clinics', noun: 'medical practices and clinics',
    re: r('medical practice|primary care|urgent care|family (practice|medicine)|pediatric|dermatolog|internal medicine|walk-in clinic|medical clinic'),
    group: 'Health' },
  { slug: 'veterinary-practices', label: 'Veterinary Practices', noun: 'veterinary practices',
    re: r('veterinar|animal hospital|vet clinic|\\bvet\\b'), group: 'Health' },
  { slug: 'pharmacies', label: 'Pharmacies', noun: 'pharmacies', re: r('pharmac|drug ?store|apothecary'), group: 'Health' },
  { slug: 'optometry-practices', label: 'Optometry & Optical Practices', noun: 'optometry and optical practices',
    re: r('optometr|optical|eye ?care|eyewear|ophthalm'), group: 'Health' },
  { slug: 'behavioral-health', label: 'Behavioral Health & Counseling Practices', noun: 'behavioral health and counseling practices',
    re: r('behavioral health|mental health|counseling|psychiatr|psycholog|substance|addiction|\\baba\\b|therapy practice'),
    group: 'Health' },
  { slug: 'medical-billing-and-labs', label: 'Medical Billing, Labs & Medical Supply', noun: 'medical billing, lab and medical supply businesses',
    re: r('medical billing|laborator|\\blab\\b|medical (supply|equipment)|\\bdme\\b|durable medical|diagnostic|imaging'),
    group: 'Health' },

  // ── Personal services ─────────────────────────────────────────────────────
  { slug: 'hair-salons', label: 'Hair Salons', noun: 'hair salons', re: r('hair salon|\\bsalon\\b|salon suite|blow ?dry'),
    not: r('nail salon|tanning salon'), group: 'Personal services' },
  { slug: 'barbershops', label: 'Barbershops', noun: 'barbershops', re: r('barber'), group: 'Personal services' },
  { slug: 'nail-salons', label: 'Nail Salons', noun: 'nail salons', re: r('\\bnails?\\b|manicure|pedicure'), group: 'Personal services' },
  { slug: 'day-spas-and-massage', label: 'Day Spas, Massage & Beauty Studios', noun: 'day spas, massage and beauty studios',
    re: r('day spa|massage|tanning|\\blash|brow|waxing|facial|beauty (studio|bar|salon)|\\bspa\\b'),
    not: r('med ?spa|medical spa|pool|hot tub'), group: 'Personal services' },
  { slug: 'dog-grooming-and-boarding', label: 'Dog Grooming, Daycare & Boarding', noun: 'dog grooming, daycare and boarding businesses',
    re: r('groom|dog (daycare|day care|walking|training)|doggy|kennel|pet (boarding|resort|hotel|sitting)|boarding'),
    parents: ['pet', 'other', 'retail'], group: 'Personal services' },
  { slug: 'martial-arts-and-dance-studios', label: 'Martial Arts, Dance & Yoga Studios', noun: 'martial arts, dance and yoga studios',
    re: r('martial arts|karate|jiu|taekwondo|tae kwon do|dance studio|gymnastics|cheer|yoga|pilates|barre|crossfit|boxing|mma\\b'),
    group: 'Personal services' },
  { slug: 'tutoring-and-education', label: 'Tutoring, Schools & Education Businesses', noun: 'tutoring centers, schools and education businesses',
    re: r('tutor|learning center|test prep|driving school|montessori|private school|charter school|academy|education|music school|coding school'),
    not: r('martial|dance|karate|gymnastics|police|fire'), group: 'Personal services' },
  { slug: 'event-venues', label: 'Event & Wedding Venues', noun: 'event and wedding venues',
    re: r('event (venue|space|center|hall)|wedding venue|banquet|reception hall|party (venue|rental)'), group: 'Personal services' },

  // ── Automotive ───────────────────────────────────────────────────────────
  { slug: 'auto-repair-shops', label: 'Auto Repair Shops', noun: 'auto repair shops',
    re: r('auto(motive)? (repair|service|care|center)|mechanic|transmission|brake|muffler|oil change|lube|repair shop|service center|smog|diesel repair|fleet repair'),
    not: r('collision|body shop|auto body'), group: 'Automotive' },
  { slug: 'collision-and-body-shops', label: 'Collision & Auto Body Shops', noun: 'collision and auto body shops',
    re: r('collision|body shop|auto ?body|paint (and|&) body|dent repair'), group: 'Automotive' },
  { slug: 'tire-shops', label: 'Tire Shops', noun: 'tire shops', re: r('\\btires?\\b'), group: 'Automotive' },
  { slug: 'towing-companies', label: 'Towing Companies', noun: 'towing companies', re: r('towing|tow truck|wrecker|roadside'), group: 'Automotive' },
  { slug: 'auto-dealerships', label: 'Auto, RV & Powersports Dealerships', noun: 'auto, RV and powersports dealerships',
    re: r('dealership|used car|auto sales|car lot|\\brv\\b|motorcycle|powersports|boat dealer|marine dealer|trailer sales'),
    group: 'Automotive' },
  { slug: 'auto-detailing', label: 'Auto Detailing & Window Tint', noun: 'auto detailing and window tint businesses',
    re: r('detailing|window tint|ceramic coating|paint protection'), group: 'Automotive' },

  // ── Trades & construction ────────────────────────────────────────────────
  { slug: 'painting-contractors', label: 'Painting Contractors', noun: 'painting contractors',
    re: r('painting|painters?\\b'), not: r('paint (and|&) body|auto|collision'), group: 'Trades' },
  { slug: 'flooring-companies', label: 'Flooring & Tile Companies', noun: 'flooring and tile companies',
    re: r('flooring|carpet (install|sales|store)|\\btile\\b|epoxy|hardwood floor|countertop'), group: 'Trades' },
  { slug: 'concrete-and-paving', label: 'Concrete, Paving & Masonry Companies', noun: 'concrete, paving and masonry companies',
    re: r('concrete|paving|asphalt|masonry|seal ?coat|stamped|driveway|hardscap'), group: 'Trades' },
  { slug: 'fencing-companies', label: 'Fence & Deck Companies', noun: 'fence and deck companies', re: r('fenc|\\bdecks?\\b|railing'), group: 'Trades' },
  { slug: 'garage-door-companies', label: 'Garage Door Companies', noun: 'garage door companies', re: r('garage door|overhead door'), group: 'Trades' },
  { slug: 'remodeling-contractors', label: 'Remodeling & Handyman Companies', noun: 'remodeling and handyman companies',
    re: r('remodel|renovation|kitchen (and|&) bath|handyman|home improvement|general contractor|home builder|custom home'),
    not: r('(fresh|newly|recently|just) (remodel|renovat)|remodeled|renovated'), group: 'Trades' },
  { slug: 'restoration-companies', label: 'Restoration & Mold Remediation Companies', noun: 'restoration and mold remediation companies',
    re: r('restoration|water damage|fire damage|mold|remediation|abatement|biohazard'), not: r('auto|car|classic'), group: 'Trades' },
  { slug: 'excavation-and-site-work', label: 'Excavation & Site Work Companies', noun: 'excavation and site work companies',
    re: r('excavat|grading|site work|demolition|earthwork|dirt work|land clearing'), group: 'Trades' },
  { slug: 'tree-service-companies', label: 'Tree Service Companies', noun: 'tree service companies',
    re: r('tree (service|care|removal|trimming)|arborist|stump'), group: 'Trades' },
  { slug: 'solar-companies', label: 'Solar Companies', noun: 'solar companies', re: r('solar'), group: 'Trades' },
  { slug: 'septic-and-portable-toilets', label: 'Septic & Portable Toilet Companies', noun: 'septic and portable toilet companies',
    re: r('septic|portable (toilet|restroom|sanitation)|\\bporta[- ]?(potty|potties|john)|grease trap|drain cleaning|sewer'), group: 'Trades' },
  { slug: 'irrigation-and-sprinkler', label: 'Irrigation & Sprinkler Companies', noun: 'irrigation and sprinkler companies',
    re: r('irrigation|sprinkler'), not: r('fire sprinkler'), group: 'Trades' },
  { slug: 'fire-protection-companies', label: 'Fire Protection & Life Safety Companies', noun: 'fire protection and life safety companies',
    re: r('fire (protection|sprinkler|alarm|suppression|extinguisher)|life safety|backflow'), group: 'Trades' },
  { slug: 'security-and-alarm-companies', label: 'Security, Alarm & Locksmith Companies', noun: 'security, alarm and locksmith companies',
    re: r('security|alarm|locksmith|surveillance|access control|\\bcctv\\b'), not: r('fire alarm|social security'), group: 'Trades' },
  { slug: 'appliance-repair', label: 'Appliance Repair Companies', noun: 'appliance repair companies', re: r('appliance'), group: 'Trades' },

  // ── Industrial ───────────────────────────────────────────────────────────
  { slug: 'machine-shops', label: 'Machine Shops & Metal Fabricators', noun: 'machine shops and metal fabricators',
    re: r('machine shop|machining|\\bcnc\\b|fabricat|welding|metal (work|fab|stamp)|sheet metal|steel'), group: 'Industrial' },
  { slug: 'cabinet-and-millwork', label: 'Cabinet, Millwork & Woodworking Shops', noun: 'cabinet, millwork and woodworking shops',
    re: r('cabinet|millwork|woodwork|wood products|furniture (maker|manufactur)'), group: 'Industrial' },
  { slug: 'plastics-and-packaging', label: 'Plastics & Packaging Manufacturers', noun: 'plastics and packaging manufacturers',
    re: r('plastic|injection mold|packaging|corrugated|label'), group: 'Industrial' },
  { slug: 'wholesale-distributors', label: 'Wholesale Distributors', noun: 'wholesale distributors',
    re: r('distribut|wholesal|import(er|s)?\\b'), not: r('route'), group: 'Industrial' },
  { slug: 'logistics-and-courier', label: 'Logistics, Freight & Courier Companies', noun: 'logistics, freight and courier companies',
    re: r('logistic|freight|\\b3pl\\b|courier|last.mile|warehous|dispatch|expedit'), group: 'Industrial' },
  { slug: 'moving-and-storage', label: 'Moving & Storage Companies', noun: 'moving and storage companies',
    re: r('moving (company|business|and|&)|\\bmovers?\\b|self.?storage|storage (facility|units)|mini.?storage'), group: 'Industrial' },

  // ── Professional & tech ──────────────────────────────────────────────────
  { slug: 'insurance-agencies', label: 'Insurance Agencies', noun: 'insurance agencies',
    re: r('insurance (agency|brokerage|book|business)|book of business'), group: 'Professional & tech' },
  { slug: 'staffing-agencies', label: 'Staffing & Recruiting Agencies', noun: 'staffing and recruiting agencies',
    re: r('staffing|recruit|employment agency|\\bpeo\\b|temp agency'), group: 'Professional & tech' },
  { slug: 'law-firms', label: 'Law Firms', noun: 'law firms', re: r('law (firm|practice|office)|legal practice|attorney'), group: 'Professional & tech' },
  { slug: 'property-management-companies', label: 'Property Management Companies', noun: 'property management companies',
    re: r('property management|\\bhoa\\b|community management|rental management|vacation rental management'), group: 'Professional & tech' },
  { slug: 'marketing-agencies', label: 'Marketing & Advertising Agencies', noun: 'marketing and advertising agencies',
    re: r('marketing (agency|firm|company)|digital marketing|advertising|\\bseo\\b|web design|media company|public relations|\\bpr firm'),
    group: 'Professional & tech' },
  { slug: 'it-and-managed-services', label: 'IT & Managed Service Providers', noun: 'IT and managed service providers',
    re: r('managed (service|it)|\\bmsp\\b|it (services|support|consulting|solutions)|cyber|network(ing)? (services|integrat)|tech support|computer repair'),
    group: 'Professional & tech' },
  { slug: 'saas-and-software', label: 'SaaS & Software Companies', noun: 'SaaS and software companies',
    re: r('\\bsaas\\b|software|\\bapp\\b|platform'), parents: ['technology', 'other'], group: 'Professional & tech' },
  { slug: 'engineering-and-consulting', label: 'Engineering & Consulting Firms', noun: 'engineering and consulting firms',
    re: r('engineering|consult|architect|surveying|environmental services'), group: 'Professional & tech' },
  { slug: 'travel-and-hospitality', label: 'Hotels, Motels & Travel Businesses', noun: 'hotels, motels and travel businesses',
    re: r('hotel|motel|\\binn\\b|bed (and|&) breakfast|\\bb&b\\b|lodge|campground|rv park|travel agency|tour (company|operator)'),
    group: 'Professional & tech' },
  { slug: 'franchise-resales', label: 'Franchise Resales', noun: 'franchise resales',
    re: r('franchise'), not: r('franchise (opportunity|available|territor)|new franchise|start (your|a) franchise'),
    group: 'Professional & tech' },
];

const BY_SLUG = new Map(NICHES.map((n) => [n.slug, n]));
for (const n of NICHES) {
  if (Object.values(TYPES).some((t) => t.slug === n.slug)) throw new Error(`niche slug collides with a type: ${n.slug}`);
}

export const nicheFromSlug = (slug: string): Niche | null => BY_SLUG.get(slug) ?? null;
export const nichePath = (slug: string) => `/businesses-for-sale/type/${slug}`;

// ── Data ────────────────────────────────────────────────────────────────────
type URow = Row & { dom_days_eff: number | null; relisted: boolean | null };

const U_COLS =
  'listing_number, title, city, state_code, vertical, price, cash_flow, dom_display, dom_basis, estimated_listed_date, broker_domain, dom_days_eff, relisted';
const PAGE = 1000;
const TTL_MS = 60 * 60 * 1000;
let cache: { at: number; rows: URow[] } | null = null;
let inflight: Promise<URow[]> | null = null;

async function universe(): Promise<URow[]> {
  if (cache && Date.now() - cache.at < TTL_MS) return cache.rows;
  if (inflight) return inflight;
  inflight = (async () => {
    const sb = getSupabase();
    const { count } = await sb.from('mv_seo_listings_v2').select('listing_number', { count: 'exact', head: true });
    const pages = Math.ceil((count || 0) / PAGE) || 1;
    const chunks = await Promise.all(
      Array.from({ length: pages }, (_, i) =>
        sb.from('mv_seo_listings_v2').select(U_COLS).order('listing_number').range(i * PAGE, i * PAGE + PAGE - 1)
          .then(({ data, error }) => {
            if (error) throw error;
            return (data || []) as URow[];
          })),
    );
    const rows = chunks.flat();
    cache = { at: Date.now(), rows };
    return rows;
  })();
  try {
    return await inflight;
  } finally {
    inflight = null;
  }
}

export const inNiche = (n: Niche, row: { title: string | null; vertical: string }) => {
  const t = row.title || '';
  if (!n.re.test(t)) return false;
  if (n.not && n.not.test(t)) return false;
  if (n.parents && !n.parents.includes(row.vertical)) return false;
  return true;
};

const median = (xs: number[]) => {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};

function statsFor(rows: URow[], slug: string, state: string | null): Stats {
  const prices = rows.map((x) => x.price).filter((v): v is number => !!v && v > 0);
  const cfs = rows.map((x) => x.cash_flow).filter((v): v is number => !!v && v > 0);
  const mults = rows
    .filter((x) => x.price && x.price > 0 && x.cash_flow && x.cash_flow > 0)
    .map((x) => x.price! / x.cash_flow!)
    .filter((m) => m > 0.2 && m < 20);
  return {
    scope: state ? 'vertical_state' : 'vertical',
    page_vertical: slug,
    page_state: state,
    listings: rows.length,
    brokers: new Set(rows.map((x) => x.broker_domain).filter(Boolean)).size,
    median_price: median(prices),
    median_cash_flow: median(cfs),
    median_multiple: median(mults),
    new_30d: rows.filter((x) => x.dom_basis === 'observed' && x.dom_days_eff != null && x.dom_days_eff >= 0 && x.dom_days_eff <= 30).length,
    relisted: rows.filter((x) => x.relisted).length,
    with_cash_flow: cfs.length,
    refreshed_at: null,
  };
}

const byListedDesc = (a: URow, b: URow) =>
  (b.estimated_listed_date || '').localeCompare(a.estimated_listed_date || '');

export async function nicheData(slug: string, state: string | null = null, limit = 200) {
  const n = nicheFromSlug(slug);
  if (!n) return null;
  const all = (await universe()).filter((x) => inNiche(n, x));
  const scoped = state ? all.filter((x) => x.state_code === state) : all;
  const byState = new Map<string, number>();
  for (const x of all) if (x.state_code) byState.set(x.state_code, (byState.get(x.state_code) || 0) + 1);
  return {
    niche: n,
    stats: statsFor(scoped, slug, state),
    rows: [...scoped].sort(byListedDesc).slice(0, limit) as Row[],
    statesWithPage: [...byState.entries()]
      .filter(([, c]) => c >= NICHE_COMBO_MIN)
      .sort((a, b) => b[1] - a[1])
      .map(([code, count]) => ({ code, count })),
  };
}

/** Counts for every niche, nationally and by state. For the hub, sitemap and cross-links. */
export async function nicheCounts() {
  const rows = await universe();
  return NICHES.map((n) => {
    const hits = rows.filter((x) => inNiche(n, x));
    const states: Record<string, number> = {};
    for (const x of hits) if (x.state_code) states[x.state_code] = (states[x.state_code] || 0) + 1;
    return { slug: n.slug, label: n.label, group: n.group, count: hits.length, states };
  });
}

/** Everything a niche page needs, national (state = null) or in one state. */
export async function nichePageProps(slug: string, state: string | null) {
  const d = await nicheData(slug, state);
  if (!d) return null;
  const counts = await nicheCounts();
  const min = state ? NICHE_COMBO_MIN : NICHE_MIN;
  const related = counts
    .filter((c) => c.slug !== slug && c.group === d.niche.group)
    .map((c) => ({ slug: c.slug, label: c.label, count: state ? c.states[state] || 0 : c.count }))
    .filter((c) => c.count >= min)
    .sort((a, b) => b.count - a.count);
  return {
    slug,
    state,
    label: d.niche.label,
    noun: d.niche.noun,
    group: d.niche.group,
    thin: d.stats.listings < min,
    stats: d.stats,
    rows: d.rows,
    states: d.statesWithPage.filter((x) => x.code !== state),
    related,
    nationalCount: counts.find((c) => c.slug === slug)?.count ?? 0,
  };
}
export type NichePageProps = NonNullable<Awaited<ReturnType<typeof nichePageProps>>>;

/** Which niche groups to cross-link from a broad vertical's pages. */
export const VERTICAL_NICHE_GROUPS: Record<string, string[]> = {
  restaurant: ['Food & drink'],
  retail: ['Retail'],
  healthcare: ['Health'],
  automotive: ['Automotive'],
  construction: ['Trades'],
  manufacturing: ['Industrial'],
  technology: ['Professional & tech'],
  salon_spa: ['Personal services'],
  pet: ['Personal services'],
  fitness: ['Personal services'],
  childcare: ['Personal services'],
};

export const NICHE_GROUP_ORDER = ['Food & drink', 'Retail', 'Health', 'Personal services', 'Automotive', 'Trades', 'Industrial', 'Professional & tech'];

/** Listings an alert covers. `key` is a TYPES vertical key, a niche slug, or null (any type). */
export async function alertMatches(state: string | null, key: string | null) {
  const niche = key ? nicheFromSlug(key) : null;
  return (await universe()).filter((x) =>
    (!state || x.state_code === state) &&
    (!key || (niche ? inNiche(niche, x) : x.vertical === key)));
}

/** Most specific niche a listing belongs to (first in NICHES order), for "more like this". */
export const firstNiche = (row: { title: string | null; vertical: string }) =>
  NICHES.find((n) => n.slug !== 'franchise-resales' && inNiche(n, row)) ?? null;
