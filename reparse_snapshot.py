#!/usr/bin/env python3
"""
reparse_snapshot.py
-------------------
Cleans a dealledger_scraper_v5 snapshot JSON/CSV:
  1. Removes junk domains (social media, google, sba, etc.)
  2. Fixes titles for known bad-title brokers (WebsiteClosers, SellingRestaurants)
  3. Extracts state/city from raw_text for known domain patterns
  4. Flags online-only businesses (QuietLight, WebsiteClosers) with state='Online'
  5. Removes records with no title or clearly nav/UI garbage titles
  6. Outputs listings_clean.json ready for push_to_supabase.py

Usage:
    python3 reparse_snapshot.py --input data/snapshots/2026-03-24/listings.json
    python3 reparse_snapshot.py --input data/snapshots/2026-03-24/listings.json --output data/snapshots/2026-03-24/listings_clean.json
"""

import re
import json
import argparse
from urllib.parse import urlparse
from datetime import datetime

# ── Junk domains - never include listings from these ─────────────────────────
JUNK_DOMAINS = {
    # Social media
    'www.facebook.com', 'facebook.com',
    'www.instagram.com', 'instagram.com',
    'twitter.com', 'x.com',
    'www.linkedin.com', 'linkedin.com',
    'www.youtube.com', 'youtube.com', 'youtu.be',
    # Google
    'www.google.com', 'google.com',
    'drive.google.com', 'docs.google.com',
    'policies.google.com', 'storage.googleapis.com',
    # Other platforms
    'meetings.hubspot.com',
    'www.sba.gov', 'sba.gov',
    'shareasale.com',
    'zfrmz.com',
    'img1.wsimg.com',
    'api.leadconnectorhq.com',
    'Where%20Would%20You%20Like%20to%20Buy',
}

# ── UI/nav titles that indicate a bad scrape ─────────────────────────────────
JUNK_TITLES = {
    'available', 'add to favorites', 'view details', 'business type',
    'business location', 'asking price', 'contact us', 'login', 'sign in',
    'search results', 'listings', 'home', 'about', 'contact',
    'featured listings', 'all listings', 'browse listings',
    'results', 'filter', 'sort by', 'page', 'next', 'previous',
}

# ── Online-only broker domains (no physical location expected) ────────────────
ONLINE_ONLY_DOMAINS = {
    'quietlight.com',
    'www.websiteclosers.com', 'websiteclosers.com',
    'websiteproperties.com', 'www.websiteproperties.com',
    'www.ecombrokers.co.uk', 'ecombrokers.co.uk',
    'businessexits.com', 'www.businessexits.com',
}

# ── US state abbreviations for extraction ────────────────────────────────────
US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
}

STATE_NAMES = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC',
}

# Canadian provinces
CA_PROVINCES = {'ON', 'BC', 'AB', 'QC', 'MB', 'SK', 'NS', 'NB', 'NL', 'PE', 'NT', 'YT', 'NU'}


def get_domain(url):
    try:
        return urlparse(str(url)).netloc
    except Exception:
        return ''


def is_junk_title(title):
    if not title:
        return True
    t = title.strip().lower()
    if t in JUNK_TITLES:
        return True
    if len(t) < 6:
        return True
    # Single word that looks like a nav element
    if len(t.split()) == 1 and t.isalpha() and len(t) < 12:
        return True
    return False


def extract_state_from_text(raw_text):
    """Try to extract a US state abbreviation from raw_text."""
    if not raw_text:
        return None, None

    text = str(raw_text)

    # Pattern: "City, ST" or "City, State"
    # Try "City, XX" where XX is a state abbreviation
    m = re.search(r',\s*([A-Z]{2})\b', text)
    if m and m.group(1) in US_STATES:
        # Try to get city too
        city_m = re.search(r'([A-Za-z\s]+),\s*' + m.group(1), text)
        city = city_m.group(1).strip() if city_m else None
        return m.group(1), city

    # Try full state name
    for state_name, abbr in STATE_NAMES.items():
        if state_name in text:
            return abbr, None

    return None, None


def fix_websiteclosers_title(raw_text):
    """WebsiteClosers puts real title after 'Available\n' or on second line."""
    if not raw_text:
        return None
    text = str(raw_text)
    # Remove leading 'Available' 
    text = re.sub(r'^Available\s*', '', text).strip()
    # Take first sentence/line up to pipe or newline
    title = re.split(r'[\n|]', text)[0].strip()
    # Truncate at 'WebsiteClosers®'
    title = re.split(r'WebsiteClosers', title)[0].strip()
    # Clean up
    title = re.sub(r'\s+', ' ', title).strip(' |')
    return title if len(title) > 10 else None


def fix_sellingrestaurants_title(raw_text, source_url=''):
    """SellingRestaurants puts '#NNNN Restaurant Name City Region...' in raw_text."""
    if not raw_text:
        return None, None, None
    text = str(raw_text)
    
    # Pattern: #6872 Quick-Service Restaurant with Drive Thru Tacoma Restaurants For Sale
    m = re.match(r'#\d+\s+(.+?)(?:Restaurants For Sale|For Sale|\$)', text)
    if m:
        title_city = m.group(1).strip()
        # Last word(s) before "Restaurants For Sale" is often city
        # Title is everything except trailing city
        parts = title_city.rsplit(' ', 2)
        if len(parts) >= 2:
            title = ' '.join(parts[:-1]).strip()
            city = parts[-1].strip()
        else:
            title = title_city
            city = None
        
        # Extract state from URL or region text
        state, _ = extract_state_from_text(text)
        return title, city, state
    
    # Fallback: just clean up the raw title
    title = re.split(r'Restaurants For Sale|Monthly Rent|Square Feet|\$', text)[0]
    title = re.sub(r'^#\d+\s*', '', title).strip()
    return title if len(title) > 6 else None, None, None


def fix_mergerscorp(raw_text):
    """MergersCorp: 'Title Country EBITDA TBD Gross TBD Price X'"""
    if not raw_text:
        return None, None
    text = str(raw_text)
    # Extract country (appears after title, before EBITDA)
    m = re.search(r'^(.+?)\s+(Romania|United States|Italy|Germany|France|Spain|UK|Canada|Australia|Mexico)\b', text)
    if m:
        country = m.group(2)
        state = 'International' if country != 'United States' else None
        return state, None
    if 'United States' in text:
        state, city = extract_state_from_text(text)
        return state, city
    return 'International', None


def reparse_listing(row):
    """Clean and fix a single listing dict. Returns None to discard."""
    domain = get_domain(row.get('source_url', ''))
    
    # Drop junk domains
    if domain in JUNK_DOMAINS:
        return None
    
    title = str(row.get('title', '') or '').strip()
    raw_text = str(row.get('raw_text', '') or '')
    state = row.get('state')
    city = row.get('city')

    # ── Domain-specific title/location fixes ─────────────────────────────────

    if domain == 'www.websiteclosers.com':
        if is_junk_title(title):
            fixed_title = fix_websiteclosers_title(raw_text)
            if fixed_title:
                title = fixed_title
            else:
                return None
        if not state:
            state = 'Online'

    elif domain in ('sellingrestaurants.com', 'www.sellingrestaurants.com', 'restaurantforsales.com'):
        if is_junk_title(title):
            fixed_title, fixed_city, fixed_state = fix_sellingrestaurants_title(raw_text, row.get('source_url', ''))
            if fixed_title:
                title = fixed_title
                if fixed_city and not city:
                    city = fixed_city
                if fixed_state and not state:
                    state = fixed_state
            else:
                return None
        if not state:
            extracted_state, extracted_city = extract_state_from_text(raw_text)
            if extracted_state:
                state = extracted_state
            if extracted_city and not city:
                city = extracted_city

    elif domain == 'mergerscorp.com':
        if not state:
            fixed_state, fixed_city = fix_mergerscorp(raw_text)
            if fixed_state:
                state = fixed_state
            if fixed_city and not city:
                city = fixed_city

    elif domain in ONLINE_ONLY_DOMAINS:
        if not state:
            state = 'Online'

    else:
        # Generic: try to extract state from raw_text if missing
        if not state and raw_text:
            extracted_state, extracted_city = extract_state_from_text(raw_text)
            if extracted_state:
                state = extracted_state
            if extracted_city and not city:
                city = extracted_city

    # ── Final junk title check ────────────────────────────────────────────────
    if is_junk_title(title):
        return None

    # ── Build clean record ────────────────────────────────────────────────────
    row['title'] = title
    row['state'] = state
    row['city'] = city
    return row


def main():
    parser = argparse.ArgumentParser(description='Reparse and clean a v5 snapshot')
    parser.add_argument('--input', required=True, help='Path to listings.json or listings.csv')
    parser.add_argument('--output', help='Output path (default: same dir as input, listings_clean.json)')
    args = parser.parse_args()

    # Determine output path
    import os
    if args.output:
        output_path = args.output
    else:
        input_dir = os.path.dirname(args.input)
        output_path = os.path.join(input_dir, 'listings_clean.json')

    # Load input
    print(f'Loading {args.input}...')
    if args.input.endswith('.csv'):
        import pandas as pd
        df = pd.read_csv(args.input)
        listings = df.where(pd.notnull(df), None).to_dict('records')
    else:
        with open(args.input) as f:
            listings = json.load(f)

    print(f'Loaded {len(listings)} listings')

    # Reparse
    clean = []
    discarded = 0
    domain_stats = {}

    for row in listings:
        result = reparse_listing(row)
        if result is None:
            discarded += 1
            domain = get_domain(row.get('source_url', ''))
            domain_stats[domain] = domain_stats.get(domain, 0) + 1
        else:
            clean.append(result)

    # Stats
    has_state  = sum(1 for r in clean if r.get('state'))
    has_price  = sum(1 for r in clean if r.get('asking_price'))
    has_cf     = sum(1 for r in clean if r.get('cash_flow'))
    has_all    = sum(1 for r in clean if r.get('state') and r.get('asking_price'))

    print(f'\n============================================================')
    print(f'REPARSE COMPLETE')
    print(f'============================================================')
    print(f'Input:      {len(listings)}')
    print(f'Clean:      {len(clean)}')
    print(f'Discarded:  {discarded}')
    print(f'')
    print(f'Clean breakdown:')
    print(f'  Has state:         {has_state} ({has_state/len(clean)*100:.0f}%)')
    print(f'  Has asking_price:  {has_price} ({has_price/len(clean)*100:.0f}%)')
    print(f'  Has cash_flow:     {has_cf} ({has_cf/len(clean)*100:.0f}%)')
    print(f'  Has state+price:   {has_all} ({has_all/len(clean)*100:.0f}%)')
    print(f'')
    print(f'Top discarded domains:')
    for domain, count in sorted(domain_stats.items(), key=lambda x: -x[1])[:10]:
        print(f'  {domain or "(empty)":<50} {count}')
    print(f'============================================================')

    # Save
    with open(output_path, 'w') as f:
        json.dump(clean, f, indent=2, default=str)
    print(f'\nSaved to {output_path}')


if __name__ == '__main__':
    main()
