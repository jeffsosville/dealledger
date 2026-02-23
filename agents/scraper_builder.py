#!/usr/bin/env python3
"""
DealLedger Scraper Builder Agent
=================================
Submit a broker URL → AI analyzes the page → outputs a scraper config.

This is the "AI-assisted plumbing" layer:
- Claude analyzes the DOM structure
- Identifies listing containers, price fields, pagination
- Generates a deterministic scraper config (JSON)
- The config runs in the boring Python pipeline

Usage:
    python3 scraper_builder.py https://example-broker.com/listings
    python3 scraper_builder.py --batch urls.txt
    python3 scraper_builder.py --url https://example-broker.com/listings --output configs/

Requirements:
    pip3 install anthropic requests beautifulsoup4 --break-system-packages
    export ANTHROPIC_API_KEY=sk-ant-...
"""

import argparse
import json
import os
import sys
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    print("⚠️  anthropic package not installed. Install: pip3 install anthropic")


USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


ANALYSIS_PROMPT = """You are a web scraping expert analyzing a broker website that lists businesses for sale.

Given the HTML below, identify the pattern for extracting business listings.

Return ONLY a JSON object (no markdown, no explanation) with these fields:

{{
    "broker_name": "Name of the brokerage firm",
    "has_listings": true/false,
    "listing_container_selector": "CSS selector for the repeating listing element",
    "title_selector": "CSS selector for listing title (relative to container)",
    "price_selector": "CSS selector for asking price (relative to container)",
    "revenue_selector": "CSS selector for revenue/gross (relative to container) or null",
    "cashflow_selector": "CSS selector for cash flow/SDE (relative to container) or null", 
    "location_selector": "CSS selector for city/state (relative to container) or null",
    "detail_link_selector": "CSS selector for detail page link (relative to container) or null",
    "pagination_next_selector": "CSS selector for next page button/link or null",
    "listing_count_on_page": number of listings visible on this page,
    "notes": "any relevant observations about the site structure",
    "confidence": "high/medium/low"
}}

If the page does NOT contain business-for-sale listings, set has_listings to false and leave selectors as null.

Important:
- Use specific CSS selectors that would work with BeautifulSoup's .select()
- Prefer class-based selectors over nth-child or positional selectors
- The container selector should match ALL listing items on the page
- Relative selectors should work when applied to each container element

URL: {url}

HTML (truncated to key content):
{html}
"""


def fetch_page(url):
    """Fetch a page and return cleaned HTML."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    }
    resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
    resp.raise_for_status()
    return resp.text


def clean_html_for_analysis(html, max_chars=15000):
    """Strip scripts, styles, and reduce HTML for API consumption."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Remove noise
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg", "path"]):
        tag.decompose()
    
    # Remove comments
    from bs4 import Comment
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    # Remove hidden elements
    for el in soup.find_all(attrs={"style": re.compile(r"display:\s*none", re.I)}):
        el.decompose()
    
    # Get the body content
    body = soup.find("body")
    if body:
        html_clean = str(body)
    else:
        html_clean = str(soup)
    
    # Truncate intelligently - keep the middle where listings likely are
    if len(html_clean) > max_chars:
        # Take first 5000 and last 10000 chars (listings are usually in the main content)
        html_clean = html_clean[:5000] + "\n...[TRUNCATED]...\n" + html_clean[-10000:]
    
    return html_clean


def analyze_with_claude(url, html):
    """Send HTML to Claude for analysis."""
    if not HAS_ANTHROPIC:
        print("❌ anthropic package required. Install: pip3 install anthropic")
        sys.exit(1)
    
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ Set ANTHROPIC_API_KEY environment variable")
        sys.exit(1)
    
    client = anthropic.Anthropic(api_key=api_key)
    
    cleaned_html = clean_html_for_analysis(html)
    prompt = ANALYSIS_PROMPT.format(url=url, html=cleaned_html)
    
    print(f"   🤖 Sending to Claude for analysis ({len(cleaned_html):,} chars)...")
    
    response = client.messages.create(
        model="claude-sonnet-4-5-20250514",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    
    # Parse response
    text = response.content[0].text.strip()
    
    # Clean up potential markdown wrapping
    if text.startswith("```"):
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
    
    try:
        config = json.loads(text)
        return config
    except json.JSONDecodeError as e:
        print(f"   ⚠️  Failed to parse Claude response: {e}")
        print(f"   Raw response: {text[:500]}")
        return None


def validate_config(config, html):
    """Validate the generated config against the actual HTML."""
    if not config or not config.get("has_listings"):
        return config
    
    soup = BeautifulSoup(html, "html.parser")
    
    selector = config.get("listing_container_selector", "")
    if selector:
        try:
            elements = soup.select(selector)
            config["validation"] = {
                "elements_found": len(elements),
                "matches_claimed_count": abs(len(elements) - config.get("listing_count_on_page", 0)) <= 3,
                "validated": len(elements) >= 2,
            }
            
            if len(elements) == 0:
                config["validation"]["warning"] = "Selector found 0 elements - may need adjustment"
                config["confidence"] = "low"
            
        except Exception as e:
            config["validation"] = {
                "error": str(e),
                "validated": False,
            }
    
    return config


def save_config(config, url, output_dir="data/scraper_configs"):
    """Save the scraper config to a JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    
    domain = urlparse(url).netloc.replace("www.", "")
    safe_name = re.sub(r'[^a-zA-Z0-9.-]', '_', domain)
    filepath = os.path.join(output_dir, f"{safe_name}.json")
    
    # Add metadata
    config["_meta"] = {
        "source_url": url,
        "domain": domain,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator": "dealledger-scraper-builder-v1",
    }
    
    with open(filepath, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"   💾 Config saved to {filepath}")
    return filepath


def build_scraper(url, output_dir="data/scraper_configs"):
    """Full pipeline: fetch → analyze → validate → save."""
    print(f"\n{'='*60}")
    print(f"🔨 Building scraper for: {url}")
    print(f"{'='*60}")
    
    # Fetch
    print(f"   📥 Fetching page...")
    try:
        html = fetch_page(url)
        print(f"   ✅ Fetched ({len(html):,} bytes)")
    except Exception as e:
        print(f"   ❌ Fetch failed: {e}")
        return None
    
    # Analyze
    config = analyze_with_claude(url, html)
    if not config:
        return None
    
    # Validate
    config = validate_config(config, html)
    
    # Report
    if config.get("has_listings"):
        print(f"\n   📊 Results:")
        print(f"      Broker: {config.get('broker_name', '?')}")
        print(f"      Container: {config.get('listing_container_selector', '?')}")
        print(f"      Listings found: {config.get('listing_count_on_page', '?')}")
        print(f"      Confidence: {config.get('confidence', '?')}")
        
        validation = config.get("validation", {})
        if validation.get("validated"):
            print(f"      ✅ Validated: {validation.get('elements_found', '?')} elements match selector")
        elif validation.get("warning"):
            print(f"      ⚠️  {validation['warning']}")
        
        # Save
        filepath = save_config(config, url, output_dir)
        return filepath
    else:
        print(f"   ℹ️  No business listings detected on this page")
        print(f"      Notes: {config.get('notes', 'None')}")
        return None


def main():
    parser = argparse.ArgumentParser(description="DealLedger Scraper Builder Agent")
    parser.add_argument("url", nargs="?", help="Broker URL to analyze")
    parser.add_argument("--batch", help="File with URLs (one per line)")
    parser.add_argument("--output", default="data/scraper_configs", help="Output directory for configs")
    
    args = parser.parse_args()
    
    if args.batch:
        with open(args.batch) as f:
            urls = [line.strip() for line in f if line.strip() and line.strip().startswith("http")]
        
        print(f"🔨 Building scrapers for {len(urls)} URLs...")
        results = []
        for url in urls:
            result = build_scraper(url, args.output)
            results.append({"url": url, "config": result})
        
        success = sum(1 for r in results if r["config"])
        print(f"\n{'='*60}")
        print(f"BATCH COMPLETE: {success}/{len(urls)} successful")
        print(f"{'='*60}")
    
    elif args.url:
        build_scraper(args.url, args.output)
    
    else:
        print("Provide a URL or --batch file")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
