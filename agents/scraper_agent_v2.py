#!/usr/bin/env python3
"""
DealLedger Scraper Agent v2 — True Agentic Loop
================================================
Uses Claude's tool use API. Claude decides what to do next.

Loop:
  1. Fetch page
  2. Claude analyzes → calls run_scraper tool
  3. Sees results → if 0, calls fetch_page with different URL or adjusts
  4. If good → calls save_config
  5. Done

Usage:
    python3 scraper_agent_v2.py https://example-broker.com/listings
    python3 scraper_agent_v2.py --batch urls.txt
    export ANTHROPIC_API_KEY=sk-ant-...
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup, Comment

try:
    import anthropic
except ImportError:
    print("Install: pip3 install anthropic --break-system-packages")
    sys.exit(1)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

# ── Tools Claude can call ──────────────────────────────────────────────────────

def tool_fetch_page(url: str) -> dict:
    """Fetch a URL and return cleaned HTML snippet for analysis."""
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg"]):
            tag.decompose()
        for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
            c.extract()

        body = soup.find("body") or soup
        clean = str(body)

        # Keep first 8000 + last 4000 chars
        if len(clean) > 12000:
            clean = clean[:8000] + "\n...[TRUNCATED]...\n" + clean[-4000:]

        return {"ok": True, "url": url, "html": clean, "raw_length": len(html)}
    except Exception as e:
        return {"ok": False, "url": url, "error": str(e)}


def tool_run_scraper(url: str, container_selector: str, fields: dict) -> dict:
    """
    Actually run a scraper config against a live page.
    fields = {title, price, revenue, cashflow, location, link}
    Returns sample records and count.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        containers = soup.select(container_selector)
        if not containers:
            return {"ok": False, "error": f"Selector '{container_selector}' matched 0 elements", "count": 0}

        records = []
        for el in containers[:5]:  # sample first 5
            rec = {}
            for field, sel in fields.items():
                if sel:
                    found = el.select_one(sel)
                    if found:
                        rec[field] = found.get_text(strip=True)[:200]
                    else:
                        rec[field] = None
            records.append(rec)

        # Score quality: how many records have title + price?
        has_title = sum(1 for r in records if r.get("title"))
        has_price = sum(1 for r in records if r.get("price"))

        return {
            "ok": True,
            "count": len(containers),
            "sample": records,
            "quality": {
                "has_title_pct": round(has_title / len(records) * 100) if records else 0,
                "has_price_pct": round(has_price / len(records) * 100) if records else 0,
            }
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "count": 0}


def tool_save_config(url: str, broker_name: str, container_selector: str,
                     fields: dict, pagination_selector: str = None,
                     notes: str = "", confidence: str = "medium") -> dict:
    """Save a validated scraper config to data/scraper_configs/."""
    os.makedirs("data/scraper_configs", exist_ok=True)
    domain = urlparse(url).netloc.replace("www.", "")
    safe = re.sub(r"[^a-zA-Z0-9.-]", "_", domain)
    path = f"data/scraper_configs/{safe}.json"

    config = {
        "broker_name": broker_name,
        "source_url": url,
        "container_selector": container_selector,
        "fields": fields,
        "pagination_next_selector": pagination_selector,
        "confidence": confidence,
        "notes": notes,
        "_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generator": "dealledger-scraper-agent-v2",
        }
    }

    with open(path, "w") as f:
        json.dump(config, f, indent=2)

    return {"ok": True, "path": path, "broker_name": broker_name}


def tool_find_listings_url(base_url: str) -> dict:
    """
    Try to find the 'businesses for sale' or 'listings' page on a broker site
    by checking common URL patterns.
    """
    domain = urlparse(base_url).netloc
    scheme = urlparse(base_url).scheme
    candidates = [
        f"{scheme}://{domain}/businesses-for-sale",
        f"{scheme}://{domain}/listings",
        f"{scheme}://{domain}/business-listings",
        f"{scheme}://{domain}/buy-a-business",
        f"{scheme}://{domain}/available-businesses",
        f"{scheme}://{domain}/businesses",
    ]

    working = []
    for url in candidates:
        try:
            r = requests.head(url, headers={"User-Agent": UA}, timeout=8, allow_redirects=True)
            if r.status_code == 200:
                working.append(url)
        except:
            pass

    return {"candidates": candidates, "working": working, "tried": len(candidates)}


# ── Tool registry ──────────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "fetch_page",
        "description": "Fetch a URL and return cleaned HTML for analysis. Use this first to see the page structure.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Full URL to fetch"}
            },
            "required": ["url"]
        }
    },
    {
        "name": "run_scraper",
        "description": "Test a CSS selector config against a live page. Returns actual scraped records so you can see if it works. Always run this before saving.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL to scrape"},
                "container_selector": {"type": "string", "description": "CSS selector for the repeating listing container"},
                "fields": {
                    "type": "object",
                    "description": "Dict of field_name → CSS selector (relative to container). Keys: title, price, revenue, cashflow, location, link",
                    "properties": {
                        "title": {"type": "string"},
                        "price": {"type": "string"},
                        "revenue": {"type": "string"},
                        "cashflow": {"type": "string"},
                        "location": {"type": "string"},
                        "link": {"type": "string"}
                    }
                }
            },
            "required": ["url", "container_selector", "fields"]
        }
    },
    {
        "name": "save_config",
        "description": "Save a validated scraper config. Only call this after run_scraper confirms the selectors work (count > 0, quality scores > 50%).",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "broker_name": {"type": "string"},
                "container_selector": {"type": "string"},
                "fields": {"type": "object"},
                "pagination_selector": {"type": "string"},
                "notes": {"type": "string"},
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]}
            },
            "required": ["url", "broker_name", "container_selector", "fields"]
        }
    },
    {
        "name": "find_listings_url",
        "description": "If the given URL doesn't have listings, try common URL patterns to find the listings page on the same domain.",
        "input_schema": {
            "type": "object",
            "properties": {
                "base_url": {"type": "string", "description": "The broker's base URL"}
            },
            "required": ["base_url"]
        }
    }
]

SYSTEM_PROMPT = """You are a web scraping agent for DealLedger, an open-source ledger of business-for-sale listings.

Your job: Given a broker website URL, figure out how to scrape their business listings.

Strategy:
1. Call fetch_page to see the HTML structure
2. Identify CSS selectors for listings (container, title, price, location, etc.)
3. Call run_scraper to TEST your selectors — see actual results
4. If count=0 or quality is poor: adjust selectors and retry (up to 3 attempts)
5. If no listings on that page: call find_listings_url to find the right page
6. Only call save_config when run_scraper confirms it works

Be persistent. Brokers use many different HTML patterns. Try multiple selector approaches.
If a site blocks you or has no listings at all, say so clearly and stop.

When done, summarize: broker name, URL, how many listings found, confidence level."""


def dispatch_tool(name: str, inputs: dict) -> str:
    """Call the right Python function and return JSON string."""
    if name == "fetch_page":
        result = tool_fetch_page(inputs["url"])
    elif name == "run_scraper":
        result = tool_run_scraper(inputs["url"], inputs["container_selector"], inputs.get("fields", {}))
    elif name == "save_config":
        result = tool_save_config(
            inputs["url"], inputs["broker_name"], inputs["container_selector"],
            inputs.get("fields", {}), inputs.get("pagination_selector"),
            inputs.get("notes", ""), inputs.get("confidence", "medium")
        )
    elif name == "find_listings_url":
        result = tool_find_listings_url(inputs["base_url"])
    else:
        result = {"error": f"Unknown tool: {name}"}

    return json.dumps(result)


# ── Agent loop ─────────────────────────────────────────────────────────────────

def run_agent(url: str, verbose: bool = True) -> dict:
    """Run the full agentic loop for one URL."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ Set ANTHROPIC_API_KEY")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    messages = [
        {"role": "user", "content": f"Build a scraper for this broker website: {url}"}
    ]

    print(f"\n{'='*60}")
    print(f"🤖 Agent starting: {url}")
    print(f"{'='*60}")

    max_iterations = 10
    iteration = 0
    final_result = {"url": url, "success": False, "config_path": None}

    while iteration < max_iterations:
        iteration += 1

        response = client.messages.create(
            model="claude-sonnet-4-5-20250514",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Collect text output
        for block in response.content:
            if hasattr(block, "text") and block.text:
                if verbose:
                    print(f"\n💭 {block.text}")

        # Check stop reason
        if response.stop_reason == "end_turn":
            print(f"\n✅ Agent finished after {iteration} iterations")
            break

        if response.stop_reason != "tool_use":
            print(f"\n⚠️  Unexpected stop: {response.stop_reason}")
            break

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue

            tool_name = block.name
            tool_input = block.input

            print(f"\n🔧 Tool: {tool_name}")
            if verbose and tool_name != "fetch_page":  # fetch_page output is too long
                print(f"   Input: {json.dumps(tool_input, indent=2)[:300]}")

            result_str = dispatch_tool(tool_name, tool_input)
            result_data = json.loads(result_str)

            if verbose:
                if tool_name == "fetch_page":
                    print(f"   Result: fetched {result_data.get('raw_length', 0):,} bytes → ok={result_data.get('ok')}")
                elif tool_name == "run_scraper":
                    print(f"   Result: count={result_data.get('count', 0)}, quality={result_data.get('quality', {})}")
                    if result_data.get("sample"):
                        print(f"   Sample[0]: {json.dumps(result_data['sample'][0])[:200]}")
                else:
                    print(f"   Result: {result_str[:300]}")

            # Track save_config calls
            if tool_name == "save_config" and result_data.get("ok"):
                final_result["success"] = True
                final_result["config_path"] = result_data.get("path")
                final_result["broker_name"] = result_data.get("broker_name")

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result_str,
            })

        # Append assistant turn + tool results
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

    if iteration >= max_iterations:
        print(f"\n⚠️  Hit iteration limit ({max_iterations})")

    return final_result


def main():
    parser = argparse.ArgumentParser(description="DealLedger Scraper Agent v2")
    parser.add_argument("url", nargs="?", help="Broker URL")
    parser.add_argument("--batch", help="File with URLs (one per line)")
    parser.add_argument("--quiet", action="store_true", help="Less output")
    args = parser.parse_args()

    verbose = not args.quiet

    if args.batch:
        with open(args.batch) as f:
            urls = [l.strip() for l in f if l.strip().startswith("http")]
        print(f"🔨 Running agent on {len(urls)} URLs...")
        results = []
        for url in urls:
            r = run_agent(url, verbose=verbose)
            results.append(r)
        success = sum(1 for r in results if r["success"])
        print(f"\n{'='*60}")
        print(f"BATCH: {success}/{len(urls)} successful")
        for r in results:
            status = "✅" if r["success"] else "❌"
            print(f"  {status} {r['url']} → {r.get('config_path', 'no config')}")
    elif args.url:
        run_agent(args.url, verbose=verbose)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
