#!/usr/bin/env python3
"""
DealLedger Scraper Agent v3 — With Specialized Scraper Routing
Uses Claude tool use API. Routes big brokers to specialized scrapers,
handles the long tail with the agentic loop.
"""
import argparse, json, os, re, sys
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup, Comment
try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except:
    HAS_CFFI = False

try:
    import anthropic
except ImportError:
    print("Install: pip3 install anthropic --break-system-packages")
    sys.exit(1)

# Import specialized scrapers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scrapers'))
try:
    from specialized_scrapers import scrape_specialized_broker, get_specialized_broker_names
    HAS_SPECIALIZED = True
except ImportError:
    HAS_SPECIALIZED = False
    print("⚠️  specialized_scrapers not found - will use generic agent only")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

# ── Tool implementations ───────────────────────────────────────────────────────

def tool_check_specialized(url: str) -> dict:
    """Check if this URL belongs to a known broker with a specialized scraper."""
    if not HAS_SPECIALIZED:
        return {"is_specialized": False, "reason": "specialized_scrapers module not available"}
    
    domain = urlparse(url).netloc.lower().replace("www.", "")
    name = domain.split(".")[0]
    
    # Match against known brokers
    broker = {"account": "agent-run", "name": name, "url": url}
    
    known_domains = {
        "murphybusiness.com": "Murphy Business",
        "hedgestone.com": "Hedgestone Business Advisors", 
        "tworld.com": "Transworld Business Advisors",
        "transworld.net": "Transworld Business Advisors",
        "twbusinessadvisors.com": "Transworld Business Advisors",
        "sunbeltnetwork.com": "Sunbelt Business Brokers",
        "vrbusinessbrokers.com": "VR Business Brokers",
        "vrbbusa.com": "VR Business Brokers",
        "fcbb.com": "First Choice Business Brokers",
        "linkbusiness.com": "Link Business",
        "execbb.com": "Executive Business Brokers",
    }
    
    broker_name = known_domains.get(domain)
    if broker_name:
        return {
            "is_specialized": True,
            "broker_name": broker_name,
            "domain": domain,
            "message": f"Use run_specialized_scraper for {broker_name} — it has a dedicated scraper that handles JS/API/pagination."
        }
    
    return {
        "is_specialized": False,
        "domain": domain,
        "known_brokers": list(known_domains.values()),
        "message": "No specialized scraper. Use fetch_page → run_scraper → save_config flow."
    }


def tool_run_specialized_scraper(url: str, max_pages: int = 5) -> dict:
    """Run a specialized scraper for known big brokers. Returns listing count and sample."""
    if not HAS_SPECIALIZED:
        return {"ok": False, "error": "specialized_scrapers module not available"}
    
    domain = urlparse(url).netloc.lower().replace("www.", "")
    broker = {"account": "agent-run", "name": domain, "url": url}
    
    try:
        print(f"   🔩 Running specialized scraper for {domain}...")
        listings = scrape_specialized_broker(broker, verbose=True)
        
        if listings is None:
            return {"ok": False, "error": "No specialized scraper matched this URL"}
        
        # Save results
        os.makedirs("data/listings", exist_ok=True)
        safe = re.sub(r"[^a-zA-Z0-9.-]", "_", domain)
        path = f"data/listings/{safe}_{datetime.now().strftime('%Y%m%d')}.json"
        with open(path, "w") as f:
            json.dump(listings, f, indent=2)
        
        sample = listings[:3] if listings else []
        return {
            "ok": True,
            "count": len(listings),
            "saved_to": path,
            "sample": sample,
            "with_price": sum(1 for l in listings if l.get("price")),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def tool_fetch_page(url: str) -> dict:
    try:
        resp = cffi_requests.get(url, impersonate="chrome120", timeout=15) if HAS_CFFI else requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["script","style","noscript","iframe","svg"]):
            tag.decompose()
        for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
            c.extract()
        body = soup.find("body") or soup
        clean = str(body)
        if len(clean) > 12000:
            clean = clean[:8000] + "\n...[TRUNCATED]...\n" + clean[-4000:]
        return {"ok": True, "url": url, "html": clean, "raw_length": len(resp.text)}
    except Exception as e:
        return {"ok": False, "url": url, "error": str(e)}


def tool_run_scraper(url: str, container_selector: str, fields: dict) -> dict:
    try:
        resp = cffi_requests.get(url, impersonate="chrome120", timeout=15) if HAS_CFFI else requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        containers = soup.select(container_selector)
        if not containers:
            return {"ok": False, "error": f"Selector '{container_selector}' matched 0 elements", "count": 0}
        records = []
        for el in containers[:5]:
            rec = {}
            for field, sel in fields.items():
                if sel:
                    found = el.select_one(sel)
                    rec[field] = found.get_text(strip=True)[:200] if found else None
            records.append(rec)
        has_title = sum(1 for r in records if r.get("title"))
        has_price = sum(1 for r in records if r.get("price"))
        return {
            "ok": True, "count": len(containers), "sample": records,
            "quality": {
                "has_title_pct": round(has_title/len(records)*100) if records else 0,
                "has_price_pct": round(has_price/len(records)*100) if records else 0,
            }
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "count": 0}


def tool_save_config(url: str, broker_name: str, container_selector: str, fields: dict,
                     pagination_selector: str = None, notes: str = "", confidence: str = "medium") -> dict:
    os.makedirs("data/scraper_configs", exist_ok=True)
    domain = urlparse(url).netloc.replace("www.","")
    safe = re.sub(r"[^a-zA-Z0-9.-]","_", domain)
    path = f"data/scraper_configs/{safe}.json"
    config = {
        "broker_name": broker_name, "source_url": url,
        "container_selector": container_selector, "fields": fields,
        "pagination_next_selector": pagination_selector,
        "confidence": confidence, "notes": notes,
        "_meta": {"generated_at": datetime.now(timezone.utc).isoformat(), "generator": "dealledger-scraper-agent-v3"}
    }
    with open(path,"w") as f:
        json.dump(config, f, indent=2)
    return {"ok": True, "path": path, "broker_name": broker_name}


def tool_find_listings_url(base_url: str) -> dict:
    domain = urlparse(base_url).netloc
    scheme = urlparse(base_url).scheme
    candidates = [
        f"{scheme}://{domain}/businesses-for-sale",
        f"{scheme}://{domain}/listings",
        f"{scheme}://{domain}/business-listings",
        f"{scheme}://{domain}/buy-a-business",
        f"{scheme}://{domain}/available-businesses",
    ]
    working = []
    for url in candidates:
        try:
            r = requests.head(url, headers={"User-Agent": UA}, timeout=8, allow_redirects=True)
            if r.status_code == 200:
                working.append(url)
        except:
            pass
    return {"candidates": candidates, "working": working}


# ── Tool definitions for Claude ────────────────────────────────────────────────

TOOLS = [
    {
        "name": "check_specialized",
        "description": "ALWAYS call this first. Checks if the URL belongs to a known broker with a specialized scraper (Murphy, Transworld, Sunbelt, FCBB, etc.). If yes, use run_specialized_scraper instead of the generic flow.",
        "input_schema": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}
    },
    {
        "name": "run_specialized_scraper",
        "description": "Run a pre-built specialized scraper for known big brokers. Handles JS rendering, APIs, pagination automatically. Returns actual listing count and sample data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "max_pages": {"type": "integer", "description": "Max pages to scrape (default 5 for testing)"}
            },
            "required": ["url"]
        }
    },
    {
        "name": "fetch_page",
        "description": "Fetch a URL and return cleaned HTML. Use for unknown brokers after check_specialized returns is_specialized=false.",
        "input_schema": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}
    },
    {
        "name": "run_scraper",
        "description": "Test CSS selectors against a live page. Returns actual records. Always run before save_config.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "container_selector": {"type": "string"},
                "fields": {"type": "object"}
            },
            "required": ["url","container_selector","fields"]
        }
    },
    {
        "name": "save_config",
        "description": "Save a validated scraper config. Only call after run_scraper confirms count>0.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "broker_name": {"type": "string"},
                "container_selector": {"type": "string"},
                "fields": {"type": "object"},
                "pagination_selector": {"type": "string"},
                "notes": {"type": "string"},
                "confidence": {"type": "string", "enum": ["high","medium","low"]}
            },
            "required": ["url","broker_name","container_selector","fields"]
        }
    },
    {
        "name": "find_listings_url",
        "description": "Try common URL patterns to find the listings page on a broker site.",
        "input_schema": {"type": "object", "properties": {"base_url": {"type": "string"}}, "required": ["base_url"]}
    }
]

SYSTEM = """You are a web scraping agent for DealLedger, an open-source ledger of business-for-sale listings.

ALWAYS start by calling check_specialized(url).

If is_specialized=true:
  → Call run_specialized_scraper(url) — it handles JS, APIs, and pagination automatically
  → Report results and you're done

If is_specialized=false (unknown/independent broker):
  1. Call fetch_page to see HTML structure
  2. Identify CSS selectors for listing containers, title, price, location
  3. Call run_scraper to TEST selectors with real data
  4. If count=0: adjust selectors and retry (up to 3 times), or try find_listings_url
  5. If results good (count>0, quality>50%): call save_config
  6. If JS-rendered (empty containers): note that Playwright/Selenium needed

Always summarize: broker name, listing count, confidence, what worked."""


def dispatch(name, inputs):
    if name == "check_specialized": result = tool_check_specialized(inputs["url"])
    elif name == "run_specialized_scraper": result = tool_run_specialized_scraper(inputs["url"], inputs.get("max_pages", 5))
    elif name == "fetch_page": result = tool_fetch_page(inputs["url"])
    elif name == "run_scraper": result = tool_run_scraper(inputs["url"], inputs["container_selector"], inputs.get("fields", {}))
    elif name == "save_config": result = tool_save_config(inputs["url"], inputs["broker_name"], inputs["container_selector"], inputs.get("fields",{}), inputs.get("pagination_selector"), inputs.get("notes",""), inputs.get("confidence","medium"))
    elif name == "find_listings_url": result = tool_find_listings_url(inputs["base_url"])
    else: result = {"error": f"Unknown tool: {name}"}
    return json.dumps(result)


def run_agent(url, verbose=True):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    messages = [{"role": "user", "content": f"Build a scraper for: {url}"}]
    print(f"\n{'='*60}\n🤖 Agent v3: {url}\n{'='*60}")
    final = {"url": url, "success": False}

    for i in range(10):
        resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=4096,
            system=SYSTEM,
            tools=TOOLS,
            messages=messages
        )
        for block in resp.content:
            if hasattr(block,"text") and block.text:
                print(f"\n💭 {block.text}")
        if resp.stop_reason == "end_turn":
            print(f"\n✅ Done ({i+1} iterations)")
            break
        if resp.stop_reason != "tool_use":
            break
        tool_results = []
        for block in resp.content:
            if block.type != "tool_use": continue
            print(f"\n🔧 {block.name}: {json.dumps(block.input)[:150]}")
            result_str = dispatch(block.name, block.input)
            result_data = json.loads(result_str)
            if block.name == "fetch_page":
                print(f"   → {result_data.get('raw_length',0):,} bytes ok={result_data.get('ok')}")
            elif block.name in ("run_scraper", "run_specialized_scraper"):
                print(f"   → count={result_data.get('count',0)}")
                if result_data.get("sample"):
                    print(f"   → sample[0]: {json.dumps(result_data['sample'][0])[:150]}")
            else:
                print(f"   → {result_str[:200]}")
            if block.name in ("save_config","run_specialized_scraper") and result_data.get("ok"):
                final["success"] = True
                final["count"] = result_data.get("count", 0)
            tool_results.append({"type":"tool_result","tool_use_id":block.id,"content":result_str})
        messages.append({"role":"assistant","content":resp.content})
        messages.append({"role":"user","content":tool_results})
    return final


    # Load URLs from CSV - handle both plain URL files and CSV with columns
    urls = []
    with open(csv_path) as f:
        content = f.read(500)
    
    # Detect if it's a CSV with headers or plain URL list
    if ',' in content.split('\n')[0]:
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Try common URL column names
                url = row.get('listings_url') or row.get('url') or row.get('URL') or row.get('website') or list(row.values())[0]
                if url and url.strip().startswith('http'):
                    urls.append(url.strip())
    else:
        with open(csv_path) as f:
            urls = [l.strip() for l in f if l.strip().startswith('http')]

    total = len(urls)
    print(f"\n🚀 Starting parallel batch: {total} URLs, {workers} workers")
    print(f"   Est. time: {total * 45 // workers // 60} min at ~45s per URL\n")

    results = []
    completed = 0
    success = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(run_agent, url, False): url for url in urls}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            try:
                result = future.result()
                if result["success"]:
                    success += 1
                    print(f"✅ [{completed}/{total}] {url[:60]} → {result.get('count',0)} listings")
                else:
                    print(f"❌ [{completed}/{total}] {url[:60]}")
            except Exception as e:
                print(f"💥 [{completed}/{total}] {url[:60]} → ERROR: {e}")
                result = {"url": url, "success": False}
            
            results.append(result)

            # Progress every 25 brokers
            if completed % 25 == 0:
                print(f"\n📊 Progress: {completed}/{total} done | {success} successful ({round(success/completed*100)}%)\n")

    # Final report
    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE: {success}/{total} successful ({round(success/total*100)}%)")
    print(f"Configs saved to: data/scraper_configs/")
    
    # Save summary

    summary_path = f"data/batch_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    os.makedirs("data", exist_ok=True)
    with open(summary_path, "w") as f:
        json.dump({
            "total": total, "success": success,
            "rate": round(success/total*100),
            "results": results
        }, f, indent=2)
    print(f"Summary saved to: {summary_path}")
    return results


def run_batch_parallel(csv_path, workers=5):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import csv

    urls = []
    with open(csv_path) as f:
        first = f.read(500)
    
    if ',' in first.split('\n')[0]:
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('listings_url') or row.get('url') or row.get('URL') or row.get('website') or list(row.values())[0]
                if url and url.strip().startswith('http'):
                    urls.append(url.strip())
    else:
        with open(csv_path) as f:
            urls = [l.strip() for l in f if l.strip().startswith('http')]

    total = len(urls)
    print(f"\n🚀 Parallel batch: {total} URLs, {workers} workers")
    print(f"   Est. time: {total * 45 // workers // 60} min\n")

    results = []
    completed = 0
    success = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(run_agent, url, False): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            try:
                result = future.result()
                if result["success"]:
                    success += 1
                    print(f"✅ [{completed}/{total}] {url[:60]} → {result.get('count',0)} listings")
                else:
                    print(f"❌ [{completed}/{total}] {url[:60]}")
            except Exception as e:
                print(f"💥 [{completed}/{total}] {url[:60]} ERROR: {e}")
                result = {"url": url, "success": False}
            results.append(result)
            if completed % 25 == 0:
                print(f"\n📊 {completed}/{total} | {success} successful ({round(success/completed*100)}%)\n")

    print(f"\n{'='*60}")
    print(f"COMPLETE: {success}/{total} ({round(success/total*100)}%)")
    summary = f"data/batch_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    os.makedirs("data", exist_ok=True)
    with open(summary, "w") as f:
        json.dump({"total": total, "success": success, "results": results}, f, indent=2)
    print(f"Summary: {summary}")
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DealLedger Scraper Agent v3")
    parser.add_argument("url", nargs="?")
    parser.add_argument("--batch", help="CSV or URL file")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers")
    args = parser.parse_args()
    if args.batch:
        run_batch_parallel(args.batch, workers=args.workers)
    elif args.url:
        run_agent(args.url)
    else:
        parser.print_help()
