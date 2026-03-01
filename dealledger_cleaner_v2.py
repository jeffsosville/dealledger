#!/usr/bin/env python3
"""
DealLedger Listing Cleaner v2 - Fast Mode

Usage:
  # Test
  python3 dealledger_cleaner_v2.py --input data/all_listings.csv --output data/clean/listings_clean.csv --fast --test

  # Full fast run (~30 min, ~$3)
  python3 dealledger_cleaner_v2.py --input data/all_listings.csv --output data/clean/listings_clean.csv --fast

  # Resume
  python3 dealledger_cleaner_v2.py --input data/all_listings.csv --output data/clean/listings_clean.csv --fast --resume
"""

import os, json, time, argparse, asyncio
import pandas as pd
from tqdm import tqdm
from anthropic import Anthropic

client = Anthropic()

def build_fast_prompt(row):
    return (
        "You are a data extraction agent for DealLedger, a public index of small businesses for sale.\n\n"
        "Using ONLY the listing info below, extract what you can and return ONLY valid JSON.\n\n"
        "Title: " + str(row.get("title", "")) + "\n"
        "Broker: " + str(row.get("broker_name", "")) + "\n"
        "URL: " + str(row.get("source_url", "")) + "\n"
        "asking_price: " + str(row.get("asking_price", "")) + "\n"
        "cash_flow: " + str(row.get("cash_flow", "")) + "\n"
        "revenue: " + str(row.get("revenue", "")) + "\n"
        "city: " + str(row.get("city", "")) + "\n"
        "state: " + str(row.get("state", "")) + "\n\n"
        "Return a JSON object with exactly these keys:\n"
        "asking_price (number or null), revenue (number or null), cash_flow (number or null),\n"
        "city (string or null), state (2-letter code or null),\n"
        "vertical (one of: cleaning, vending, landscape, hvac, food, retail, healthcare, auto, construction, other, or null),\n"
        "business_type (short label or null),\n"
        "is_real_listing (true or false),\n"
        "reject_reason (string or null),\n"
        "confidence (0-100)\n\n"
        "Rules:\n"
        "- Infer vertical from title keywords\n"
        "- Monetary values as plain numbers only (e.g. 1200000 not $1.2M)\n"
        "- is_real_listing=false for franchise opps, job posts, or real estate listings\n"
        "- Return JSON only, no other text"
    )

def build_full_prompt(row, page_text):
    return (
        "You are a data extraction agent for DealLedger, a public index of small businesses for sale.\n\n"
        "Extract missing fields from this listing. Return ONLY valid JSON.\n\n"
        "Current data:\n"
        "title: " + str(row.get("title", "")) + "\n"
        "asking_price: " + str(row.get("asking_price", "")) + "\n"
        "cash_flow: " + str(row.get("cash_flow", "")) + "\n"
        "revenue: " + str(row.get("revenue", "")) + "\n"
        "city: " + str(row.get("city", "")) + "\n"
        "state: " + str(row.get("state", "")) + "\n"
        "vertical: " + str(row.get("vertical", "")) + "\n"
        "business_type: " + str(row.get("business_type", "")) + "\n\n"
        "Page content:\n" + page_text[:4000] + "\n\n"
        "Return a JSON object with exactly these keys:\n"
        "asking_price (number or null), revenue (number or null), cash_flow (number or null),\n"
        "city (string or null), state (2-letter code or null),\n"
        "vertical (one of: cleaning, vending, landscape, hvac, food, retail, healthcare, auto, construction, other, or null),\n"
        "business_type (short label or null),\n"
        "is_real_listing (true or false),\n"
        "reject_reason (string or null),\n"
        "confidence (0-100)\n\n"
        "- Monetary values as plain numbers only\n"
        "- is_real_listing=false for franchise opps, job posts, real estate only\n"
        "- Return JSON only"
    )

def call_claude(prompt, retries=2):
    for attempt in range(retries + 1):
        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            text = resp.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            return json.loads(text)
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return {"confidence": 0, "is_real_listing": None, "_error": str(e)}

async def fetch_page_text(url, timeout=15000):
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": "Mozilla/5.0"})
            try:
                await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)
                text = await page.evaluate("""() => {
                    ['nav','footer','script','style','header'].forEach(t => {
                        document.querySelectorAll(t).forEach(e => e.remove());
                    });
                    return document.body.innerText.slice(0, 5000);
                }""")
                return text
            except Exception as e:
                return f"FETCH_ERROR: {e}"
            finally:
                await browser.close()
    except ImportError:
        return "FETCH_ERROR: playwright not installed"

def fetch_page_sync(url):
    return asyncio.run(fetch_page_text(url))

def merge_row(original, extracted):
    result = original.copy()
    for field in ["asking_price", "revenue", "cash_flow"]:
        val = original.get(field)
        if (val is None or (isinstance(val, float) and pd.isna(val))) and extracted.get(field) is not None:
            result[field] = extracted[field]
    for field in ["city", "state", "vertical", "business_type"]:
        val = original.get(field)
        if not val or (isinstance(val, float) and pd.isna(val)):
            if extracted.get(field):
                result[field] = extracted[field]
    result["confidence"] = extracted.get("confidence", 0)
    result["is_real_listing"] = extracted.get("is_real_listing", True)
    result["reject_reason"] = extracted.get("reject_reason", None)
    return result

def clean_listings(input_path, output_path, test_mode=False, resume=False, fast_mode=False):
    print(f"Loading {input_path}...")
    df = pd.read_csv(input_path)

    if test_mode:
        df = df.head(10)
        print(f"TEST MODE: processing {len(df)} listings")
    else:
        mode = "FAST (title-only)" if fast_mode else "FULL (fetching URLs)"
        print(f"{mode} mode — {len(df)} listings")

    already_done = set()
    if resume and os.path.exists(output_path):
        done_df = pd.read_csv(output_path)
        already_done = set(done_df["id"].astype(str).tolist())
        df = df[~df["id"].astype(str).isin(already_done)]
        print(f"Resuming — {len(already_done)} done, {len(df)} remaining")

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    results, rejected, fetch_errors = [], [], []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Cleaning"):
        row_dict = row.to_dict()

        if fast_mode:
            prompt = build_fast_prompt(row_dict)
            extracted = call_claude(prompt)
        else:
            url = row_dict.get("source_url", "")
            page_text = fetch_page_sync(url) if url else "NO_URL"
            if page_text.startswith("FETCH_ERROR") or page_text == "NO_URL":
                fetch_errors.append({**row_dict, "_fetch_error": page_text})
                page_text = f"Title: {row_dict.get('title', '')} Broker: {row_dict.get('broker_name', '')}"
            prompt = build_full_prompt(row_dict, page_text)
            extracted = call_claude(prompt)

        merged = merge_row(row_dict, extracted)

        if merged.get("is_real_listing") is False:
            rejected.append(merged)
        else:
            results.append(merged)

        time.sleep(0.05 if fast_mode else 0.15)

    clean_df = pd.DataFrame(results)
    if resume and os.path.exists(output_path):
        existing = pd.read_csv(output_path)
        clean_df = pd.concat([existing, clean_df], ignore_index=True)
    clean_df.to_csv(output_path, index=False)

    if rejected:
        pd.DataFrame(rejected).to_csv(output_path.replace(".csv", "_rejected.csv"), index=False)
    if fetch_errors:
        pd.DataFrame(fetch_errors).to_csv(output_path.replace(".csv", "_fetch_errors.csv"), index=False)

    total = len(df)
    print(f"\n{'='*55}")
    print(f"DealLedger Cleaner v2 — Complete")
    print(f"{'='*55}")
    print(f"Clean:    {len(results)} ({len(results)/total*100:.1f}%)")
    print(f"Rejected: {len(rejected)}")
    if fetch_errors:
        print(f"Errors:   {len(fetch_errors)}")
    print(f"\nOutput: {output_path}")

    if results:
        rdf = pd.DataFrame(results)
        print(f"\nField fill rate:")
        for col in ["asking_price", "revenue", "cash_flow", "city", "state", "vertical", "business_type"]:
            if col in rdf.columns:
                filled = rdf[col].notna().sum()
                print(f"  {col:20s}: {filled:5d}/{len(rdf)} ({filled/len(rdf)*100:.0f}%)")
        conf = [r.get("confidence", 0) for r in results if r.get("confidence")]
        if conf:
            print(f"\n  Avg confidence: {sum(conf)/len(conf):.0f}/100")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--fast", action="store_true")
    args = parser.parse_args()
    clean_listings(args.input, args.output, test_mode=args.test, resume=args.resume, fast_mode=args.fast)
