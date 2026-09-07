/* ============================================================
   DealLedger — homepage DOM integration
   Drop-in patch for public/index.html

   Adds the anchored DOM clock to the market pulse table.
   Reads the v_listing_dom view (live, anon-readable).
   ============================================================ */


/* ------------------------------------------------------------
   1. ADD near the other globals (around line 381, next to PER_PAGE)
   ------------------------------------------------------------ */

let domByNumber = {};   // listing_number -> {days, tier, display}


/* ------------------------------------------------------------
   2. ADD this function anywhere above loadData()

   Pages the view the same way loadData() pages listings.
   Fails soft: if this errors, the table still renders without
   the clock rather than showing nothing.
   ------------------------------------------------------------ */

async function loadDOM() {
    let from = 0;
    const map = {};
    try {
        while (true) {
            const res = await fetch(
                `${SUPABASE_URL}/rest/v1/v_listing_dom` +
                `?select=listing_number,dom_days,dom_confidence,dom_display` +
                `&limit=${BATCH}&offset=${from}`,
                { headers: { apikey: SUPABASE_ANON,
                             Authorization: `Bearer ${SUPABASE_ANON}` } }
            );
            if (!res.ok) break;
            const batch = await res.json();
            for (const r of batch) {
                map[r.listing_number] = {
                    days:    r.dom_days,          // null when floored
                    tier:    r.dom_confidence,    // high | medium | low | floor
                    display: r.dom_display        // pre-rendered, safe to print
                };
            }
            if (batch.length < BATCH) break;
            from += BATCH;
        }
    } catch (e) {
        console.warn('[DealLedger] DOM clock unavailable:', e);
    }
    domByNumber = map;
}


/* ------------------------------------------------------------
   3. CALL it in loadData(), before the table renders.

   Run it in parallel with the listings fetch so it costs no
   extra wall-clock:

       await Promise.all([ loadDOM(), <existing listings fetch> ]);

   If you'd rather not restructure loadData(), just add
       loadDOM();
   at the top of it — the table will pick the values up on the
   next render pass.
   ------------------------------------------------------------ */


/* ------------------------------------------------------------
   4. REPLACE the DOM badge helper (currently around line 415).

   The old one read l.days_on_market, which is the marketplace's
   own field and is unpopulated on most rows. This reads the
   anchored estimate instead, and never prints a bare number for
   a listing we cannot date.
   ------------------------------------------------------------ */

function domBadge(listingNumber) {
    const d = domByNumber[listingNumber];

    // No estimate at all — say nothing rather than guess.
    if (!d) return '<span class="dom-badge dom-normal">—</span>';

    // Older than our earliest anchor: floor only, never a number.
    if (d.days === null || d.days === undefined) {
        return `<span class="dom-badge dom-zombie" title="${d.display}">2yr+</span>`;
    }

    const days = d.days;
    let cls = 'dom-normal';
    if (days <= 30)       cls = 'dom-fresh';
    else if (days <= 90)  cls = 'dom-normal';
    else if (days <= 180) cls = 'dom-stale';
    else if (days <= 365) cls = 'dom-hot';
    else                  cls = 'dom-zombie';

    // Mark weak estimates so nobody quotes them as precise.
    const qualifier = (d.tier === 'low') ? '~' : '';
    const tip = `Estimated from BizBuySell listing sequence · ${d.tier} confidence`;

    return `<span class="dom-badge ${cls}" title="${tip}">${qualifier}${days}d</span>`;
}


/* ------------------------------------------------------------
   5. UPDATE the row renderer (around line 530).

   Was:  const dom = l.days_on_market;
   Use:  domBadge(l.listing_number)
   ------------------------------------------------------------ */


/* ------------------------------------------------------------
   6. UPDATE the DOM filter buttons (around line 463).

   Was:  const dom = l.days_on_market || 0;

   The `|| 0` is what made every undated listing look brand new.
   Undated rows should be excluded from a DOM filter, not
   treated as zero days old.
   ------------------------------------------------------------ */

function domFilterPass(l, domFilter) {
    if (domFilter === 'all') return true;
    const d = domByNumber[l.listing_number];
    if (!d) return false;                       // no estimate → not in any band
    if (d.days === null) return domFilter === 'zombie';  // floored = 2yr+
    const days = d.days;
    if (domFilter === 'fresh')  return days <= 30;
    if (domFilter === 'new')    return days <= 90;
    if (domFilter === 'stale')  return days > 90 && days <= 365;
    if (domFilter === 'zombie') return days > 365;
    return true;
}


/* ------------------------------------------------------------
   7. ADD a method link under the table. This is the whole point —
      the number is only worth anything if it is checkable.
   ------------------------------------------------------------ */

/*
<p style="font-size:12px;color:var(--text-muted);margin-top:12px;">
  Days on market is estimated from the marketplace's own sequential
  listing numbers, calibrated against dated observations.
  <a class="inline" href="/dom-method.html">Method and calibration anchors →</a>
</p>
*/
