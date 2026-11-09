"""Source check: public site copy makes no aggregate days-on-market claims, carries no
hard-coded broker counts, and doesn't link the pages unlinked on 2026-11-09 (kept in
drafts/ for rewriting). Run: python3 tests/test_public_copy.py (no network)."""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")

# Everything a visitor can load. drafts/ is deliberately excluded.
SCAN = ["public", "lib", "pages", "components"]
EXT = (".html", ".ts", ".tsx", ".xml")

FORBIDDEN = {
    r'href="/why(\.html)?"': "/why is unlinked until rewritten (drafts/writing/)",
    r"no-reason-to-fix\"|/writing/no-reason-to-fix<": "essay is unlinked until rewritten (drafts/writing/)",
    r'href="/pulse(\.html)?"|dealledger\.org/pulse': "Pulse page is unlinked until rebuilt from verified data",
    r"(?i)\b(true|real) days on market": "no days-on-market claim; listing age uses the evidence hierarchy",
    r"(?i)days on market reflect": "no days-on-market claim; listing age uses the evidence hierarchy",
    r"(?i)median (dom|days)": "no aggregate listing-age figure (DOM hold)",
    r"(?i)on our record (at least )?90\+? days": "no aggregate listing-age figure (DOM hold)",
    r"DL:STALEPCT": "aggregate 90+ day share removed (DOM hold)",
    r"\b1,700\+?\s*(broker|individual)": "broker counts come from public_stats, never hard-coded",
    r"\b33,000\+": "listing counts come from public_stats, never hard-coded",
}

# Lines that are allowed to mention a pattern (comments explaining the rule).
ALLOW = re.compile(r"Unlinked 2026-11-09|No percentile or 'slowest'")


def main() -> int:
    bad = []
    for top in SCAN:
        for dirpath, _, files in os.walk(os.path.join(ROOT, top)):
            for name in files:
                if not name.endswith(EXT):
                    continue
                path = os.path.join(dirpath, name)
                with open(path, encoding="utf-8", errors="replace") as fh:
                    for n, line in enumerate(fh, 1):
                        if ALLOW.search(line):
                            continue
                        for pat, why in FORBIDDEN.items():
                            if re.search(pat, line):
                                rel = os.path.relpath(path, ROOT)
                                bad.append(f"{rel}:{n}: {why}\n    {line.strip()[:160]}")
    if bad:
        print("Public copy check FAILED:\n" + "\n".join(bad))
        return 1
    print("Public copy check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
