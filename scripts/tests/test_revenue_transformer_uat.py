#!/usr/bin/env python3
"""
scripts/tests/test_revenue_transformer_uat.py

Revenue Transformer — UAT End-to-End Verification (Step 4)
===========================================================

Runs the full revenue pipeline against the real UAT analytics backend and
validates that each stage works:

    settings.ANALYTICS_BACKEND_URL
           │
           ▼
    AnalyticsClient  (auth headers pulled from settings)
           │
           ▼  6 × HTTP POST to /api/v1/leo/revenue/*
           │
    RevenueExtractor.run()   ← the transformer
           │
           ▼
    structured docs (one per period / staff / location / etc.)

What it checks:
  - All 6 endpoints return without error
  - tenant_id on every doc matches the requested business_id
  - domain is 'revenue' on every doc
  - Every doc has non-empty text (otherwise embeddings will be useless)
  - trend_slope is computed and consistent across monthly_summary docs
  - Optional tenant-isolation run with biz 42 (known empty) to confirm
    no cross-tenant leaks

Run (from project root):
    python scripts/tests/test_revenue_transformer_uat.py
    python scripts/tests/test_revenue_transformer_uat.py --show-sample
    python scripts/tests/test_revenue_transformer_uat.py --tenant-isolation
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import date
from pathlib import Path

# ── Make `app` and `etl` importable whether run via `python scripts/tests/...`
#    or `PYTHONPATH=. python scripts/tests/...`. Walk up from this file until
#    we find the project root (the dir containing the `app` package), and
#    prepend it to sys.path.
_HERE = Path(__file__).resolve()
for _candidate in (_HERE.parent, *_HERE.parents):
    if (_candidate / "app").is_dir():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

# Auto-load .env at project root so ANALYTICS_BACKEND_URL etc. are present.
# Silent no-op if python-dotenv is missing — settings will then fail loudly
# if required vars aren't set in the shell, which is the behavior we want.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ImportError:
    pass

from app.core.config import settings
from app.services.analytics_client import AnalyticsClient
from etl.transforms.revenue_etl import RevenueExtractor


# ── ANSI colors ───────────────────────────────────────────────────────────────
class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN = "\033[31m", "\033[32m", "\033[33m", "\033[36m"


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


# ── Runner ────────────────────────────────────────────────────────────────────

async def run_for_business(business_id: int, start: date, end: date) -> list[dict]:
    """Instantiate client + transformer and run against UAT."""
    client = AnalyticsClient(base_url=str(settings.ANALYTICS_BACKEND_URL))
    extractor = RevenueExtractor(client=client)
    return await extractor.run(
        business_id=business_id,
        start_date=start,
        end_date=end,
    )


# ── Validation ────────────────────────────────────────────────────────────────

def summarize(docs: list[dict], business_id: int) -> tuple[int, int]:
    """Print a concise report. Returns (passed_checks, total_checks)."""
    if not docs:
        print(c("  → 0 docs produced (biz has no revenue data in this range)", C.YELLOW))
        return (0, 1)

    by_type = Counter(d.get("doc_type") for d in docs)
    print(c(f"  → {len(docs)} docs produced:", C.DIM))
    for dt, n in by_type.most_common():
        print(c(f"       {dt:25s} {n:>3} docs", C.DIM))

    checks: list[tuple[str, bool, str | None]] = []

    # tenant_id
    mismatches = [d for d in docs if d.get("tenant_id") != business_id]
    checks.append((
        f"tenant_id == {business_id} on every doc",
        not mismatches,
        f"{len(mismatches)} docs had wrong tenant_id" if mismatches else None,
    ))

    # domain
    wrong_domain = [d for d in docs if d.get("domain") != "revenue"]
    checks.append((
        "domain == 'revenue' on every doc",
        not wrong_domain,
        f"{len(wrong_domain)} docs had wrong domain" if wrong_domain else None,
    ))

    # text field
    no_text = [d for d in docs if not (d.get("text") or "").strip()]
    checks.append((
        "Non-empty text on every doc (embedding input)",
        not no_text,
        f"{len(no_text)} docs had empty/missing text" if no_text else None,
    ))

    # monthly_summary: trend_slope computed + consistent
    monthly = [d for d in docs if d.get("doc_type") == "monthly_summary"]
    if monthly:
        missing_slope = [d for d in monthly if d.get("trend_slope") is None]
        checks.append((
            f"trend_slope computed on all monthly_summary docs ({len(monthly)})",
            not missing_slope,
            f"{len(missing_slope)} missing" if missing_slope else None,
        ))
        slopes = {d.get("trend_slope") for d in monthly}
        checks.append((
            "trend_slope consistent across all monthly docs (same calc)",
            len(slopes) == 1,
            f"found {len(slopes)} different slopes: {slopes}" if len(slopes) > 1 else None,
        ))

    # staff_revenue: revenue_rank populated
    staff = [d for d in docs if d.get("doc_type") == "staff_revenue"]
    if staff:
        missing_rank = [d for d in staff if d.get("revenue_rank") is None]
        checks.append((
            f"revenue_rank set on all staff_revenue docs ({len(staff)})",
            not missing_rank,
            f"{len(missing_rank)} missing" if missing_rank else None,
        ))

    # Print check results
    print()
    print(c("  Checks:", C.BOLD))
    passed = 0
    for label, ok, detail in checks:
        mark = c("✅", C.GREEN) if ok else c("❌", C.RED)
        print(f"    {mark} {label}")
        if detail:
            print(c(f"       → {detail}", C.RED))
        if ok:
            passed += 1

    return (passed, len(checks))


def print_sample_docs(docs: list[dict]) -> None:
    """Print one sample doc of each type (truncated text)."""
    print()
    print(c("  Sample doc per type:", C.BOLD))
    seen: set[str] = set()
    for d in docs:
        dt = d.get("doc_type")
        if dt in seen:
            continue
        seen.add(dt)
        text = d.get("text") or ""
        if len(text) > 180:
            text = text[:180] + " …"
        print(c(f"    [{dt}]", C.CYAN))
        print(c(f"      {text}", C.DIM))


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Revenue Transformer UAT end-to-end verification"
    )
    parser.add_argument("--business-id", type=int, default=40)
    parser.add_argument("--start-date",  default="2026-01-01")
    parser.add_argument("--end-date",    default="2026-03-31")
    parser.add_argument(
        "--tenant-isolation", action="store_true",
        help="Also run with biz 42 and confirm no cross-tenant data",
    )
    parser.add_argument(
        "--show-sample", action="store_true",
        help="Print the embedding text of one doc per type",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end   = date.fromisoformat(args.end_date)

    print(c(f"\n{'═' * 72}", C.CYAN))
    print(c("  Revenue Transformer — UAT End-to-End Verification", C.BOLD))
    print(c(f"  Backend  : {settings.ANALYTICS_BACKEND_URL}", C.DIM))
    print(c(f"  Business : {args.business_id}", C.DIM))
    print(c(f"  Range    : {start}  →  {end}", C.DIM))
    auth_bits = []
    if settings.ANALYTICS_BACKEND_API_KEY:
        auth_bits.append("X-API-Key")
    if getattr(settings, "ANALYTICS_BACKEND_BEARER_TOKEN", ""):
        auth_bits.append("Bearer")
    print(c(f"  Auth     : {', '.join(auth_bits) if auth_bits else 'none'}", C.DIM))
    print(c(f"{'═' * 72}\n", C.CYAN))

    # ── Run 1: main business_id ───────────────────────────────────────────────
    print(c(f"▶ Running transformer against business_id={args.business_id}...", C.BOLD))
    try:
        docs = await run_for_business(args.business_id, start, end)
    except Exception as e:
        print(c(f"  ❌ Transformer crashed: {type(e).__name__}: {e}", C.RED))
        return 1

    p1, t1 = summarize(docs, args.business_id)
    if args.show_sample:
        print_sample_docs(docs)

    # ── Run 2: tenant isolation (optional) ────────────────────────────────────
    p2, t2 = 0, 0
    if args.tenant_isolation:
        print()
        print(c("▶ Tenant isolation check — running against business_id=42...", C.BOLD))
        try:
            docs_42 = await run_for_business(42, start, end)
        except Exception as e:
            print(c(f"  ❌ Crashed on biz 42: {type(e).__name__}: {e}", C.RED))
            return 1

        print(c(f"  → {len(docs_42)} docs produced for biz 42", C.DIM))
        wrong_tenant = [d for d in docs_42 if d.get("tenant_id") != 42]
        print(c("  Checks:", C.BOLD))
        if wrong_tenant:
            print(c(f"    ❌ {len(wrong_tenant)} docs had tenant_id != 42  "
                    f"(CROSS-TENANT LEAK)", C.RED))
        else:
            print(c("    ✅ All biz 42 docs have tenant_id == 42 "
                    "(no cross-tenant leak)", C.GREEN))
            p2 += 1
        t2 += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    total_passed = p1 + p2
    total_checks = t1 + t2

    print()
    print(c("═" * 72, C.CYAN))
    if total_passed == total_checks:
        print(c(f"  ✅ ALL {total_checks} CHECKS PASSED", C.GREEN + C.BOLD))
        print(c("     → Revenue domain Step 4 verified end-to-end against UAT",
                C.GREEN))
        print(c("     → Safe to proceed to Step 5 (Connect to Chat)", C.GREEN))
        code = 0
    else:
        print(c(f"  ❌ {total_checks - total_passed} of {total_checks} checks failed",
                C.RED + C.BOLD))
        code = 1
    print(c("═" * 72, C.CYAN))
    print()

    return code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))