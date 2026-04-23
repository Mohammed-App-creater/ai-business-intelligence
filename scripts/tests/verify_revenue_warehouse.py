#!/usr/bin/env python3
"""
scripts/tests/verify_revenue_warehouse.py

Verify revenue warehouse population.
=====================================
Dumps the contents of wh_monthly_revenue and wh_payment_breakdown
for a given tenant, so you can see that the extractor successfully
wrote rows alongside embedding them into pgvector.

Run AFTER embed_documents.py to confirm the end-to-end pipeline:
    UAT backend → RevenueExtractor → wh_* tables + pgvector.

Usage:
    PYTHONPATH=. python scripts/tests/verify_revenue_warehouse.py --tenant 40
    PYTHONPATH=. python scripts/tests/verify_revenue_warehouse.py --tenants 40 42
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# sys.path shim
_HERE = Path(__file__).resolve()
for _c in (_HERE.parent, *_HERE.parents):
    if (_c / "app").is_dir():
        if str(_c) not in sys.path:
            sys.path.insert(0, str(_c))
        break

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.services.db.db_pool import PGPool, PGTarget


class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN = "\033[31m", "\033[32m", "\033[33m", "\033[36m"


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


async def main_async(args: argparse.Namespace) -> int:
    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)

    try:
        async with wh_pool.acquire() as conn:
            for tenant in args.tenants:
                print(c(f"\n═══ tenant={tenant} ═══════════════════════════════════════════",
                        C.CYAN + C.BOLD))

                # ── wh_monthly_revenue ──────────────────────────────────────
                rows = await conn.fetch("""
                    SELECT location_id, period_start, period_end,
                           total_revenue, total_tips, total_tax, total_discounts,
                           total_gc_amount, gross_revenue, visit_count,
                           successful_visit_count, refunded_visit_count,
                           cancelled_visit_count, avg_visit_value,
                           cash_revenue, card_revenue, other_revenue,
                           updated_at
                    FROM wh_monthly_revenue
                    WHERE business_id = $1
                    ORDER BY period_start, location_id
                """, int(tenant))

                print(c(f"\n▶ wh_monthly_revenue ({len(rows)} rows)", C.BOLD))
                if not rows:
                    print(c("    (empty — did embed_documents.py run with revenue domain?)",
                            C.YELLOW))
                else:
                    # Print rollup rows first (location_id=0), then per-location
                    header = (
                        f"    {'loc':>4}  {'period':<12}  "
                        f"{'revenue':>12}  {'tips':>10}  {'tax':>10}  "
                        f"{'visits':>7}  {'avg':>10}"
                    )
                    print(c(header, C.DIM))
                    print(c("    " + "─" * (len(header) - 4), C.DIM))
                    for r in rows:
                        loc_tag = "ORG" if r["location_id"] == 0 else str(r["location_id"])
                        line = (
                            f"    {loc_tag:>4}  "
                            f"{r['period_start']!s:<12}  "
                            f"${float(r['total_revenue']):>10,.2f}  "
                            f"${float(r['total_tips']):>8,.2f}  "
                            f"${float(r['total_tax']):>8,.2f}  "
                            f"{r['visit_count']:>7}  "
                            f"${float(r['avg_visit_value']):>8,.2f}"
                        )
                        # Highlight org rollup
                        color = C.GREEN if r["location_id"] == 0 else C.DIM
                        print(c(line, color))

                # ── wh_payment_breakdown ────────────────────────────────────
                rows = await conn.fetch("""
                    SELECT location_id, period_start, period_end,
                           cash_amount, cash_count,
                           card_amount, card_count,
                           gift_card_amount, gift_card_count,
                           other_amount, other_count,
                           total_amount, total_count,
                           updated_at
                    FROM wh_payment_breakdown
                    WHERE business_id = $1
                    ORDER BY period_start, location_id
                """, int(tenant))

                print(c(f"\n▶ wh_payment_breakdown ({len(rows)} rows)", C.BOLD))
                if not rows:
                    print(c("    (empty)", C.YELLOW))
                else:
                    header = (
                        f"    {'period':<12}  "
                        f"{'cash':>12}  {'card':>12}  "
                        f"{'gc':>10}  {'other':>10}  {'total':>12}"
                    )
                    print(c(header, C.DIM))
                    print(c("    " + "─" * (len(header) - 4), C.DIM))
                    for r in rows:
                        line = (
                            f"    {r['period_start']!s:<12}  "
                            f"${float(r['cash_amount']):>10,.2f}  "
                            f"${float(r['card_amount']):>10,.2f}  "
                            f"${float(r['gift_card_amount']):>8,.2f}  "
                            f"${float(r['other_amount']):>8,.2f}  "
                            f"${float(r['total_amount']):>10,.2f}"
                        )
                        print(c(line, C.DIM))

                # ── Totals summary ───────────────────────────────────────────
                total_rev_row = await conn.fetchrow("""
                    SELECT SUM(total_revenue) AS total_rev,
                           SUM(visit_count) AS total_visits
                    FROM wh_monthly_revenue
                    WHERE business_id = $1 AND location_id = 0
                """, int(tenant))

                if total_rev_row and total_rev_row["total_rev"] is not None:
                    print(c(
                        f"\n    ORG-LEVEL TOTALS: "
                        f"${float(total_rev_row['total_rev']):,.2f} revenue "
                        f"across {total_rev_row['total_visits']} visits",
                        C.BOLD + C.GREEN,
                    ))

    finally:
        await wh_pool.close()

    print()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify revenue warehouse tables are populated after embed_documents.py"
    )
    parser.add_argument("--tenants", nargs="+", default=None)
    parser.add_argument("--tenant", default=None,
                        help="Single tenant shortcut")
    args = parser.parse_args()

    tenants: list[str] = []
    if args.tenant:
        tenants.append(str(args.tenant))
    if args.tenants:
        tenants.extend(str(t) for t in args.tenants)
    if not tenants:
        parser.error("Provide --tenant <ID> or --tenants <ID> <ID> ...")
    args.tenants = tenants

    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()