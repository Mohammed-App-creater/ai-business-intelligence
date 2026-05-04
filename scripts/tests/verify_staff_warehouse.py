#!/usr/bin/env python3
"""
scripts/tests/verify_staff_warehouse.py

Verify staff warehouse population after embed_documents.py --domain staff.
Read-only sanity checks on wh_staff_performance_monthly, wh_staff_summary,
wh_staff_attendance.

Mirrors verify_revenue_warehouse.py shape: PGPool WAREHOUSE, tenant loop, colored output.

Usage:
    PYTHONPATH=. python scripts/tests/verify_staff_warehouse.py
    PYTHONPATH=. python scripts/tests/verify_staff_warehouse.py --tenant-id 42
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

_HERE = Path(__file__).resolve()
for _c in (_HERE.parent, *_HERE.parents):
    if (_c / "app").is_dir():
        if str(_c) not in sys.path:
            sys.path.insert(0, str(_c))
        break

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from app.services.db.db_pool import PGPool, PGTarget


class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN = "\033[31m", "\033[32m", "\033[33m", "\033[36m"


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())


async def main_async(args: argparse.Namespace) -> int:
    tenant = int(str(args.tenant_id).strip())
    win_lo = _parse_date(args.start_date)
    win_hi = _parse_date(args.end_date)

    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)

    try:
        async with wh_pool.acquire() as conn:
            print(
                c(
                    f"\n═══ Staff warehouse verify — tenant business_id={tenant} ═══",
                    C.CYAN + C.BOLD,
                )
            )
            print(
                c(
                    f"  Date window (sanity): {win_lo} → {win_hi}",
                    C.DIM,
                )
            )

            # ── 1. Row counts ─────────────────────────────────────────────
            print(c("\n── 1. Row counts ──────────────────────────────────────────────", C.CYAN))
            for label, sql in [
                ("wh_staff_performance_monthly", "wh_staff_performance_monthly"),
                ("wh_staff_summary", "wh_staff_summary"),
                ("wh_staff_attendance", "wh_staff_attendance"),
            ]:
                n = await conn.fetchval(
                    f"SELECT COUNT(*)::bigint FROM {sql} WHERE business_id = $1",
                    tenant,
                )
                print(c(f"  {label}: {n}", C.BOLD))

            # ── 2. Date range coverage ────────────────────────────────────
            print(c("\n── 2. Date range (period_start min/max) ───────────────────────", C.CYAN))
            r1 = await conn.fetchrow(
                """
SELECT MIN(period_start) AS lo, MAX(period_start) AS hi
FROM wh_staff_performance_monthly WHERE business_id = $1
""",
                tenant,
            )
            print(
                c(
                    f"  wh_staff_performance_monthly: "
                    f"{r1['lo']!s} → {r1['hi']!s}",
                    C.DIM,
                )
            )
            r2 = await conn.fetchrow(
                """
SELECT MIN(period_start) AS lo, MAX(period_start) AS hi
FROM wh_staff_attendance WHERE business_id = $1
""",
                tenant,
            )
            print(
                c(
                    f"  wh_staff_attendance:        "
                    f"{r2['lo']!s} → {r2['hi']!s}",
                    C.DIM,
                )
            )
            r3 = await conn.fetchrow(
                """
SELECT MIN(period_from) AS pf_min, MAX(period_to) AS pf_max
FROM wh_staff_summary WHERE business_id = $1
""",
                tenant,
            )
            print(
                c(
                    f"  wh_staff_summary (period_from / period_to VARCHAR): "
                    f"{r3['pf_min']!s} → {r3['pf_max']!s}",
                    C.DIM,
                )
            )

            # Rows in monthly table outside CLI window (informational)
            oob = await conn.fetchval(
                """
SELECT COUNT(*)::bigint FROM wh_staff_performance_monthly
WHERE business_id = $1
  AND (period_start < $2::date OR period_start > $3::date)
""",
                tenant,
                win_lo,
                win_hi,
            )
            print(c(f"  Rows in monthly with period_start outside [{win_lo}, {win_hi}]: {oob}", C.YELLOW))

            # ── 3. Active vs inactive (monthly) ──────────────────────────
            print(c("\n── 3. Active vs inactive (wh_staff_performance_monthly) ───────", C.CYAN))
            rows_ai = await conn.fetch(
                """
SELECT is_active, COUNT(*)::bigint AS n
FROM wh_staff_performance_monthly
WHERE business_id = $1
GROUP BY is_active
ORDER BY is_active DESC
""",
                tenant,
            )
            for r in rows_ai:
                tag = "active" if r["is_active"] else "inactive"
                print(c(f"  is_active={r['is_active']!s} ({tag}): {r['n']}", C.DIM))

            # ── 4. NULL hire_date ────────────────────────────────────────
            print(c("\n── 4. NULL hire_date count (monthly) ──────────────────────────", C.CYAN))
            null_h = await conn.fetchval(
                """
SELECT COUNT(*)::bigint FROM wh_staff_performance_monthly
WHERE business_id = $1 AND hire_date IS NULL
""",
                tenant,
            )
            print(c(f"  Rows with hire_date IS NULL: {null_h}", C.DIM))

            # ── 5. Multi-location pairs ─────────────────────────────────
            print(c("\n── 5. Multi-location (employee_id, period_start) > 1 row ──────", C.CYAN))
            multi_n = await conn.fetchval(
                """
SELECT COUNT(*)::bigint FROM (
    SELECT employee_id, period_start
    FROM wh_staff_performance_monthly
    WHERE business_id = $1
    GROUP BY employee_id, period_start
    HAVING COUNT(*) > 1
) t
""",
                tenant,
            )
            print(c(f"  Distinct (employee_id, period_start) pairs with multiple locations: {multi_n}", C.BOLD))

            # ── 6. Sample row per table ───────────────────────────────────
            print(c("\n── 6. Sample row per table ────────────────────────────────────", C.CYAN))
            sm = await conn.fetchrow(
                "SELECT * FROM wh_staff_performance_monthly WHERE business_id = $1 LIMIT 1",
                tenant,
            )
            if sm:
                all_keys = list(sm.keys())
                keys = all_keys[:12]
                print(c("  wh_staff_performance_monthly (subset):", C.BOLD))
                for k in keys:
                    print(c(f"    {k}: {sm[k]!r}", C.DIM))
                if len(all_keys) > 12:
                    print(c(f"    … ({len(all_keys) - 12} more columns)", C.DIM))
            else:
                print(c("  wh_staff_performance_monthly: (empty)", C.YELLOW))

            ss = await conn.fetchrow(
                "SELECT * FROM wh_staff_summary WHERE business_id = $1 LIMIT 1",
                tenant,
            )
            if ss:
                all_keys_s = list(ss.keys())
                keys = all_keys_s[:12]
                print(c("\n  wh_staff_summary (subset):", C.BOLD))
                for k in keys:
                    print(c(f"    {k}: {ss[k]!r}", C.DIM))
                if len(all_keys_s) > 12:
                    print(c(f"    … ({len(all_keys_s) - 12} more columns)", C.DIM))
            else:
                print(c("\n  wh_staff_summary: (empty)", C.YELLOW))

            sa = await conn.fetchrow(
                "SELECT * FROM wh_staff_attendance WHERE business_id = $1 LIMIT 1",
                tenant,
            )
            if sa:
                all_keys_a = list(sa.keys())
                keys = all_keys_a[:12]
                print(c("\n  wh_staff_attendance (subset):", C.BOLD))
                for k in keys:
                    print(c(f"    {k}: {sa[k]!r}", C.DIM))
                if len(all_keys_a) > 12:
                    print(c(f"    … ({len(all_keys_a) - 12} more columns)", C.DIM))
            else:
                print(c("\n  wh_staff_attendance: (empty)", C.YELLOW))

            print()
    finally:
        await wh_pool.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only verification of staff warehouse tables"
    )
    parser.add_argument("--tenant-id", default="40", help="business_id (integer). Default: 40")
    parser.add_argument(
        "--start-date",
        default="2025-10-01",
        help="Expected embed window start for OOB row count (ISO). Default: 2025-10-01",
    )
    parser.add_argument(
        "--end-date",
        default="2026-03-31",
        help="Expected embed window end for OOB row count (ISO). Default: 2026-03-31",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
