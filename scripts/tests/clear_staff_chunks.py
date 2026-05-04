#!/usr/bin/env python3
"""
scripts/tests/clear_staff_chunks.py

Deletes staff-domain chunks in pgvector for one tenant.
Optionally clears wh_staff_* warehouse tables for the same business_id.

Built by mirroring clear_revenue_chunks.py safety posture:
  - Dry-run by default; pass --confirm to delete.
  - Tenant must be explicit (--tenant-id required — no default).
  - Before destructive delete: print summary + 5-second countdown (Ctrl+C to cancel).

Additionally (staff-specific):
  - Optional --warehouse: DELETE FROM wh_staff_performance_monthly / summary / attendance
    WHERE business_id = <int>.

Embeddings deletion (always when confirming vector clear):
  DELETE FROM embeddings WHERE tenant_id = $1 AND doc_domain = 'staff';

Run:
    PYTHONPATH=. python scripts/tests/clear_staff_chunks.py --tenant-id 40
    PYTHONPATH=. python scripts/tests/clear_staff_chunks.py --tenant-id 40 --confirm
    PYTHONPATH=. python scripts/tests/clear_staff_chunks.py --tenant-id 40 --confirm --warehouse
"""
from __future__ import annotations

import argparse
import asyncio
import sys
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

STAFF_DOMAIN = "staff"
COUNTDOWN_SECONDS = 5


class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN = "\033[31m", "\033[32m", "\033[33m", "\033[36m"


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


async def _count_staff_embeddings(conn, tenant: str) -> int:
    return int(
        await conn.fetchval(
            """
SELECT COUNT(*)::bigint FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
""",
            tenant,
            STAFF_DOMAIN,
        )
        or 0
    )


async def _count_wh_rows(conn, biz: int) -> dict[str, int]:
    out: dict[str, int] = {}
    for tbl in (
        "wh_staff_performance_monthly",
        "wh_staff_summary",
        "wh_staff_attendance",
    ):
        out[tbl] = int(
            await conn.fetchval(
                f"SELECT COUNT(*)::bigint FROM {tbl} WHERE business_id = $1",
                biz,
            )
            or 0
        )
    return out


async def main_async(args: argparse.Namespace) -> int:
    tenant = str(args.tenant_id).strip()
    try:
        biz = int(tenant)
    except ValueError:
        print(c("❌ --tenant-id must be numeric for warehouse business_id mapping.", C.RED))
        return 2

    vec_pool = await PGPool.from_env(PGTarget.VECTOR)

    try:
        wh_counts: dict[str, int] = {}
        if args.warehouse:
            wh_pool_pre = await PGPool.from_env(PGTarget.WAREHOUSE)
            try:
                async with wh_pool_pre.acquire() as whc:
                    wh_counts = await _count_wh_rows(whc, biz)
            finally:
                await wh_pool_pre.close()

        async with vec_pool.acquire() as conn:
            before_vec = await _count_staff_embeddings(conn, tenant)

            print(c("\n── clear_staff_chunks ──────────────────────────────────────────", C.CYAN))
            print(c(f"  tenant_id (embeddings): {tenant!r}", C.DIM))
            print(c(f"  doc_domain: {STAFF_DOMAIN!r}", C.DIM))
            print(c(f"  Staff embedding rows to delete: {before_vec}", C.BOLD))

            if args.warehouse:
                tw = sum(wh_counts.values())
                print(
                    c(
                        f"  --warehouse: wh_staff_* rows for business_id={biz}: {tw} total",
                        C.YELLOW,
                    )
                )
                for tbl, n in wh_counts.items():
                    print(c(f"      {tbl}: {n}", C.DIM))

            wh_total = sum(wh_counts.values()) if args.warehouse else 0
            will_delete_any = before_vec > 0 or wh_total > 0

            if not will_delete_any:
                print(c("\n  (nothing to delete)", C.DIM))

            if not args.confirm:
                print(
                    c(
                        "\n  [DRY RUN] No changes made. Pass --confirm to execute DELETE.",
                        C.YELLOW,
                    )
                )
                return 0

            if not will_delete_any:
                print(c("\n  --confirm set but nothing to delete.", C.YELLOW))
                return 0

            print(
                c(
                    f"\n  ⚠  CONFIRMED DELETE: embeddings={before_vec}; tenant_id={tenant!r}",
                    C.RED + C.BOLD,
                )
            )
            if args.warehouse and wh_total > 0:
                print(c(f"  ⚠  Warehouse DELETE for business_id={biz}", C.RED))
            print(
                c(
                    f"  {COUNTDOWN_SECONDS}-second pause — Ctrl+C to cancel…",
                    C.YELLOW,
                )
            )
            for sec in range(COUNTDOWN_SECONDS, 0, -1):
                print(c(f"    … {sec}", C.DIM))
                await asyncio.sleep(1)

            deleted_vec = await conn.execute(
                """
DELETE FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
""",
                tenant,
                STAFF_DOMAIN,
            )
            n_vec = int(deleted_vec.split()[-1]) if deleted_vec.startswith("DELETE") else 0
            print(c(f"\n  ✅ Deleted from embeddings: {n_vec} row(s)", C.GREEN))

            after_vec = await _count_staff_embeddings(conn, tenant)
            if after_vec != 0:
                print(c(f"  ⚠ Unexpected: {after_vec} staff rows remain in embeddings.", C.RED))

        if args.warehouse:
            wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
            try:
                async with wh_pool.acquire() as wh_conn:
                    tables = (
                        "wh_staff_performance_monthly",
                        "wh_staff_summary",
                        "wh_staff_attendance",
                    )
                    print(c("\n── Warehouse deletes ─────────────────────────────────────────", C.CYAN))
                    for tbl in tables:
                        res = await wh_conn.execute(
                            f"DELETE FROM {tbl} WHERE business_id = $1",
                            biz,
                        )
                        n = int(res.split()[-1]) if res.startswith("DELETE") else 0
                        print(c(f"  ✅ {tbl}: deleted {n} row(s)", C.GREEN))
            finally:
                await wh_pool.close()

        print()
        return 0

    finally:
        await vec_pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear staff chunks from pgvector (and optionally warehouse tables)"
    )
    parser.add_argument(
        "--tenant-id",
        required=True,
        help="Tenant id (required). embeddings.tenant_id TEXT; also cast to int for warehouse.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually delete. Without this, dry-run only (exit 0).",
    )
    parser.add_argument(
        "--warehouse",
        action="store_true",
        help="Also DELETE rows from wh_staff_* for business_id = int(tenant-id)",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
