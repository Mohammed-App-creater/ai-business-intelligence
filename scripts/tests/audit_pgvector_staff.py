#!/usr/bin/env python3
"""
scripts/tests/audit_pgvector_staff.py

Pgvector Audit — Staff Domain
==============================
Read-only inspection of staff chunks in the embeddings table for one tenant.

Mirrors the UX posture of audit_pgvector_revenue.py (PGPool VECTOR, dotenv, colors)
but targets doc_domain = 'staff' with staff-specific reporting sections.

Run:
    PYTHONPATH=. python scripts/tests/audit_pgvector_staff.py
    PYTHONPATH=. python scripts/tests/audit_pgvector_staff.py --tenant-id 40
    PYTHONPATH=. python scripts/tests/audit_pgvector_staff.py --tenant-id 40 --no-show-samples
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

# sys.path shim — same pattern as audit_pgvector_revenue.py
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

# ── ANSI colors ───────────────────────────────────────────────────────────────
class C:
    RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"
    RED, GREEN, YELLOW, CYAN, BLUE = (
        "\033[31m",
        "\033[32m",
        "\033[33m",
        "\033[36m",
        "\033[34m",
    )


def c(t: str, col: str) -> str:
    return f"{col}{t}{C.RESET}"


def _parse_date(s: str) -> date:
    return date.fromisoformat(s.strip())


async def main_async(args: argparse.Namespace) -> int:
    tenant = str(args.tenant_id).strip()
    win_start = _parse_date(args.start_date)
    win_end = _parse_date(args.end_date)

    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    try:
        async with vec_pool.acquire() as conn:
            print(c("\n▶ Staff domain pgvector audit", C.BOLD + C.CYAN))
            print(
                c(
                    f"  tenant_id={tenant!r}  doc_domain={STAFF_DOMAIN!r}\n"
                    f"  embed window (CLI): {win_start} → {win_end}",
                    C.DIM,
                )
            )

            # 1. Total count
            total = await conn.fetchval(
                """
SELECT COUNT(*)::bigint
FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
""",
                tenant,
                STAFF_DOMAIN,
            )
            print(c(f"\n── 1. Total staff chunks ─────────────────────────────────────", C.CYAN))
            print(c(f"  Count: {total}", C.BOLD))

            # 2. Breakdown by doc_type + min/max period_start
            rows_bt = await conn.fetch(
                """
SELECT doc_type,
       COUNT(*)::bigint AS n,
       MIN(period_start) AS min_ps,
       MAX(period_start) AS max_ps
FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
GROUP BY doc_type
ORDER BY doc_type
""",
                tenant,
                STAFF_DOMAIN,
            )
            print(c(f"\n── 2. By doc_type (count + period_start range) ─────────────────", C.CYAN))
            if not rows_bt:
                print(c("  (no rows)", C.YELLOW))
            else:
                for r in rows_bt:
                    print(
                        c(
                            f"  {r['doc_type']!s:22s}  n={r['n']:>4}  "
                            f"min(period_start)={r['min_ps']!s}  max(period_start)={r['max_ps']!s}",
                            C.DIM,
                        )
                    )

            # 3. Distinct period_start per doc_type (coverage)
            rows_cov = await conn.fetch(
                """
SELECT doc_type, period_start, COUNT(*)::bigint AS n
FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2 AND period_start IS NOT NULL
GROUP BY doc_type, period_start
ORDER BY doc_type, period_start
""",
                tenant,
                STAFF_DOMAIN,
            )
            print(c(f"\n── 3. Coverage — distinct period_start per doc_type ──────────", C.CYAN))
            by_dt: dict[str, list[Any]] = defaultdict(list)
            for r in rows_cov:
                by_dt[str(r["doc_type"])].append(r["period_start"])
            for dt in sorted(by_dt.keys()):
                periods = by_dt[dt]
                print(
                    c(
                        f"  {dt}: {len(periods)} distinct period(s) — "
                        f"{', '.join(str(p) for p in periods)}",
                        C.DIM,
                    )
                )
            if not rows_cov:
                print(c("  (no rows with period_start set)", C.YELLOW))

            # 4. Out-of-window chunks
            bad = await conn.fetch(
                """
SELECT doc_type, doc_id, period_start,
       LEFT(chunk_text, 120) AS snippet
FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
  AND period_start IS NOT NULL
  AND (period_start < $3::date OR period_start > $4::date)
ORDER BY doc_type, period_start
""",
                tenant,
                STAFF_DOMAIN,
                win_start,
                win_end,
            )
            print(c(f"\n── 4. Out-of-window (period_start ∉ [{win_start}, {win_end}]) ─", C.CYAN))
            if not bad:
                print(c("  ✅ None — all dated chunks fall inside the declared window.", C.GREEN))
            else:
                print(c(f"  ⚠ {len(bad)} chunk(s) outside window:", C.YELLOW))
                for r in bad[:50]:
                    sn = (r["snippet"] or "").replace("\n", " ")
                    print(
                        c(
                            f"    {r['doc_type']!s}  period_start={r['period_start']!s}  "
                            f"doc_id={r['doc_id']!s}",
                            C.RED,
                        )
                    )
                    print(c(f"      {sn[:100]}…", C.DIM))
                if len(bad) > 50:
                    print(c(f"    … ({len(bad) - 50} more)", C.DIM))

            # 5. Sample row per doc_type
            if args.show_samples:
                types_rows = await conn.fetch(
                    """
SELECT DISTINCT doc_type FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2
ORDER BY doc_type
""",
                    tenant,
                    STAFF_DOMAIN,
                )
                print(c(f"\n── 5. Sample per doc_type (doc_id, period_start, 200 chars) ───", C.CYAN))
                for tr in types_rows:
                    dt = tr["doc_type"]
                    one = await conn.fetchrow(
                        """
SELECT doc_id, period_start, chunk_text
FROM embeddings
WHERE tenant_id = $1 AND doc_domain = $2 AND doc_type = $3
LIMIT 1
""",
                        tenant,
                        STAFF_DOMAIN,
                        dt,
                    )
                    if not one:
                        continue
                    txt = (one["chunk_text"] or "").replace("\n", " ")
                    prev = txt[:200] + (" …" if len(txt) > 200 else "")
                    print(c(f"  doc_type={dt!r}", C.BOLD))
                    print(c(f"    doc_id={one['doc_id']!s}", C.DIM))
                    print(c(f"    period_start={one['period_start']!s}", C.DIM))
                    print(c(f"    {prev}", C.DIM))

            print()
    finally:
        await vec_pool.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only audit of staff-domain chunks in pgvector (embeddings)"
    )
    parser.add_argument(
        "--tenant-id",
        default="40",
        help="Tenant id (embeddings.tenant_id TEXT). Default: 40",
    )
    parser.add_argument(
        "--start-date",
        default="2025-10-01",
        help="Embed window start for out-of-window check (ISO date). Default: 2025-10-01",
    )
    parser.add_argument(
        "--end-date",
        default="2026-03-31",
        help="Embed window end for out-of-window check (ISO date). Default: 2026-03-31",
    )
    parser.add_argument(
        "--show-samples",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print one sample chunk per doc_type (default: true)",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
