"""
ETL orchestrator: production MySQL → transforms → analytics warehouse (PostgreSQL).

Run via cron or CLI: ``python scripts/etl_job.py``.
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from scripts.etl.base import ETLLogger, run_etl_job
from scripts.etl.extractors import (
    AppointmentsExtractor,
    AttendanceExtractor,
    CampaignsExtractor,
    ClientsExtractor,
    DailyRevenueExtractor,
    ExpensesExtractor,
    PaymentsExtractor,
    RevenueExtractor,
    ReviewsExtractor,
    ServicesExtractor,
    StaffExtractor,
    SubscriptionsExtractor,
)
from scripts.etl.loaders import (
    AppointmentsLoader,
    AttendanceLoader,
    CampaignsLoader,
    ClientsLoader,
    DailyRevenueLoader,
    ExpensesLoader,
    PaymentsLoader,
    RevenueLoader,
    ReviewsLoader,
    ServicesLoader,
    StaffLoader,
    SubscriptionsLoader,
)
from scripts.etl.transforms import (
    transform_appointments,
    transform_attendance,
    transform_campaigns,
    transform_clients,
    transform_daily_revenue,
    transform_expenses,
    transform_payments,
    transform_revenue,
    transform_reviews,
    transform_services,
    transform_staff,
    transform_subscriptions,
)
from app.services.db.db_pool import DBPool, DBTarget, PGPool, PGTarget

log = logging.getLogger(__name__)

_ACTIVE_ORG_SQL = """
SELECT Id FROM tbl_organization
WHERE ClientStatus = 1
  AND IsDeleted = b'1'
ORDER BY Id
""".strip()


@dataclass
class TableJob:
    table_name: str
    extractor_cls: type
    transform_fn: Callable[..., list[dict]]
    loader_cls: type
    needs_period: bool = True
    multi_input: bool = False


TABLE_REGISTRY: list[TableJob] = [
    TableJob("wh_monthly_revenue", RevenueExtractor, transform_revenue, RevenueLoader),
    TableJob(
        "wh_daily_revenue",
        DailyRevenueExtractor,
        transform_daily_revenue,
        DailyRevenueLoader,
    ),
    TableJob("wh_staff_performance", StaffExtractor, transform_staff, StaffLoader),
    TableJob(
        "wh_service_performance",
        ServicesExtractor,
        transform_services,
        ServicesLoader,
    ),
    TableJob(
        "wh_client_metrics",
        ClientsExtractor,
        transform_clients,
        ClientsLoader,
        needs_period=False,
    ),
    TableJob(
        "wh_appointment_metrics",
        AppointmentsExtractor,
        transform_appointments,
        AppointmentsLoader,
    ),
    TableJob("wh_expense_summary", ExpensesExtractor, transform_expenses, ExpensesLoader),
    TableJob(
        "wh_review_summary",
        ReviewsExtractor,
        transform_reviews,
        ReviewsLoader,
        multi_input=True,
    ),
    TableJob(
        "wh_payment_breakdown",
        PaymentsExtractor,
        transform_payments,
        PaymentsLoader,
    ),
    TableJob(
        "wh_campaign_performance",
        CampaignsExtractor,
        transform_campaigns,
        CampaignsLoader,
    ),
    TableJob(
        "wh_attendance_summary",
        AttendanceExtractor,
        transform_attendance,
        AttendanceLoader,
    ),
    TableJob(
        "wh_subscription_revenue",
        SubscriptionsExtractor,
        transform_subscriptions,
        SubscriptionsLoader,
        multi_input=True,
    ),
]


def compute_periods(
    mode: str,
    period_start_arg: str | None = None,
    period_end_arg: str | None = None,
    *,
    today: date | None = None,
) -> tuple[date, date]:
    """
    Returns ``(period_start, period_end)`` based on mode.

    If ``period_start_arg`` and ``period_end_arg`` are both provided (YYYY-MM-DD),
    they override mode.

    ``today`` is optional for tests; defaults to ``date.today()``.
    """
    ref = today or date.today()
    if period_start_arg is not None and period_end_arg is not None:
        return date.fromisoformat(period_start_arg), date.fromisoformat(period_end_arg)

    period_end = date(ref.year, ref.month, calendar.monthrange(ref.year, ref.month)[1])

    if mode == "incremental":
        y, m = ref.year, ref.month
        m -= 2
        while m < 1:
            m += 12
            y -= 1
        period_start = date(y, m, 1)
        return period_start, period_end

    if mode == "full":
        period_start = date(ref.year - 2, ref.month, 1)
        return period_start, period_end

    raise ValueError(f"Unknown mode: {mode!r}")


async def get_active_org_ids(prod_pool: Any) -> list[int]:
    """
    Active organizations from production MySQL.

    ``IsDeleted = b'1'`` means active (legacy schema).
    """
    async with prod_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(_ACTIVE_ORG_SQL)
            rows = await cur.fetchall()
    return [int(r["Id"]) for r in rows]


async def run_table_for_org(
    job: TableJob,
    org_id: int,
    period_start: date,
    period_end: date,
    prod_pool: Any,
    wh_pool: Any,
    etl_logger: ETLLogger,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Extract → transform → load for one warehouse table and one org.

    Returns ``(rows_inserted, rows_updated)``, or ``(0, 0)`` when ``dry_run`` is True.
    On failure, ``run_etl_job`` records the failure and the exception propagates.
    """
    async with run_etl_job(
        etl_logger,
        job.table_name,
        org_id=org_id,
        period_start=period_start,
        period_end=period_end,
    ) as result:
        extractor = job.extractor_cls(prod_pool)
        if job.needs_period:
            raw_rows = await extractor.extract(org_id, period_start, period_end)
        else:
            raw_rows = await extractor.extract(org_id)

        if job.table_name == "wh_review_summary":
            rows = job.transform_fn(*raw_rows)
        elif job.table_name == "wh_subscription_revenue":
            rows = job.transform_fn(raw_rows, raw_rows)
        else:
            rows = job.transform_fn(raw_rows)

        if dry_run:
            log.info(
                "org=%s | table=%s | dry_run row_count=%s",
                org_id,
                job.table_name,
                len(rows),
            )
            return 0, 0

        loader = job.loader_cls(wh_pool)
        inserted, updated = await loader.load(rows)
        result.rows_inserted = inserted
        result.rows_updated = updated
    return inserted, updated


async def run_org(
    org_id: int,
    tables: list[TableJob],
    period_start: date,
    period_end: date,
    prod_pool: Any,
    wh_pool: Any,
    etl_logger: ETLLogger,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run all ``tables`` for one org; failures are isolated per table."""
    summary: dict[str, Any] = {
        "org_id": org_id,
        "tables_ok": 0,
        "tables_failed": 0,
        "total_inserted": 0,
        "total_updated": 0,
        "errors": [],
    }

    for job in tables:
        t0 = time.perf_counter()
        try:
            ins, upd = await run_table_for_org(
                job,
                org_id,
                period_start,
                period_end,
                prod_pool,
                wh_pool,
                etl_logger,
                dry_run=dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            elapsed = time.perf_counter() - t0
            msg = str(exc)
            summary["tables_failed"] += 1
            summary["errors"].append(f"{job.table_name}: {msg}")
            log.warning(
                "org=%s | table=%s | FAILED: %s | %.2fs",
                org_id,
                job.table_name,
                msg,
                elapsed,
            )
            continue

        elapsed = time.perf_counter() - t0
        summary["tables_ok"] += 1
        summary["total_inserted"] += ins
        summary["total_updated"] += upd
        log.info(
            "org=%s | table=%s | inserted=%s updated=%s | %.2fs",
            org_id,
            job.table_name,
            ins,
            upd,
            elapsed,
        )

    log.info(
        "org=%s done | %s/%s tables OK | %s failed",
        org_id,
        summary["tables_ok"],
        len(tables),
        summary["tables_failed"],
    )
    return summary


async def run_all(args: argparse.Namespace) -> None:
    """Main async entry: pools, orgs, tables, logging, teardown."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    run_started = time.perf_counter()
    prod_pool = await DBPool.from_env(DBTarget.PRODUCTION)
    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)

    total_inserted = 0
    total_updated = 0
    tables_ok_all = 0
    tables_failed_all = 0

    try:
        etl_logger = ETLLogger(wh_pool)
        cleaned = await etl_logger.cleanup_orphaned_runs()
        if cleaned:
            log.info("Cleaned up %s orphaned ETL runs", cleaned)

        if args.period_start is not None and args.period_end is None:
            log.warning("Ignoring --period-start without --period-end; using mode window.")
            period_start, period_end = compute_periods(args.mode)
        elif args.period_end is not None and args.period_start is None:
            log.warning("Ignoring --period-end without --period-start; using mode window.")
            period_start, period_end = compute_periods(args.mode)
        elif args.period_start is not None and args.period_end is not None:
            period_start, period_end = compute_periods(
                args.mode,
                args.period_start,
                args.period_end,
            )
        else:
            period_start, period_end = compute_periods(args.mode)

        if args.org_id is not None:
            org_ids = [args.org_id]
        else:
            org_ids = await get_active_org_ids(prod_pool)

        jobs = list(TABLE_REGISTRY)
        if args.table:
            jobs = [j for j in TABLE_REGISTRY if j.table_name == args.table]
            if not jobs:
                log.error("Unknown warehouse table: %s", args.table)
                return

        log.info(
            "Starting ETL | mode=%s | orgs=%s | period=%s..%s",
            args.mode,
            len(org_ids),
            period_start,
            period_end,
        )

        for org_id in org_ids:
            summary = await run_org(
                org_id,
                jobs,
                period_start,
                period_end,
                prod_pool,
                wh_pool,
                etl_logger,
                dry_run=args.dry_run,
            )
            tables_ok_all += summary["tables_ok"]
            tables_failed_all += summary["tables_failed"]
            total_inserted += summary["total_inserted"]
            total_updated += summary["total_updated"]

        elapsed = time.perf_counter() - run_started
        log.info(
            "ETL complete | orgs=%s | inserted=%s updated=%s | %.1fs",
            len(org_ids),
            total_inserted,
            total_updated,
            elapsed,
        )
        print(
            f"Total orgs: {len(org_ids)}\n"
            f"Tables OK: {tables_ok_all}  |  Tables failed: {tables_failed_all}\n"
            f"Rows inserted: {total_inserted}  |  Rows updated: {total_updated}"
        )
    finally:
        await prod_pool.close()
        await wh_pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETL: Production DB → Analytics Warehouse",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
    )
    parser.add_argument("--org-id", type=int, default=None)
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Run only this warehouse table, e.g. wh_monthly_revenue",
    )
    parser.add_argument("--period-start", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--period-end", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract + transform only — skip warehouse writes",
    )
    args = parser.parse_args()
    asyncio.run(run_all(args))


if __name__ == "__main__":
    main()
