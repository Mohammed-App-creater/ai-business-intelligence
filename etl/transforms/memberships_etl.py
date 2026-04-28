"""
etl/transforms/memberships_etl.py
==================================
Memberships Domain — ETL Extractor (Step 4)

Pulls Set A and Set B from the Analytics Backend, writes them to the
warehouse tables (wh_membership_units, wh_membership_monthly), and
returns the warehouse rows for downstream doc generation.

Pattern matches AppointmentsExtractor / ClientsExtractor:
    extractor = MembershipsExtractor(client=..., wh_pool=...)
    rows = await extractor.run(business_id, start_date, end_date)
    # rows is a dict ready for generate_membership_docs()

NOTE: doc-text generation lives in
      app/services/doc_generators/domains/memberships.py
      — this file only handles fetch + warehouse write.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

import asyncpg

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Type coercion — JSON strings → Python datetime/date for asyncpg
# ─────────────────────────────────────────────────────────────────────────────
#  The Analytics Backend returns timestamps as ISO 8601 strings (per the
#  Step 3 API spec). asyncpg's TIMESTAMPTZ / DATE codecs require Python
#  datetime / date objects, so we coerce on the warehouse-write boundary.

def _to_ts(v: Any) -> Optional[datetime]:
    """ISO string / date / datetime / None  →  datetime (or None)."""
    if v is None or isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, datetime.min.time(), tzinfo=timezone.utc)
    if isinstance(v, str):
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    raise TypeError(f"Cannot coerce {type(v).__name__} to datetime: {v!r}")


def _to_date(v: Any) -> Optional[date]:
    """ISO string / date / datetime / None  →  date (or None)."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return date.fromisoformat(v[:10])
    raise TypeError(f"Cannot coerce {type(v).__name__} to date: {v!r}")


class MembershipsExtractor:
    """
    Pulls and persists all memberships data for one tenant.

    Lifecycle:
        1. Fetch Set A from /api/v1/analytics/memberships          (unit grain)
        2. Fetch Set B from /api/v1/analytics/memberships/monthly  (location-month grain)
        3. UPSERT to wh_membership_units and wh_membership_monthly
        4. Return raw rows for the doc generator to consume
    """

    DOMAIN = "memberships"

    def __init__(
        self,
        client: AnalyticsClient,
        wh_pool: asyncpg.Pool,
    ):
        self._client = client
        self._wh = wh_pool

    # ─────────────────────────────────────────────────────────────────────────
    #  Main entry point
    # ─────────────────────────────────────────────────────────────────────────
    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        as_of_date: Optional[date] = None,
    ) -> dict:
        """
        Returns:
            {
                "units":        list[dict],       # 1 row per subscription
                "monthly":      list[dict],       # 1 row per (location, month)
                "as_of_date":   date,             # snapshot date for unit rows
                "period_start": date,             # window start
                "period_end":   date,             # window end
                "counts":       {"units": int, "monthly": int},
            }
        """
        as_of = as_of_date or end_date

        logger.info(
            "memberships ETL start: biz=%s as_of=%s window=%s..%s",
            business_id, as_of, start_date, end_date,
        )

        # 1. Fetch
        units_payload = await self._client.get_memberships(business_id, as_of)
        monthly_payload = await self._client.get_memberships_monthly(
            business_id, start_date, end_date,
        )

        units = units_payload.get("data", [])
        monthly = monthly_payload.get("data", [])

        # 2. Warehouse write (idempotent UPSERT, safe even on empty)
        if units or monthly:
            await self._write_units(business_id, as_of, units)
            await self._write_monthly(monthly)
            logger.info(
                "memberships warehouse write complete for biz=%d — units=%d monthly=%d",
                business_id, len(units), len(monthly),
            )
        else:
            logger.warning(
                "memberships ETL: no data returned by analytics backend for biz=%s",
                business_id,
            )

        # 3. Return rows for downstream doc generator
        return {
            "units":        units,
            "monthly":      monthly,
            "as_of_date":   as_of,
            "period_start": start_date,
            "period_end":   end_date,
            "counts": {
                "units":   len(units),
                "monthly": len(monthly),
            },
        }

    # ─────────────────────────────────────────────────────────────────────────
    #  Warehouse writes (idempotent UPSERT on PK)
    # ─────────────────────────────────────────────────────────────────────────
    async def _write_units(
        self,
        business_id: int,
        as_of: date,
        units: list[dict],
    ) -> None:
        if not units:
            return

        sql = """
        INSERT INTO wh_membership_units (
            business_id, as_of_date, subscription_id,
            location_id, customer_id, customer_name,
            service_id, service_name,
            amount, discount, net_amount,
            interval_days, interval_bucket,
            monthly_equivalent_revenue, estimated_ltv,
            created_at, canceled_at, is_active, is_reactivation, tenure_days,
            next_execution_date, days_until_next_charge, is_due_in_7_days,
            total_charge_count, approved_charge_count, failed_charge_count,
            total_billed, last_successful_charge_at, days_since_last_charge,
            visit_count_in_window, last_visit_at, is_used,
            etl_run_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26,
            $27, $28, $29, $30, $31, $32,
            NOW()
        )
        ON CONFLICT (business_id, as_of_date, subscription_id)
        DO UPDATE SET
            location_id                = EXCLUDED.location_id,
            customer_id                = EXCLUDED.customer_id,
            customer_name              = EXCLUDED.customer_name,
            service_id                 = EXCLUDED.service_id,
            service_name               = EXCLUDED.service_name,
            amount                     = EXCLUDED.amount,
            discount                   = EXCLUDED.discount,
            net_amount                 = EXCLUDED.net_amount,
            interval_days              = EXCLUDED.interval_days,
            interval_bucket            = EXCLUDED.interval_bucket,
            monthly_equivalent_revenue = EXCLUDED.monthly_equivalent_revenue,
            estimated_ltv              = EXCLUDED.estimated_ltv,
            created_at                 = EXCLUDED.created_at,
            canceled_at                = EXCLUDED.canceled_at,
            is_active                  = EXCLUDED.is_active,
            is_reactivation            = EXCLUDED.is_reactivation,
            tenure_days                = EXCLUDED.tenure_days,
            next_execution_date        = EXCLUDED.next_execution_date,
            days_until_next_charge     = EXCLUDED.days_until_next_charge,
            is_due_in_7_days           = EXCLUDED.is_due_in_7_days,
            total_charge_count         = EXCLUDED.total_charge_count,
            approved_charge_count      = EXCLUDED.approved_charge_count,
            failed_charge_count        = EXCLUDED.failed_charge_count,
            total_billed               = EXCLUDED.total_billed,
            last_successful_charge_at  = EXCLUDED.last_successful_charge_at,
            days_since_last_charge     = EXCLUDED.days_since_last_charge,
            visit_count_in_window      = EXCLUDED.visit_count_in_window,
            last_visit_at              = EXCLUDED.last_visit_at,
            is_used                    = EXCLUDED.is_used,
            etl_run_at                 = NOW()
        """

        rows = [
            (
                business_id, as_of, r["subscription_id"],
                r["location_id"], r["customer_id"], r["customer_name"],
                r["service_id"], r["service_name"],
                r["amount"], r["discount"], r["net_amount"],
                r["interval_days"], r["interval_bucket"],
                r["monthly_equivalent_revenue"], r["estimated_ltv"],
                _to_ts(r["created_at"]),                    # $16 TIMESTAMPTZ
                _to_ts(r["canceled_at"]),                   # $17 TIMESTAMPTZ
                r["is_active"],
                r["is_reactivation"], r["tenure_days"],
                _to_ts(r["next_execution_date"]),           # $21 TIMESTAMPTZ
                r["days_until_next_charge"], r["is_due_in_7_days"],
                r["total_charge_count"], r["approved_charge_count"], r["failed_charge_count"],
                r["total_billed"],
                _to_ts(r["last_successful_charge_at"]),     # $28 TIMESTAMPTZ
                r["days_since_last_charge"],
                r["visit_count_in_window"],
                _to_ts(r["last_visit_at"]),                 # $31 TIMESTAMPTZ
                r["is_used"],
            )
            for r in units
        ]

        async with self._wh.acquire() as conn:
            await conn.executemany(sql, rows)

    async def _write_monthly(self, monthly: list[dict]) -> None:
        if not monthly:
            return

        sql = """
        INSERT INTO wh_membership_monthly (
            business_id, location_id, month_start,
            new_signups, reactivations, cancellations,
            active_at_month_end, mrr, avg_discount,
            gross_billed, approved_charges, failed_charges,
            prev_mrr, mrr_mom_pct, prev_active, churn_rate_pct,
            etl_run_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15, $16, NOW()
        )
        ON CONFLICT (business_id, location_id, month_start)
        DO UPDATE SET
            new_signups          = EXCLUDED.new_signups,
            reactivations        = EXCLUDED.reactivations,
            cancellations        = EXCLUDED.cancellations,
            active_at_month_end  = EXCLUDED.active_at_month_end,
            mrr                  = EXCLUDED.mrr,
            avg_discount         = EXCLUDED.avg_discount,
            gross_billed         = EXCLUDED.gross_billed,
            approved_charges     = EXCLUDED.approved_charges,
            failed_charges       = EXCLUDED.failed_charges,
            prev_mrr             = EXCLUDED.prev_mrr,
            mrr_mom_pct          = EXCLUDED.mrr_mom_pct,
            prev_active          = EXCLUDED.prev_active,
            churn_rate_pct       = EXCLUDED.churn_rate_pct,
            etl_run_at           = NOW()
        """

        rows = [
            (
                r["business_id"], r["location_id"],
                _to_date(r["month_start"]),                 # $3 DATE
                r["new_signups"], r["reactivations"], r["cancellations"],
                r["active_at_month_end"], r["mrr"], r["avg_discount"],
                r["gross_billed"], r["approved_charges"], r["failed_charges"],
                r["prev_mrr"], r["mrr_mom_pct"], r["prev_active"], r["churn_rate_pct"],
            )
            for r in monthly
        ]

        async with self._wh.acquire() as conn:
            await conn.executemany(sql, rows)