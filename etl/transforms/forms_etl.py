"""
etl/transforms/forms_etl.py
=============================
Forms domain ETL extractor for LEO AI BI Sprint 10.

Mirrors the gift cards (etl/transforms/giftcards_etl.py) pattern:
  1. Fetch all 4 analytics endpoints (catalog, monthly, per-form, lifecycle)
  2. Persist each to its corresponding wh_form_* warehouse table
  3. Return a rows dict for the doc generator to consume

Idempotent via INSERT ... ON CONFLICT DO UPDATE in a single transaction per run.
Tenant-isolated via business_id filter on every operation.

USAGE
=====
    extractor = FormsExtractor(analytics_client, wh_pool)
    rows = await extractor.run(
        business_id=42,
        start_date=date(2025, 1, 1),
        end_date=date(2026, 3, 31),
        snapshot_date=date(2026, 3, 31),
    )
    # rows = {"catalog": dict, "monthly": list[dict],
    #         "per_form": list[dict], "lifecycle": dict}
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.services.analytics_client import AnalyticsClient
from app.services.db.db_pool import PGPool

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Date coercion — JSON over the wire gives us ISO strings; asyncpg wants date.
# ─────────────────────────────────────────────────────────────────────────────

def _to_date(value: Any) -> date | None:
    """ISO 'YYYY-MM-DD' (or longer) -> date. None / '' pass through. date passes through."""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _to_datetime(value: Any) -> datetime | None:
    """ISO datetime string -> datetime. None passes through."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    # Tolerate trailing 'Z'
    s = str(value).rstrip("Z")
    return datetime.fromisoformat(s)


# ─────────────────────────────────────────────────────────────────────────────
# Extractor
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FormsExtractor:
    """Extract forms-domain analytics for one business and persist to warehouse."""

    analytics: AnalyticsClient
    wh: PGPool

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        snapshot_date: date,
    ) -> dict[str, Any]:
        """Fetch + persist + return rows for one business / one window.

        Returns:
            {
                "catalog":   dict (1 row),
                "monthly":   list[dict] (N rows),
                "per_form":  list[dict] (N rows, one per template),
                "lifecycle": dict (1 row, ALWAYS-EMIT),
            }
        """
        logger.info(
            "forms_etl.run business_id=%d start=%s end=%s snapshot=%s",
            business_id, start_date, end_date, snapshot_date,
        )

        # ── Fetch all 4 endpoints in parallel-ish (async, sequential ok) ──
        catalog   = await self.analytics.get_forms_catalog_snapshot(
            business_id, snapshot_date)
        monthly   = await self.analytics.get_forms_monthly(
            business_id, start_date, end_date)
        per_form  = await self.analytics.get_forms_per_form_snapshot(
            business_id, snapshot_date)
        lifecycle = await self.analytics.get_forms_lifecycle_snapshot(
            business_id, snapshot_date)

        logger.info(
            "forms_etl fetched — catalog=%d monthly=%d per_form=%d lifecycle=%d",
            1 if catalog else 0,
            len(monthly) if monthly else 0,
            len(per_form) if per_form else 0,
            1 if lifecycle else 0,
        )

        # ── Persist to warehouse (idempotent, single transaction) ──
        await self._write_to_warehouse(
            business_id    = business_id,
            catalog        = catalog,
            monthly        = monthly,
            per_form       = per_form,
            lifecycle      = lifecycle,
        )

        logger.info(
            "warehouse write complete domain=forms business_id=%d "
            "catalog=%d monthly=%d per_form=%d lifecycle=%d",
            business_id,
            1 if catalog else 0,
            len(monthly) if monthly else 0,
            len(per_form) if per_form else 0,
            1 if lifecycle else 0,
        )

        # ── Return rows for doc gen consumption ──
        return {
            "catalog":   catalog,
            "monthly":   monthly or [],
            "per_form":  per_form or [],
            "lifecycle": lifecycle,
        }

    # ───────────────────────────────────────────────────────────────────────
    # Warehouse writers — one per table, all idempotent
    # ───────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        business_id: int,
        catalog: dict | None,
        monthly: list[dict] | None,
        per_form: list[dict] | None,
        lifecycle: dict | None,
    ) -> None:
        """Single-transaction write across all 4 wh_form_* tables."""
        async with self.wh.acquire() as conn:
            async with conn.transaction():
                if catalog:
                    await self._upsert_catalog(conn, business_id, catalog)
                if monthly:
                    await self._upsert_monthly(conn, business_id, monthly)
                if per_form:
                    await self._upsert_per_form(conn, business_id, per_form)
                if lifecycle:
                    # ALWAYS-EMIT: lifecycle is never None per FQ4 contract
                    await self._upsert_lifecycle(conn, business_id, lifecycle)

    async def _upsert_catalog(self, conn, business_id: int, row: dict) -> None:
        await conn.execute(
            """
            INSERT INTO wh_form_catalog_snapshot (
                business_id, snapshot_date,
                total_template_count, active_template_count, inactive_template_count,
                active_dormant_count, inactive_dormant_count,
                lifetime_submission_total, recent_90d_submission_total,
                most_recent_template_added, distinct_category_ids, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
            ON CONFLICT (business_id, snapshot_date) DO UPDATE SET
                total_template_count        = EXCLUDED.total_template_count,
                active_template_count       = EXCLUDED.active_template_count,
                inactive_template_count     = EXCLUDED.inactive_template_count,
                active_dormant_count        = EXCLUDED.active_dormant_count,
                inactive_dormant_count      = EXCLUDED.inactive_dormant_count,
                lifetime_submission_total   = EXCLUDED.lifetime_submission_total,
                recent_90d_submission_total = EXCLUDED.recent_90d_submission_total,
                most_recent_template_added  = EXCLUDED.most_recent_template_added,
                distinct_category_ids       = EXCLUDED.distinct_category_ids,
                updated_at                  = NOW()
            """,
            business_id,
            _to_date(row["snapshot_date"]),
            row.get("total_template_count", 0),
            row.get("active_template_count", 0),
            row.get("inactive_template_count", 0),
            row.get("active_dormant_count", 0),
            row.get("inactive_dormant_count", 0),
            row.get("lifetime_submission_total", 0),
            row.get("recent_90d_submission_total", 0),
            _to_datetime(row.get("most_recent_template_added")),
            row.get("distinct_category_ids", []),
        )

    async def _upsert_monthly(self, conn, business_id: int, rows: list[dict]) -> None:
        for r in rows:
            await conn.execute(
                """
                INSERT INTO wh_form_monthly (
                    business_id, period_start,
                    submission_count, ready_count, complete_count, approved_count,
                    distinct_forms_used, distinct_customers_filling,
                    mom_submission_pct, yoy_submission_pct, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
                ON CONFLICT (business_id, period_start) DO UPDATE SET
                    submission_count           = EXCLUDED.submission_count,
                    ready_count                = EXCLUDED.ready_count,
                    complete_count             = EXCLUDED.complete_count,
                    approved_count             = EXCLUDED.approved_count,
                    distinct_forms_used        = EXCLUDED.distinct_forms_used,
                    distinct_customers_filling = EXCLUDED.distinct_customers_filling,
                    mom_submission_pct         = EXCLUDED.mom_submission_pct,
                    yoy_submission_pct         = EXCLUDED.yoy_submission_pct,
                    updated_at                 = NOW()
                """,
                business_id,
                _to_date(r["period_start"]),
                r.get("submission_count", 0),
                r.get("ready_count", 0),
                r.get("complete_count", 0),
                r.get("approved_count", 0),
                r.get("distinct_forms_used", 0),
                r.get("distinct_customers_filling", 0),
                r.get("mom_submission_pct"),
                r.get("yoy_submission_pct"),
            )

    async def _upsert_per_form(self, conn, business_id: int, rows: list[dict]) -> None:
        for r in rows:
            await conn.execute(
                """
                INSERT INTO wh_form_per_form_snapshot (
                    business_id, snapshot_date, form_id,
                    form_name, form_description, is_active, category_id,
                    template_created_at,
                    lifetime_submission_count, complete_count, approved_count, ready_count,
                    submissions_last_30d, submissions_last_90d,
                    most_recent_submission_at, distinct_customers,
                    is_dormant, is_active_dormant,
                    completion_rate_pct, rank_by_submissions, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18, $19, $20, NOW()
                )
                ON CONFLICT (business_id, snapshot_date, form_id) DO UPDATE SET
                    form_name                 = EXCLUDED.form_name,
                    form_description          = EXCLUDED.form_description,
                    is_active                 = EXCLUDED.is_active,
                    category_id               = EXCLUDED.category_id,
                    template_created_at       = EXCLUDED.template_created_at,
                    lifetime_submission_count = EXCLUDED.lifetime_submission_count,
                    complete_count            = EXCLUDED.complete_count,
                    approved_count            = EXCLUDED.approved_count,
                    ready_count               = EXCLUDED.ready_count,
                    submissions_last_30d      = EXCLUDED.submissions_last_30d,
                    submissions_last_90d      = EXCLUDED.submissions_last_90d,
                    most_recent_submission_at = EXCLUDED.most_recent_submission_at,
                    distinct_customers        = EXCLUDED.distinct_customers,
                    is_dormant                = EXCLUDED.is_dormant,
                    is_active_dormant         = EXCLUDED.is_active_dormant,
                    completion_rate_pct       = EXCLUDED.completion_rate_pct,
                    rank_by_submissions       = EXCLUDED.rank_by_submissions,
                    updated_at                = NOW()
                """,
                business_id,
                _to_date(r["snapshot_date"]),
                r["form_id"],
                r["form_name"],
                r.get("form_description"),
                r.get("is_active", True),
                r.get("category_id", 1),
                _to_datetime(r.get("template_created_at")),
                r.get("lifetime_submission_count", 0),
                r.get("complete_count", 0),
                r.get("approved_count", 0),
                r.get("ready_count", 0),
                r.get("submissions_last_30d", 0),
                r.get("submissions_last_90d", 0),
                _to_datetime(r.get("most_recent_submission_at")),
                r.get("distinct_customers", 0),
                r.get("is_dormant", False),
                r.get("is_active_dormant", False),
                r.get("completion_rate_pct"),
                r.get("rank_by_submissions", 999),
            )

    async def _upsert_lifecycle(self, conn, business_id: int, row: dict) -> None:
        # ⚠️ ALWAYS-EMIT — even when every count is 0, this row is written.
        await conn.execute(
            """
            INSERT INTO wh_form_lifecycle_snapshot (
                business_id, snapshot_date,
                total_submissions, ready_count, complete_count, approved_count,
                unknown_status_count, completion_rate_pct,
                stuck_ready_count, stuck_ready_total_age_days,
                most_recent_submission_at, stuck_ready_submission_ids, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW()
            )
            ON CONFLICT (business_id, snapshot_date) DO UPDATE SET
                total_submissions          = EXCLUDED.total_submissions,
                ready_count                = EXCLUDED.ready_count,
                complete_count             = EXCLUDED.complete_count,
                approved_count             = EXCLUDED.approved_count,
                unknown_status_count       = EXCLUDED.unknown_status_count,
                completion_rate_pct        = EXCLUDED.completion_rate_pct,
                stuck_ready_count          = EXCLUDED.stuck_ready_count,
                stuck_ready_total_age_days = EXCLUDED.stuck_ready_total_age_days,
                most_recent_submission_at  = EXCLUDED.most_recent_submission_at,
                stuck_ready_submission_ids = EXCLUDED.stuck_ready_submission_ids,
                updated_at                 = NOW()
            """,
            business_id,
            _to_date(row["snapshot_date"]),
            row.get("total_submissions", 0),
            row.get("ready_count", 0),
            row.get("complete_count", 0),
            row.get("approved_count", 0),
            row.get("unknown_status_count", 0),
            row.get("completion_rate_pct"),
            row.get("stuck_ready_count", 0),
            row.get("stuck_ready_total_age_days", 0),
            _to_datetime(row.get("most_recent_submission_at")),
            row.get("stuck_ready_submission_ids", []),
        )