"""
etl/transforms/giftcards_etl.py
================================
Gift Cards domain ETL extractor (Domain 9, Sprint 9).

Fetches data from the analytics backend via AnalyticsClient. Two outputs:
  1. Writes to the 8 wh_giftcard_* warehouse tables (idempotent persistence).
  2. Returns a dict of all fetched rows for immediate use by the doc generator.

Pattern matches clients_etl.py / promos_etl.py — return rows, doc gen reads
from the returned dict (NOT from the warehouse). Warehouse is for historical
persistence; doc gen consumes the in-memory rows.

Per Lesson 1 — verify warehouse landed before claiming success. We log:
    "warehouse write complete domain=giftcards business_id=N
     monthly=A liability=B staff=C location=D aging=E
     anomalies=F denomination=G health=H"

Per Lesson 17 — ETL is idempotent: each upsert uses INSERT ... ON CONFLICT
DO UPDATE inside a single transaction. Re-running on the same business_id +
date range overwrites cleanly without duplicates. New snapshot_date values
append historical rows.

Usage
-----
    extractor = GiftcardsExtractor(client=analytics_client, wh_pool=wh_pool)
    rows = await extractor.run(
        business_id=42,
        start_date=date(2025, 1, 1),
        end_date=date(2026, 3, 31),
        snapshot_date=date(2026, 3, 31),   # optional, defaults to end_date
    )
    # rows = {"monthly": [...], "liability": {...}, "by_staff": [...], ...}
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


class GiftcardsExtractor:
    """
    Extracts gift card data from the analytics backend, writes to the
    warehouse (idempotent), and returns the rows for doc generation.

    Parameters
    ----------
    client:   AnalyticsClient — exposes 8 get_giftcard_* methods.
    wh_pool:  Optional asyncpg pool. When None, warehouse write is skipped
              (test mode). When provided, all 8 tables get upserted inside
              a single transaction.
    """

    DOMAIN = "giftcards"

    def __init__(self, client: Any, wh_pool: Any = None) -> None:
        self._client = client
        self._wh = wh_pool

    # =========================================================================
    # Public entry point
    # =========================================================================

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        snapshot_date: date | None = None,
    ) -> dict:
        """
        Fetch all 8 endpoints, write to warehouse, return all rows.

        Returns
        -------
        dict with keys:
            monthly:        list[dict]   per-period redemption + activation
            liability:      dict         current liability snapshot
            by_staff:       list[dict]   per-(staff, period) redemption
            by_location:    list[dict]   per-(location, period) redemption
            aging:          list[dict]   5 rows: 4 buckets + 1 dormancy summary
            anomalies:      dict         always-emit anomalies snapshot
            denomination:   list[dict]   6 bucket rows
            health:         dict         lifetime population health
            snapshot_date:  date         the snapshot date used
            counts:         dict         row counts (logging/debugging)
        """
        snapshot_date = snapshot_date or end_date

        logger.info(
            "giftcards_etl.run business_id=%s start=%s end=%s snapshot=%s",
            business_id, start_date, end_date, snapshot_date,
        )

        # ── 1. Fetch all 8 endpoints ────────────────────────────────────────
        # Sequential (matches promos_etl). Switch to gather() if latency hurts.
        monthly      = await self._client.get_giftcard_monthly(
            business_id, start_date, end_date)
        liability    = await self._client.get_giftcard_liability_snapshot(
            business_id, snapshot_date)
        by_staff     = await self._client.get_giftcard_by_staff(
            business_id, start_date, end_date)
        by_location  = await self._client.get_giftcard_by_location(
            business_id, start_date, end_date)
        aging        = await self._client.get_giftcard_aging_snapshot(
            business_id, snapshot_date)
        anomalies    = await self._client.get_giftcard_anomalies_snapshot(
            business_id, snapshot_date, start_date, end_date)
        denomination = await self._client.get_giftcard_denomination_snapshot(
            business_id, snapshot_date)
        health       = await self._client.get_giftcard_health_snapshot(
            business_id, snapshot_date)

        counts = {
            "monthly":      len(monthly) if monthly else 0,
            "liability":    1 if liability else 0,
            "by_staff":     len(by_staff) if by_staff else 0,
            "by_location":  len(by_location) if by_location else 0,
            "aging":        len(aging) if aging else 0,
            "anomalies":    1 if anomalies else 0,
            "denomination": len(denomination) if denomination else 0,
            "health":       1 if health else 0,
        }

        logger.info(
            "giftcards_etl fetched — monthly=%d liability=%d staff=%d "
            "location=%d aging=%d anomalies=%d denomination=%d health=%d",
            counts["monthly"], counts["liability"], counts["by_staff"],
            counts["by_location"], counts["aging"], counts["anomalies"],
            counts["denomination"], counts["health"],
        )

        # ── 2. Write to warehouse (if pool provided) ────────────────────────
        if self._wh is not None:
            await self._write_to_warehouse(
                business_id   = business_id,
                snapshot_date = snapshot_date,
                start_date    = start_date,
                end_date      = end_date,
                monthly       = monthly,
                liability     = liability,
                by_staff      = by_staff,
                by_location   = by_location,
                aging         = aging,
                anomalies     = anomalies,
                denomination  = denomination,
                health        = health,
            )
            logger.info(
                "warehouse write complete domain=giftcards business_id=%s "
                "monthly=%d liability=%d staff=%d location=%d aging=%d "
                "anomalies=%d denomination=%d health=%d",
                business_id,
                counts["monthly"], counts["liability"], counts["by_staff"],
                counts["by_location"], counts["aging"], counts["anomalies"],
                counts["denomination"], counts["health"],
            )
        else:
            logger.debug("giftcards_etl: wh_pool not provided — skipping warehouse write")

        # ── 3. Return rows + counts for doc generator ───────────────────────
        return {
            "monthly":       monthly or [],
            "liability":     liability,
            "by_staff":      by_staff or [],
            "by_location":   by_location or [],
            "aging":         aging or [],
            "anomalies":     anomalies,
            "denomination":  denomination or [],
            "health":        health,
            "snapshot_date": snapshot_date,
            "counts":        counts,
        }

    # =========================================================================
    # Warehouse writes — one transaction, 8 upserts. All idempotent.
    # =========================================================================

    async def _write_to_warehouse(
        self,
        business_id: int,
        snapshot_date: date,
        start_date: date,
        end_date: date,
        monthly: list[dict],
        liability: dict,
        by_staff: list[dict],
        by_location: list[dict],
        aging: list[dict],
        anomalies: dict,
        denomination: list[dict],
        health: dict,
    ) -> None:
        async with self._wh.acquire() as conn:
            async with conn.transaction():
                await self._upsert_monthly(conn, business_id, monthly)
                await self._upsert_liability(conn, business_id, snapshot_date, liability)
                await self._upsert_by_staff(conn, business_id, by_staff)
                await self._upsert_by_location(conn, business_id, by_location)
                await self._upsert_aging(conn, business_id, snapshot_date, aging)
                await self._upsert_anomalies(conn, business_id, snapshot_date, start_date, end_date, anomalies)
                await self._upsert_denomination(conn, business_id, snapshot_date, denomination)
                await self._upsert_health(conn, business_id, snapshot_date, health)

    # ─── EP1: monthly ────────────────────────────────────────────────────────
    async def _upsert_monthly(self, conn, business_id: int, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_giftcard_monthly (
                business_id, period_start,
                redemption_count, redemption_amount_total, distinct_cards_redeemed,
                activation_count,
                weekend_redemption_count, weekday_redemption_count,
                avg_uplift_per_visit, uplift_total,
                mom_redemption_pct, mom_activation_pct, yoy_redemption_pct,
                generated_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW()
            )
            ON CONFLICT (business_id, period_start) DO UPDATE SET
                redemption_count          = EXCLUDED.redemption_count,
                redemption_amount_total   = EXCLUDED.redemption_amount_total,
                distinct_cards_redeemed   = EXCLUDED.distinct_cards_redeemed,
                activation_count          = EXCLUDED.activation_count,
                weekend_redemption_count  = EXCLUDED.weekend_redemption_count,
                weekday_redemption_count  = EXCLUDED.weekday_redemption_count,
                avg_uplift_per_visit      = EXCLUDED.avg_uplift_per_visit,
                uplift_total              = EXCLUDED.uplift_total,
                mom_redemption_pct        = EXCLUDED.mom_redemption_pct,
                mom_activation_pct        = EXCLUDED.mom_activation_pct,
                yoy_redemption_pct        = EXCLUDED.yoy_redemption_pct,
                generated_at              = NOW(),
                updated_at                = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                business_id,
                _parse_date(r["period_start"]),
                r["redemption_count"], r["redemption_amount_total"],
                r["distinct_cards_redeemed"], r["activation_count"],
                r["weekend_redemption_count"], r["weekday_redemption_count"],
                r["avg_uplift_per_visit"], r["uplift_total"],
                r.get("mom_redemption_pct"), r.get("mom_activation_pct"),
                r.get("yoy_redemption_pct"),
            )

    # ─── EP2: liability_snapshot ─────────────────────────────────────────────
    async def _upsert_liability(self, conn, business_id: int, snapshot_date: date,
                                  obj: dict) -> None:
        if not obj:
            return
        sql = """
            INSERT INTO wh_giftcard_liability_snapshot (
                business_id, snapshot_date,
                active_card_count, outstanding_liability_total,
                avg_remaining_balance_excl_drained, avg_remaining_balance_incl_drained,
                drained_active_count, median_remaining_balance,
                generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
            ON CONFLICT (business_id, snapshot_date) DO UPDATE SET
                active_card_count                  = EXCLUDED.active_card_count,
                outstanding_liability_total        = EXCLUDED.outstanding_liability_total,
                avg_remaining_balance_excl_drained = EXCLUDED.avg_remaining_balance_excl_drained,
                avg_remaining_balance_incl_drained = EXCLUDED.avg_remaining_balance_incl_drained,
                drained_active_count               = EXCLUDED.drained_active_count,
                median_remaining_balance           = EXCLUDED.median_remaining_balance,
                generated_at                       = NOW(),
                updated_at                         = NOW()
        """
        await conn.execute(
            sql, business_id,
            _parse_date(obj.get("snapshot_date")) or snapshot_date,
            obj["active_card_count"], obj["outstanding_liability_total"],
            obj["avg_remaining_balance_excl_drained"],
            obj["avg_remaining_balance_incl_drained"],
            obj["drained_active_count"], obj["median_remaining_balance"],
        )

    # ─── EP3: by_staff ───────────────────────────────────────────────────────
    async def _upsert_by_staff(self, conn, business_id: int, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_giftcard_by_staff (
                business_id, staff_id, staff_name, is_active, period_start,
                redemption_count, redemption_amount_total, distinct_cards_redeemed,
                rank_in_period, generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
            ON CONFLICT (business_id, staff_id, period_start) DO UPDATE SET
                staff_name              = EXCLUDED.staff_name,
                is_active               = EXCLUDED.is_active,
                redemption_count        = EXCLUDED.redemption_count,
                redemption_amount_total = EXCLUDED.redemption_amount_total,
                distinct_cards_redeemed = EXCLUDED.distinct_cards_redeemed,
                rank_in_period          = EXCLUDED.rank_in_period,
                generated_at            = NOW(),
                updated_at              = NOW()
        """
        for r in rows:
            await conn.execute(
                sql, business_id,
                r["staff_id"], r["staff_name"], r["is_active"],
                _parse_date(r["period_start"]),
                r["redemption_count"], r["redemption_amount_total"],
                r["distinct_cards_redeemed"], r["rank_in_period"],
            )

    # ─── EP4: by_location ────────────────────────────────────────────────────
    async def _upsert_by_location(self, conn, business_id: int, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_giftcard_by_location (
                business_id, location_id, location_name, period_start,
                redemption_count, redemption_amount_total, distinct_cards_redeemed,
                pct_of_org_redemption, mom_redemption_pct,
                generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
            ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
                location_name           = EXCLUDED.location_name,
                redemption_count        = EXCLUDED.redemption_count,
                redemption_amount_total = EXCLUDED.redemption_amount_total,
                distinct_cards_redeemed = EXCLUDED.distinct_cards_redeemed,
                pct_of_org_redemption   = EXCLUDED.pct_of_org_redemption,
                mom_redemption_pct      = EXCLUDED.mom_redemption_pct,
                generated_at            = NOW(),
                updated_at              = NOW()
        """
        for r in rows:
            await conn.execute(
                sql, business_id,
                r["location_id"], r["location_name"],
                _parse_date(r["period_start"]),
                r["redemption_count"], r["redemption_amount_total"],
                r["distinct_cards_redeemed"],
                r.get("pct_of_org_redemption"), r.get("mom_redemption_pct"),
            )

    # ─── EP5: aging_snapshot ─────────────────────────────────────────────────
    async def _upsert_aging(self, conn, business_id: int, snapshot_date: date,
                             rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_giftcard_aging_snapshot (
                business_id, snapshot_date, row_type, age_bucket,
                card_count, liability_amount, pct_of_total_liability,
                never_redeemed_in_bucket, avg_days_to_first_redemption,
                longest_dormant_card_id, longest_dormant_days,
                generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())
            ON CONFLICT (business_id, snapshot_date, age_bucket) DO UPDATE SET
                row_type                       = EXCLUDED.row_type,
                card_count                     = EXCLUDED.card_count,
                liability_amount               = EXCLUDED.liability_amount,
                pct_of_total_liability         = EXCLUDED.pct_of_total_liability,
                never_redeemed_in_bucket       = EXCLUDED.never_redeemed_in_bucket,
                avg_days_to_first_redemption   = EXCLUDED.avg_days_to_first_redemption,
                longest_dormant_card_id        = EXCLUDED.longest_dormant_card_id,
                longest_dormant_days           = EXCLUDED.longest_dormant_days,
                generated_at                   = NOW(),
                updated_at                     = NOW()
        """
        for r in rows:
            await conn.execute(
                sql, business_id, snapshot_date,
                r["row_type"], r["age_bucket"],
                r["card_count"], r["liability_amount"],
                r.get("pct_of_total_liability"),
                r["never_redeemed_in_bucket"],
                r.get("avg_days_to_first_redemption"),
                r.get("longest_dormant_card_id"), r.get("longest_dormant_days"),
            )

    # ─── EP6: anomalies_snapshot (ALWAYS-EMIT) ───────────────────────────────
    async def _upsert_anomalies(self, conn, business_id: int, snapshot_date: date,
                                  start_date: date, end_date: date, obj: dict) -> None:
        if not obj:
            obj = {
                "drained_active_count": 0, "drained_active_card_ids": [],
                "deactivated_count": 0, "deactivated_value_total_derived": 0,
                "refunded_redemption_count": 0, "refunded_redemption_amount": 0,
            }
        sql = """
            INSERT INTO wh_giftcard_anomalies_snapshot (
                business_id, snapshot_date,
                drained_active_count, drained_active_card_ids,
                deactivated_count, deactivated_value_total_derived,
                refunded_redemption_count, refunded_redemption_amount,
                period_start, period_end,
                generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
            ON CONFLICT (business_id, snapshot_date) DO UPDATE SET
                drained_active_count             = EXCLUDED.drained_active_count,
                drained_active_card_ids          = EXCLUDED.drained_active_card_ids,
                deactivated_count                = EXCLUDED.deactivated_count,
                deactivated_value_total_derived  = EXCLUDED.deactivated_value_total_derived,
                refunded_redemption_count        = EXCLUDED.refunded_redemption_count,
                refunded_redemption_amount       = EXCLUDED.refunded_redemption_amount,
                period_start                     = EXCLUDED.period_start,
                period_end                       = EXCLUDED.period_end,
                generated_at                     = NOW(),
                updated_at                       = NOW()
        """
        await conn.execute(
            sql, business_id, snapshot_date,
            obj["drained_active_count"],
            obj.get("drained_active_card_ids", []),
            obj["deactivated_count"], obj["deactivated_value_total_derived"],
            obj["refunded_redemption_count"], obj["refunded_redemption_amount"],
            start_date, end_date,
        )

    # ─── EP7: denomination_snapshot ──────────────────────────────────────────
    async def _upsert_denomination(self, conn, business_id: int, snapshot_date: date,
                                     rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT INTO wh_giftcard_denomination_snapshot (
                business_id, snapshot_date, denomination_bucket,
                card_count, total_value_issued, avg_face_value, pct_of_cards,
                generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
            ON CONFLICT (business_id, snapshot_date, denomination_bucket) DO UPDATE SET
                card_count           = EXCLUDED.card_count,
                total_value_issued   = EXCLUDED.total_value_issued,
                avg_face_value       = EXCLUDED.avg_face_value,
                pct_of_cards         = EXCLUDED.pct_of_cards,
                generated_at         = NOW(),
                updated_at           = NOW()
        """
        for r in rows:
            await conn.execute(
                sql, business_id, snapshot_date,
                r["denomination_bucket"], r["card_count"],
                r["total_value_issued"], r["avg_face_value"], r["pct_of_cards"],
            )

    # ─── EP8: health_snapshot ────────────────────────────────────────────────
    async def _upsert_health(self, conn, business_id: int, snapshot_date: date,
                              obj: dict) -> None:
        if not obj:
            return
        sql = """
            INSERT INTO wh_giftcard_health_snapshot (
                business_id, snapshot_date,
                total_cards_issued, cards_with_redemption, redemption_rate_pct,
                single_visit_drained_count, multi_visit_redeemed_count,
                single_visit_drained_pct_of_redeemed,
                multi_visit_redeemed_pct_of_redeemed,
                distinct_customer_redeemers,
                generated_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
            ON CONFLICT (business_id, snapshot_date) DO UPDATE SET
                total_cards_issued                    = EXCLUDED.total_cards_issued,
                cards_with_redemption                 = EXCLUDED.cards_with_redemption,
                redemption_rate_pct                   = EXCLUDED.redemption_rate_pct,
                single_visit_drained_count            = EXCLUDED.single_visit_drained_count,
                multi_visit_redeemed_count            = EXCLUDED.multi_visit_redeemed_count,
                single_visit_drained_pct_of_redeemed  = EXCLUDED.single_visit_drained_pct_of_redeemed,
                multi_visit_redeemed_pct_of_redeemed  = EXCLUDED.multi_visit_redeemed_pct_of_redeemed,
                distinct_customer_redeemers           = EXCLUDED.distinct_customer_redeemers,
                generated_at                          = NOW(),
                updated_at                            = NOW()
        """
        await conn.execute(
            sql, business_id, snapshot_date,
            obj["total_cards_issued"], obj["cards_with_redemption"],
            obj.get("redemption_rate_pct"),
            obj["single_visit_drained_count"], obj["multi_visit_redeemed_count"],
            obj.get("single_visit_drained_pct_of_redeemed"),
            obj.get("multi_visit_redeemed_pct_of_redeemed"),
            obj["distinct_customer_redeemers"],
        )


# =============================================================================
# Helpers
# =============================================================================

def _parse_date(value):
    """Accept date | str | None — return date or None."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])