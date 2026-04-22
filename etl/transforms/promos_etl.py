"""
etl/transforms/promos_etl.py
=============================
Promos domain ETL extractor (Domain 8).

Pulls data from the analytics backend via AnalyticsClient and writes it to
the 5 warehouse tables. Doc generator reads from the warehouse afterward.

Pattern matches existing domain extractors (revenue_etl, marketing_etl, etc.).

Per Lesson 1 — verify warehouse landed before claiming success. We log:
    "warehouse write complete domain=promos business_id=N
     monthly=A codes=B locations=C location_codes=D catalog_health=E"

Per Lesson 17 — ETL is idempotent: existing rows are upserted via DELETE+INSERT
inside a transaction, so re-running doesn't duplicate.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


class PromosExtractor:
    """
    Extracts promo data from the analytics backend and lands it in the warehouse.

    Usage
    -----
        extractor = PromosExtractor(analytics_client, wh_pool)
        await extractor.run(business_id=42, start_date=..., end_date=...)
    """

    DOMAIN = "promos"

    def __init__(self, analytics_client: Any, wh_pool: Any) -> None:
        self._client = analytics_client
        self._wh = wh_pool

    # ---------------------------------------------------------------------
    # Public entry point
    # ---------------------------------------------------------------------

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> dict[str, int]:
        """
        Fetch all 4 endpoints and write to the 5 warehouse tables.

        Returns counts dict: {monthly, codes_monthly, codes_window,
                              locations_rollup, locations_by_code, catalog_health}.
        """
        logger.info(
            "promos_etl.start business_id=%s start=%s end=%s",
            business_id, start_date, end_date,
        )

        # Fetch in parallel where possible (4 endpoints, 5 calls because codes
        # has two granularities). Keep sequential for now — matches the
        # marketing_etl pattern; we can optimize later if needed.

        monthly       = await self._client.get_promos_monthly(
            business_id, start_date, end_date,
        )
        codes_monthly = await self._client.get_promos_codes(
            business_id, start_date, end_date, granularity="monthly",
        )
        codes_window  = await self._client.get_promos_codes(
            business_id, start_date, end_date, granularity="window",
        )
        loc_rollup    = await self._client.get_promos_locations(
            business_id, start_date, end_date, shape="rollup",
        )
        loc_by_code   = await self._client.get_promos_locations(
            business_id, start_date, end_date, shape="by_code",
        )
        catalog       = await self._client.get_promos_catalog_health(business_id)

        # Orphan tracking — Lesson per N1 (FK assumption unvalidated on dev)
        orphan_count = sum(
            1 for r in codes_monthly + codes_window + loc_by_code
            if r.get("promo_code_string") is None and r.get("promo_id") is not None
        )
        if orphan_count > 0:
            logger.warning(
                "promos_etl.orphans_detected business_id=%s orphan_count=%d "
                "(promo_ids referenced in tbl_visit but missing from tbl_promo)",
                business_id, orphan_count,
            )

        # Write to warehouse — single transaction per logical group
        async with self._wh.acquire() as conn:
            async with conn.transaction():
                m_n  = await self._write_monthly(conn, business_id, monthly)
                cm_n = await self._write_codes(conn, business_id, codes_monthly,
                                                granularity="monthly")
                cw_n = await self._write_codes(conn, business_id, codes_window,
                                                granularity="window")
                lr_n = await self._write_locations(conn, business_id, loc_rollup)
                lc_n = await self._write_location_codes(conn, business_id, loc_by_code)
                ch_n = await self._write_catalog_health(conn, business_id, catalog)

        # Per Lesson 1 — explicit success log with all counts
        logger.info(
            "warehouse write complete domain=promos business_id=%s "
            "monthly=%d codes_monthly=%d codes_window=%d "
            "locations_rollup=%d locations_by_code=%d catalog_health=%d "
            "orphans=%d",
            business_id, m_n, cm_n, cw_n, lr_n, lc_n, ch_n, orphan_count,
        )

        return {
            "monthly":           m_n,
            "codes_monthly":     cm_n,
            "codes_window":      cw_n,
            "locations_rollup":  lr_n,
            "locations_by_code": lc_n,
            "catalog_health":    ch_n,
            "orphans":           orphan_count,
        }

    # ---------------------------------------------------------------------
    # Per-table writers — DELETE + INSERT for idempotency
    # ---------------------------------------------------------------------

    async def _write_monthly(self, conn, business_id, rows) -> int:
        await conn.execute(
            "DELETE FROM wh_promo_monthly WHERE business_id = $1",
            business_id,
        )
        if not rows:
            return 0
        await conn.executemany(
            """
            INSERT INTO wh_promo_monthly (
                business_id, period_start, total_visits, promo_redemptions,
                distinct_codes_used, promo_visit_pct, total_discount_given,
                avg_discount_per_redemption, prev_month_redemptions, prev_month_discount
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            [
                (
                    business_id,
                    self._parse_date(r["period_month"]),
                    r["total_visits"],
                    r["promo_redemptions"],
                    r["distinct_codes_used"],
                    r["promo_visit_pct"],
                    r["total_discount_given"],
                    r.get("avg_discount_per_redemption"),
                    r.get("prev_month_redemptions"),
                    r.get("prev_month_discount"),
                )
                for r in rows
            ],
        )
        return len(rows)

    async def _write_codes(self, conn, business_id, rows, granularity) -> int:
        # For 'monthly' granularity we delete only monthly rows;
        # for 'window' we delete only the period_start IS NULL rows.
        if granularity == "monthly":
            await conn.execute(
                "DELETE FROM wh_promo_codes "
                "WHERE business_id = $1 AND period_start IS NOT NULL",
                business_id,
            )
        else:  # window
            await conn.execute(
                "DELETE FROM wh_promo_codes "
                "WHERE business_id = $1 AND period_start IS NULL",
                business_id,
            )

        if not rows:
            return 0

        await conn.executemany(
            """
            INSERT INTO wh_promo_codes (
                business_id, period_start, promo_id, promo_code_string,
                promo_label, promo_amount_metadata, is_active, expiration_date,
                redemptions, total_discount, avg_discount, max_single_discount,
                is_expired_now
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """,
            [
                (
                    business_id,
                    # PR2 Part A has period_month; Part B (window) has no period
                    self._parse_date(r.get("period_month")) if granularity == "monthly" else None,
                    r["promo_id"],
                    r.get("promo_code_string"),
                    r.get("promo_label"),
                    r.get("promo_amount_metadata"),
                    r.get("is_active"),
                    self._parse_date(r.get("expiration_date")),
                    # window granularity uses 'total_redemptions'; monthly uses 'redemptions'
                    r.get("total_redemptions") if granularity == "window" else r.get("redemptions", 0),
                    r.get("total_discount", 0),
                    r.get("avg_discount"),
                    r.get("max_single_discount"),
                    r.get("is_expired_now"),
                )
                for r in rows
            ],
        )
        return len(rows)

    async def _write_locations(self, conn, business_id, rows) -> int:
        await conn.execute(
            "DELETE FROM wh_promo_locations WHERE business_id = $1",
            business_id,
        )
        if not rows:
            return 0
        await conn.executemany(
            """
            INSERT INTO wh_promo_locations (
                business_id, period_start, location_id, location_name,
                total_promo_redemptions, distinct_codes_used,
                total_discount_given, avg_discount_per_redemption
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            [
                (
                    business_id,
                    self._parse_date(r["period_month"]),
                    r["location_id"],
                    r.get("location_name"),
                    r["total_promo_redemptions"],
                    r["distinct_codes_used"],
                    r["total_discount_given"],
                    r.get("avg_discount_per_redemption"),
                )
                for r in rows
            ],
        )
        return len(rows)

    async def _write_location_codes(self, conn, business_id, rows) -> int:
        await conn.execute(
            "DELETE FROM wh_promo_location_codes WHERE business_id = $1",
            business_id,
        )
        if not rows:
            return 0
        await conn.executemany(
            """
            INSERT INTO wh_promo_location_codes (
                business_id, period_start, location_id, location_name,
                promo_id, promo_code_string, promo_label,
                redemptions, total_discount, avg_discount
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            [
                (
                    business_id,
                    self._parse_date(r["period_month"]),
                    r["location_id"],
                    r.get("location_name"),
                    r["promo_id"],
                    r.get("promo_code_string"),
                    r.get("promo_label"),
                    r["redemptions"],
                    r["total_discount"],
                    r.get("avg_discount"),
                )
                for r in rows
            ],
        )
        return len(rows)

    async def _write_catalog_health(self, conn, business_id, rows) -> int:
        await conn.execute(
            "DELETE FROM wh_promo_catalog_health WHERE business_id = $1",
            business_id,
        )
        if not rows:
            return 0

        # snapshot_date = today (Python side, since EP4 returns the field)
        from datetime import date as date_cls
        snapshot_date = date_cls.today()

        await conn.executemany(
            """
            INSERT INTO wh_promo_catalog_health (
                business_id, promo_id, promo_code_string, promo_label,
                is_active, expiration_date, is_expired, active_but_expired,
                redemptions_last_90d, is_dormant, snapshot_date
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
            [
                (
                    business_id,
                    r["promo_id"],
                    r.get("promo_code_string"),
                    r.get("promo_label"),
                    r.get("is_active"),
                    self._parse_date(r.get("expiration_date")),
                    r.get("is_expired", 0),
                    r.get("active_but_expired", 0),
                    r.get("redemptions_last_90d", 0),
                    r.get("is_dormant", 0),
                    snapshot_date,
                )
                for r in rows
            ],
        )
        return len(rows)

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    @staticmethod
    def _parse_date(value):
        """Parse ISO date string to date object. None passes through."""
        if value is None or value == "":
            return None
        if isinstance(value, date):
            return value
        # ISO string YYYY-MM-DD (Lesson 2 — strict ISO)
        return date.fromisoformat(str(value)[:10])