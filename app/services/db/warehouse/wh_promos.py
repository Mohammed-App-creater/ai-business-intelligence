"""
app/services/db/warehouse/wh_promos.py
=======================================
Warehouse read-side access for the Promos domain.

Used by:
  - app/services/doc_generators/domains/promos.py
  - any analytics/reporting service that wants to read the warehouse directly

Pattern matches existing wh_revenue, wh_marketing, etc.

CRITICAL — Lesson 3 NULL handling:
  • wh_promo_codes can have period_start IS NULL (window-total rows)
  • wh_promo_catalog_health rows have no period at all
  • Doc generator must surface these with period_start=None to the embedder,
    and vector_store.search() must tolerate NULL period filters (already fixed
    globally during Services sprint).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


class WhPromos:
    """Warehouse access layer for promos data."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    # ---------------------------------------------------------------------
    # Reads — one method per warehouse table
    # ---------------------------------------------------------------------

    async def monthly(self, business_id: int) -> list[dict]:
        """All monthly rollup rows for a tenant. Ordered by period DESC."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT business_id, period_start, total_visits, promo_redemptions,
                       distinct_codes_used, promo_visit_pct, total_discount_given,
                       avg_discount_per_redemption, prev_month_redemptions,
                       prev_month_discount
                  FROM wh_promo_monthly
                 WHERE business_id = $1
              ORDER BY period_start DESC
                """,
                business_id,
            )
        return [dict(r) for r in rows]

    async def codes_monthly(self, business_id: int) -> list[dict]:
        """Per-code monthly rows. period_start IS NOT NULL."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT business_id, period_start, promo_id, promo_code_string,
                       promo_label, promo_amount_metadata, is_active,
                       expiration_date, redemptions, total_discount,
                       avg_discount, max_single_discount
                  FROM wh_promo_codes
                 WHERE business_id = $1
                   AND period_start IS NOT NULL
              ORDER BY period_start DESC, redemptions DESC
                """,
                business_id,
            )
        return [dict(r) for r in rows]

    async def codes_window(self, business_id: int) -> list[dict]:
        """Per-code window-total rows. period_start IS NULL."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT business_id, promo_id, promo_code_string, promo_label,
                       promo_amount_metadata, is_active, expiration_date,
                       redemptions AS total_redemptions, total_discount,
                       avg_discount, max_single_discount, is_expired_now
                  FROM wh_promo_codes
                 WHERE business_id = $1
                   AND period_start IS NULL
              ORDER BY redemptions DESC
                """,
                business_id,
            )
        return [dict(r) for r in rows]

    async def locations_rollup(self, business_id: int) -> list[dict]:
        """Per (period, location) rollup rows."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT business_id, period_start, location_id, location_name,
                       total_promo_redemptions, distinct_codes_used,
                       total_discount_given, avg_discount_per_redemption
                  FROM wh_promo_locations
                 WHERE business_id = $1
              ORDER BY period_start DESC, total_discount_given DESC
                """,
                business_id,
            )
        return [dict(r) for r in rows]

    async def location_codes(self, business_id: int) -> list[dict]:
        """Per (period, location, code) detail rows."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT business_id, period_start, location_id, location_name,
                       promo_id, promo_code_string, promo_label,
                       redemptions, total_discount, avg_discount
                  FROM wh_promo_location_codes
                 WHERE business_id = $1
              ORDER BY period_start DESC, location_id, redemptions DESC
                """,
                business_id,
            )
        return [dict(r) for r in rows]

    async def catalog_health(self, business_id: int) -> list[dict]:
        """Catalog state snapshot. No period — point-in-time."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT business_id, promo_id, promo_code_string, promo_label,
                       is_active, expiration_date, is_expired, active_but_expired,
                       redemptions_last_90d, is_dormant, snapshot_date
                  FROM wh_promo_catalog_health
                 WHERE business_id = $1
              ORDER BY is_dormant DESC, active_but_expired DESC, promo_id
                """,
                business_id,
            )
        return [dict(r) for r in rows]