"""
etl/transforms/promos_etl.py
=============================
Promos domain ETL extractor.

Pulls all 6 promo data slices from the analytics backend, writes them
to the warehouse (wh_promo_* tables), and returns structured docs for
immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  PromosExtractor.run()
        ↓  _write_to_warehouse()
    wh_promo_monthly              (per-period rollup)
    wh_promo_codes (monthly rows) (per-code per-period)
    wh_promo_codes (window rows)  (per-code window-total, period_start=NULL)
    wh_promo_locations            (per-location-per-period rollup)
    wh_promo_location_codes       (per-location-per-code per-period)
    wh_promo_catalog_health       (catalog snapshot — overwrite per run)
        ↓  returned to doc generator → pgvector

Usage (with warehouse write):
    extractor = PromosExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — tests):
    extractor = PromosExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Notes (from Step 2/3 spec):
  N1 — FK tbl_visit.PromoCode → tbl_promo.Id is unvalidated on dev (zero
       redemptions across all tenants). Orphans (NULL promo_code_string /
       promo_label) are passed through unchanged — doc generator handles
       NULL-safe rendering.
  N3 — promo_amount_metadata is the catalog-side Amount field. NEVER
       embedded as discount value. Discount is sourced from tbl_visit.Discount
       and arrives in *_discount_given columns from the API.
  Window rows: period_start arrives as NULL from the API. Per Lesson 3
       (catalog-style rows have period_start=NULL), we keep it that way
       in the warehouse. To avoid the PostgreSQL "NULL is distinct" UNIQUE
       quirk, _upsert_codes_window uses DELETE+INSERT for window rows.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class PromosExtractor:
    """
    Pulls and transforms all promos data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg pool — when provided, writes extracted
              rows to the warehouse before returning. When None, the
              warehouse write is skipped (useful in tests or when the
              warehouse is not yet available).
    """

    DOMAIN = "promos"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client = client
        self.wh_pool = wh_pool

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        Fetch all 6 promos slices, write to warehouse, return structured docs.

        Returns
        -------
        dict with keys:
            monthly:           list[dict] — per-period rollup
            codes_monthly:     list[dict] — per-code per-period
            codes_window:      list[dict] — per-code window-total (period=NULL)
            locations_rollup:  list[dict] — per-location per-period
            locations_by_code: list[dict] — per-location per-code per-period
            catalog_health:    list[dict] — catalog snapshot
            counts:            dict       — row counts per slice
        """
        logger.info(
            "PromosExtractor.run — business_id=%s, %s to %s",
            business_id, start_date, end_date,
        )

        # ── 1. Fetch all 6 slices in parallel ────────────────────────────────
        (
            monthly_raw,
            codes_monthly_raw,
            codes_window_raw,
            locs_rollup_raw,
            locs_by_code_raw,
            catalog_raw,
        ) = await asyncio.gather(
            self.client.get_promos_monthly(
                business_id=business_id,
                start_date=start_date,
                end_date=end_date,
            ),
            self.client.get_promos_codes_monthly(
                business_id=business_id,
                start_date=start_date,
                end_date=end_date,
            ),
            self.client.get_promos_codes_window(
                business_id=business_id,
                start_date=start_date,
                end_date=end_date,
            ),
            self.client.get_promos_locations_rollup(
                business_id=business_id,
                start_date=start_date,
                end_date=end_date,
            ),
            self.client.get_promos_locations_by_code(
                business_id=business_id,
                start_date=start_date,
                end_date=end_date,
            ),
            self.client.get_promos_catalog_health(
                business_id=business_id,
            ),
        )

        # ── 2. Transform — stamp business_id, normalize types ────────────────
        monthly_rows           = self._transform_monthly(business_id, monthly_raw)
        codes_monthly_rows     = self._transform_codes_monthly(business_id, codes_monthly_raw)
        codes_window_rows      = self._transform_codes_window(business_id, codes_window_raw)
        locations_rollup_rows  = self._transform_locations_rollup(business_id, locs_rollup_raw)
        locations_by_code_rows = self._transform_locations_by_code(business_id, locs_by_code_raw)
        catalog_rows           = self._transform_catalog(business_id, catalog_raw)

        counts = {
            "monthly":           len(monthly_rows),
            "codes_monthly":     len(codes_monthly_rows),
            "codes_window":      len(codes_window_rows),
            "locations_rollup":  len(locations_rollup_rows),
            "locations_by_code": len(locations_by_code_rows),
            "catalog_health":    len(catalog_rows),
        }

        logger.info(
            "PromosExtractor fetched — monthly=%d codes_m=%d codes_w=%d "
            "locs_r=%d locs_bc=%d catalog=%d",
            counts["monthly"],
            counts["codes_monthly"],
            counts["codes_window"],
            counts["locations_rollup"],
            counts["locations_by_code"],
            counts["catalog_health"],
        )

        # ── 3. Write to warehouse (if pool provided) ─────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(
                business_id,
                monthly_rows,
                codes_monthly_rows,
                codes_window_rows,
                locations_rollup_rows,
                locations_by_code_rows,
                catalog_rows,
            )
        else:
            logger.debug(
                "PromosExtractor: wh_pool not provided — skipping warehouse write"
            )

        return {
            "monthly":           monthly_rows,
            "codes_monthly":     codes_monthly_rows,
            "codes_window":      codes_window_rows,
            "locations_rollup":  locations_rollup_rows,
            "locations_by_code": locations_by_code_rows,
            "catalog_health":    catalog_rows,
            "counts":            counts,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Transforms — stamp business_id, normalize NULLable fields
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_monthly(self, business_id: int, raw: list[dict]) -> list[dict]:
        """EP1 → wh_promo_monthly. One row per period."""
        out = []
        for r in raw:
            out.append({
                "business_id":                 business_id,
                "period_start":                r.get("period_month") or r.get("period_start"),
                "total_visits":                int(r.get("total_visits") or 0),
                "promo_redemptions":           int(r.get("promo_redemptions") or 0),
                "distinct_codes_used":         int(r.get("distinct_codes_used") or 0),
                "promo_visit_pct":             r.get("promo_visit_pct"),
                "total_discount_given":        float(r.get("total_discount_given") or 0),
                "avg_discount_per_redemption": r.get("avg_discount_per_redemption"),
                "prev_month_redemptions":      r.get("prev_month_redemptions"),
                "prev_month_discount":         r.get("prev_month_discount"),
            })
        return out

    def _transform_codes_monthly(self, business_id: int, raw: list[dict]) -> list[dict]:
        """EP2A → wh_promo_codes (period_start NOT NULL)."""
        out = []
        for r in raw:
            out.append({
                "business_id":           business_id,
                "period_start":          r.get("period_month") or r.get("period_start"),
                "promo_id":              int(r["promo_id"]),
                "promo_code_string":     r.get("promo_code_string"),  # NULL for orphans
                "promo_label":           r.get("promo_label"),
                "promo_amount_metadata": r.get("promo_amount_metadata"),
                "is_active":             r.get("is_active"),
                "expiration_date":       r.get("expiration_date"),
                "redemptions":           int(r.get("redemptions") or 0),
                "total_discount":        float(r.get("total_discount") or 0),
                "avg_discount":          r.get("avg_discount"),
                "max_single_discount":   r.get("max_single_discount"),
                "is_expired_now":        None,  # only on window rows
            })
        return out

    def _transform_codes_window(self, business_id: int, raw: list[dict]) -> list[dict]:
        """EP2B → wh_promo_codes (period_start IS NULL — window-total snapshot)."""
        out = []
        for r in raw:
            out.append({
                "business_id":           business_id,
                "period_start":          None,                          # ← Lesson 3: window-total
                "promo_id":              int(r["promo_id"]),
                "promo_code_string":     r.get("promo_code_string"),
                "promo_label":           r.get("promo_label"),
                "promo_amount_metadata": r.get("promo_amount_metadata"),
                "is_active":             r.get("is_active"),
                "expiration_date":       r.get("expiration_date"),
                "redemptions":           int(r.get("redemptions") or 0),
                "total_discount":        float(r.get("total_discount") or 0),
                "avg_discount":          r.get("avg_discount"),
                "max_single_discount":   r.get("max_single_discount"),
                "is_expired_now":        r.get("is_expired_now"),
            })
        return out

    def _transform_locations_rollup(self, business_id: int, raw: list[dict]) -> list[dict]:
        """EP3-rollup → wh_promo_locations. One row per (period × location)."""
        out = []
        for r in raw:
            out.append({
                "business_id":                 business_id,
                "period_start":                r.get("period_month") or r.get("period_start"),
                "location_id":                 int(r["location_id"]),
                "location_name":               r.get("location_name"),
                "total_promo_redemptions":     int(r.get("total_promo_redemptions") or 0),
                "distinct_codes_used":         int(r.get("distinct_codes_used") or 0),
                "total_discount_given":        float(r.get("total_discount_given") or 0),
                "avg_discount_per_redemption": r.get("avg_discount_per_redemption"),
            })
        return out

    def _transform_locations_by_code(self, business_id: int, raw: list[dict]) -> list[dict]:
        """EP3-by_code → wh_promo_location_codes. One row per (period × loc × code)."""
        out = []
        for r in raw:
            out.append({
                "business_id":       business_id,
                "period_start":      r.get("period_month") or r.get("period_start"),
                "location_id":       int(r["location_id"]),
                "location_name":     r.get("location_name"),
                "promo_id":          int(r["promo_id"]),
                "promo_code_string": r.get("promo_code_string"),
                "promo_label":       r.get("promo_label"),
                "redemptions":       int(r.get("redemptions") or 0),
                "total_discount":    float(r.get("total_discount") or 0),
                "avg_discount":      r.get("avg_discount"),
            })
        return out

    def _transform_catalog(self, business_id: int, raw: list[dict]) -> list[dict]:
        """EP4 → wh_promo_catalog_health. One row per code (catalog snapshot)."""
        out = []
        for r in raw:
            out.append({
                "business_id":          business_id,
                "promo_id":             int(r["promo_id"]),
                "promo_code_string":    r.get("promo_code_string"),
                "promo_label":          r.get("promo_label"),
                "is_active":            r.get("is_active"),
                "expiration_date":      r.get("expiration_date"),
                "is_expired":           1 if r.get("is_expired") else 0,
                "active_but_expired":   1 if r.get("active_but_expired") else 0,
                "redemptions_last_90d": int(r.get("redemptions_last_90d") or 0),
                "is_dormant":           1 if r.get("is_dormant") else 0,
                "snapshot_date":        r.get("snapshot_date") or date.today(),
            })
        return out

    # ─────────────────────────────────────────────────────────────────────────
    # Warehouse write — 6 upsert methods. All idempotent.
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        business_id: int,
        monthly_rows: list[dict],
        codes_monthly_rows: list[dict],
        codes_window_rows: list[dict],
        locations_rollup_rows: list[dict],
        locations_by_code_rows: list[dict],
        catalog_rows: list[dict],
    ) -> None:
        """Upsert all 6 slices into wh_promo_* tables."""
        async with self.wh_pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_monthly(conn, monthly_rows)
                await self._upsert_codes_monthly(conn, codes_monthly_rows)
                await self._upsert_codes_window(conn, business_id, codes_window_rows)
                await self._upsert_locations_rollup(conn, locations_rollup_rows)
                await self._upsert_locations_by_code(conn, locations_by_code_rows)
                await self._upsert_catalog(conn, business_id, catalog_rows)
        logger.info(
            "PromosExtractor: warehouse write complete — "
            "monthly=%d codes_m=%d codes_w=%d locs_r=%d locs_bc=%d catalog=%d",
            len(monthly_rows), len(codes_monthly_rows), len(codes_window_rows),
            len(locations_rollup_rows), len(locations_by_code_rows), len(catalog_rows),
        )

    async def _upsert_monthly(self, conn, rows: list[dict]) -> None:
        """wh_promo_monthly — UNIQUE (business_id, period_start)."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_promo_monthly (
                business_id, period_start,
                total_visits, promo_redemptions, distinct_codes_used,
                promo_visit_pct, total_discount_given, avg_discount_per_redemption,
                prev_month_redemptions, prev_month_discount,
                generated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW()
            )
            ON CONFLICT (business_id, period_start) DO UPDATE SET
                total_visits                = EXCLUDED.total_visits,
                promo_redemptions           = EXCLUDED.promo_redemptions,
                distinct_codes_used         = EXCLUDED.distinct_codes_used,
                promo_visit_pct             = EXCLUDED.promo_visit_pct,
                total_discount_given        = EXCLUDED.total_discount_given,
                avg_discount_per_redemption = EXCLUDED.avg_discount_per_redemption,
                prev_month_redemptions      = EXCLUDED.prev_month_redemptions,
                prev_month_discount         = EXCLUDED.prev_month_discount,
                generated_at                = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], _parse_date(r.get("period_start")),
                r.get("total_visits"), r.get("promo_redemptions"),
                r.get("distinct_codes_used"),
                r.get("promo_visit_pct"), r.get("total_discount_given"),
                r.get("avg_discount_per_redemption"),
                r.get("prev_month_redemptions"), r.get("prev_month_discount"),
            )

    async def _upsert_codes_monthly(self, conn, rows: list[dict]) -> None:
        """wh_promo_codes (monthly variant) — UNIQUE (business_id, promo_id, period_start).
        Only for rows where period_start IS NOT NULL — window rows go through
        a separate DELETE+INSERT path."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_promo_codes (
                business_id, period_start, promo_id,
                promo_code_string, promo_label, promo_amount_metadata,
                is_active, expiration_date,
                redemptions, total_discount, avg_discount, max_single_discount,
                is_expired_now, generated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW()
            )
            ON CONFLICT (business_id, promo_id, period_start) DO UPDATE SET
                promo_code_string     = EXCLUDED.promo_code_string,
                promo_label           = EXCLUDED.promo_label,
                promo_amount_metadata = EXCLUDED.promo_amount_metadata,
                is_active             = EXCLUDED.is_active,
                expiration_date       = EXCLUDED.expiration_date,
                redemptions           = EXCLUDED.redemptions,
                total_discount        = EXCLUDED.total_discount,
                avg_discount          = EXCLUDED.avg_discount,
                max_single_discount   = EXCLUDED.max_single_discount,
                is_expired_now        = EXCLUDED.is_expired_now,
                generated_at          = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], _parse_date(r.get("period_start")),
                r["promo_id"],
                r.get("promo_code_string"), r.get("promo_label"),
                r.get("promo_amount_metadata"),
                r.get("is_active"), _parse_date(r.get("expiration_date")),
                r.get("redemptions"), r.get("total_discount"),
                r.get("avg_discount"), r.get("max_single_discount"),
                r.get("is_expired_now"),
            )

    async def _upsert_codes_window(
        self, conn, business_id: int, rows: list[dict],
    ) -> None:
        """wh_promo_codes (window variant — period_start IS NULL).

        WHY DELETE+INSERT: PostgreSQL treats NULL as distinct in UNIQUE
        constraints, so ON CONFLICT (business_id, promo_id, period_start)
        cannot dedupe rows where period_start IS NULL. We re-snapshot the
        window-total set on every ETL run anyway (snapshot semantics —
        same as wh_promo_catalog_health), so DELETE+INSERT is correct.
        """
        # Always run the DELETE — even if rows is empty — to clear stale snapshot
        await conn.execute(
            "DELETE FROM wh_promo_codes "
            "WHERE business_id = $1 AND period_start IS NULL",
            business_id,
        )
        if not rows:
            return
        sql = """
            INSERT INTO wh_promo_codes (
                business_id, period_start, promo_id,
                promo_code_string, promo_label, promo_amount_metadata,
                is_active, expiration_date,
                redemptions, total_discount, avg_discount, max_single_discount,
                is_expired_now, generated_at
            ) VALUES (
                $1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW()
            )
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], r["promo_id"],
                r.get("promo_code_string"), r.get("promo_label"),
                r.get("promo_amount_metadata"),
                r.get("is_active"), _parse_date(r.get("expiration_date")),
                r.get("redemptions"), r.get("total_discount"),
                r.get("avg_discount"), r.get("max_single_discount"),
                r.get("is_expired_now"),
            )

    async def _upsert_locations_rollup(self, conn, rows: list[dict]) -> None:
        """wh_promo_locations — UNIQUE (business_id, period_start, location_id)."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_promo_locations (
                business_id, period_start, location_id, location_name,
                total_promo_redemptions, distinct_codes_used,
                total_discount_given, avg_discount_per_redemption,
                generated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, NOW()
            )
            ON CONFLICT (business_id, period_start, location_id) DO UPDATE SET
                location_name               = EXCLUDED.location_name,
                total_promo_redemptions     = EXCLUDED.total_promo_redemptions,
                distinct_codes_used         = EXCLUDED.distinct_codes_used,
                total_discount_given        = EXCLUDED.total_discount_given,
                avg_discount_per_redemption = EXCLUDED.avg_discount_per_redemption,
                generated_at                = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], _parse_date(r.get("period_start")),
                r["location_id"], r.get("location_name"),
                r.get("total_promo_redemptions"), r.get("distinct_codes_used"),
                r.get("total_discount_given"),
                r.get("avg_discount_per_redemption"),
            )

    async def _upsert_locations_by_code(self, conn, rows: list[dict]) -> None:
        """wh_promo_location_codes — UNIQUE (business_id, period_start, location_id, promo_id)."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_promo_location_codes (
                business_id, period_start, location_id, location_name,
                promo_id, promo_code_string, promo_label,
                redemptions, total_discount, avg_discount,
                generated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW()
            )
            ON CONFLICT (business_id, period_start, location_id, promo_id) DO UPDATE SET
                location_name     = EXCLUDED.location_name,
                promo_code_string = EXCLUDED.promo_code_string,
                promo_label       = EXCLUDED.promo_label,
                redemptions       = EXCLUDED.redemptions,
                total_discount    = EXCLUDED.total_discount,
                avg_discount      = EXCLUDED.avg_discount,
                generated_at      = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], _parse_date(r.get("period_start")),
                r["location_id"], r.get("location_name"),
                r["promo_id"], r.get("promo_code_string"), r.get("promo_label"),
                r.get("redemptions"), r.get("total_discount"),
                r.get("avg_discount"),
            )

    async def _upsert_catalog(
        self, conn, business_id: int, rows: list[dict],
    ) -> None:
        """wh_promo_catalog_health — UNIQUE (business_id, promo_id).

        Snapshot semantics: every ETL run should reflect the current catalog
        only. DELETE-then-INSERT for the business so we don't carry around
        stale rows for codes that were removed from the source catalog.
        (ON CONFLICT alone would leave stale rows behind.)
        """
        await conn.execute(
            "DELETE FROM wh_promo_catalog_health WHERE business_id = $1",
            business_id,
        )
        if not rows:
            return
        sql = """
            INSERT INTO wh_promo_catalog_health (
                business_id, promo_id,
                promo_code_string, promo_label,
                is_active, expiration_date,
                is_expired, active_but_expired,
                redemptions_last_90d, is_dormant,
                snapshot_date, generated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW()
            )
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], r["promo_id"],
                r.get("promo_code_string"), r.get("promo_label"),
                r.get("is_active"), _parse_date(r.get("expiration_date")),
                r.get("is_expired"), r.get("active_but_expired"),
                r.get("redemptions_last_90d"), r.get("is_dormant"),
                _parse_date(r.get("snapshot_date")),
            )


# ── module-level helper (outside class so it's picklable) ────────────────────
def _parse_date(v):
    """Accept date, string, or None; return date or None."""
    if v is None:
        return None
    if isinstance(v, date):
        return v
    s = str(v)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None