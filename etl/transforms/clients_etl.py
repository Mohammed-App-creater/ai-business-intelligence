"""
etl/transforms/clients_etl.py
==============================
Clients domain ETL extractor.

Pulls all 3 client data slices from the analytics backend, writes them
to the warehouse (wh_client_* tables), and returns structured docs for
immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  ClientsExtractor.run()
        ↓  _write_to_warehouse()
    wh_client_retention               (per-client per-period)
    wh_client_cohort_monthly          (per-period aggregates)
    wh_client_per_location_monthly    (per-location per-period)
        ↓  returned to doc generator → pgvector

Usage (with warehouse write):
    extractor = ClientsExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — tests):
    extractor = ClientsExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

PII policy: this extractor passes include_names=False on every call to
EP1. first_name/last_name are dropped at the API layer and never enter
the warehouse on the RAG path. An ops-tools path can call include_names=
True separately for CSV export — that's outside the RAG pipeline.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Optional

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class ClientsExtractor:
    """
    Pulls and transforms all clients data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg pool — when provided, writes extracted
              rows to the warehouse before returning. When None, the
              warehouse write is skipped (useful in tests or when the
              warehouse is not yet available).
    """

    DOMAIN = "clients"

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
        Fetch all 3 clients slices, write to warehouse, return structured docs.

        Returns
        -------
        dict with keys:
            retention_snapshot: list[dict]   — per-client rows
            cohort_monthly:     list[dict]   — per-period rows
            per_location:       list[dict]   — per-location-per-period rows
            counts:             dict         — row counts per slice
        """
        logger.info(
            "ClientsExtractor.run — business_id=%s, %s to %s",
            business_id, start_date, end_date,
        )

        # ── 1. Resolve period params for the 3 endpoints ─────────────────────
        # EP1 takes period_start/period_end bounding the "current period"
        # (typically the most recent complete month).
        # EP2/EP3 take start_month/end_month as a RANGE of months.
        # ref_date is the date treated as "today" — for tests, this is fixed.

        period_start = self._first_of_month(end_date)
        period_end = self._last_of_month(end_date)
        ref_date = end_date + timedelta(days=1)   # "today" = day after end

        start_month = self._first_of_month(start_date)
        end_month = self._first_of_month(end_date)

        # ── 2. Fetch all 3 slices in parallel ────────────────────────────────
        snapshot_task = self.client.get_clients_retention_snapshot(
            business_id=business_id,
            period_start=period_start,
            period_end=period_end,
            ref_date=ref_date,
            include_names=False,                  # ← PII guard
        )
        cohort_task = self.client.get_clients_cohort_monthly(
            business_id=business_id,
            start_month=start_month,
            end_month=end_month,
            ref_date=ref_date,
        )
        per_loc_task = self.client.get_clients_per_location_monthly(
            business_id=business_id,
            start_month=start_month,
            end_month=end_month,
        )

        snapshot_rows, cohort_rows, per_loc_rows = await asyncio.gather(
            snapshot_task, cohort_task, per_loc_task
        )

        # ── 3. Transform — stamp business_id + period, drop names ────────────
        snapshot_rows = self._transform_snapshot(
            business_id, period_start, snapshot_rows
        )
        cohort_rows = self._transform_cohort(business_id, cohort_rows)
        per_loc_rows = self._transform_per_location(business_id, per_loc_rows)

        counts = {
            "retention_snapshot": len(snapshot_rows),
            "cohort_monthly":     len(cohort_rows),
            "per_location":       len(per_loc_rows),
        }

        logger.info(
            "ClientsExtractor fetched — snapshot=%d cohort=%d per_loc=%d",
            counts["retention_snapshot"],
            counts["cohort_monthly"],
            counts["per_location"],
        )

        # ── 4. Write to warehouse (if pool provided) ─────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(
                snapshot_rows, cohort_rows, per_loc_rows
            )
        else:
            logger.debug(
                "ClientsExtractor: wh_pool not provided — skipping warehouse write"
            )

        return {
            "retention_snapshot": snapshot_rows,
            "cohort_monthly":     cohort_rows,
            "per_location":       per_loc_rows,
            "counts":             counts,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Transforms — strip PII, stamp period/business_id, coerce types
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_snapshot(
        self,
        business_id: int,
        period: date,
        rows: list[dict],
    ) -> list[dict]:
        """Per-client rows — stamp period, drop PII if leaked."""
        out = []
        for r in rows:
            r.setdefault("business_id", business_id)
            # Stamp period (historical-per-period storage per Step 3 §7.2)
            r["period"] = period.isoformat()
            # Defense in depth — even if include_names=False didn't strip:
            r["first_name"] = None
            r["last_name"] = None
            out.append(r)
        return out

    def _transform_cohort(
        self,
        business_id: int,
        rows: list[dict],
    ) -> list[dict]:
        for r in rows:
            r.setdefault("business_id", business_id)
        return rows

    def _transform_per_location(
        self,
        business_id: int,
        rows: list[dict],
    ) -> list[dict]:
        for r in rows:
            r.setdefault("business_id", business_id)
        return rows

    # ─────────────────────────────────────────────────────────────────────────
    # Warehouse write — 3 upsert methods, one per table. Idempotent.
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        snapshot_rows: list[dict],
        cohort_rows: list[dict],
        per_loc_rows: list[dict],
    ) -> None:
        """Upsert all rows into 3 wh_client_* tables."""
        async with self.wh_pool.acquire() as conn:
            async with conn.transaction():
                await self._upsert_snapshot(conn, snapshot_rows)
                await self._upsert_cohort(conn, cohort_rows)
                await self._upsert_per_location(conn, per_loc_rows)
        logger.info(
            "ClientsExtractor: warehouse write complete — "
            "snapshot=%d cohort=%d per_loc=%d",
            len(snapshot_rows), len(cohort_rows), len(per_loc_rows),
        )

    async def _upsert_snapshot(self, conn, rows: list[dict]) -> None:
        """Upsert into wh_client_retention. PK = (business_id, client_id, period)."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_client_retention (
                business_id, client_id, period,
                age, age_bracket, points,
                first_visit_ever_date, last_visit_date, days_since_last_visit,
                total_visits_ever, visits_in_period,
                lifetime_revenue, lifetime_tips, lifetime_total_paid,
                revenue_in_period, avg_ticket,
                home_location_id, home_location_name,
                first_visit_location_id, first_visit_location_name,
                is_not_deleted, is_reachable_email, is_reachable_sms,
                is_member, is_new_in_period, is_returning_in_period,
                is_reactivated_in_period, at_risk_flag,
                ltv_rank, frequency_rank, points_rank, ltv_percentile_decile,
                etl_run_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, NOW()
            )
            ON CONFLICT (business_id, client_id, period) DO UPDATE SET
                age                       = EXCLUDED.age,
                age_bracket               = EXCLUDED.age_bracket,
                points                    = EXCLUDED.points,
                first_visit_ever_date     = EXCLUDED.first_visit_ever_date,
                last_visit_date           = EXCLUDED.last_visit_date,
                days_since_last_visit     = EXCLUDED.days_since_last_visit,
                total_visits_ever         = EXCLUDED.total_visits_ever,
                visits_in_period          = EXCLUDED.visits_in_period,
                lifetime_revenue          = EXCLUDED.lifetime_revenue,
                lifetime_tips             = EXCLUDED.lifetime_tips,
                lifetime_total_paid       = EXCLUDED.lifetime_total_paid,
                revenue_in_period         = EXCLUDED.revenue_in_period,
                avg_ticket                = EXCLUDED.avg_ticket,
                home_location_id          = EXCLUDED.home_location_id,
                home_location_name        = EXCLUDED.home_location_name,
                first_visit_location_id   = EXCLUDED.first_visit_location_id,
                first_visit_location_name = EXCLUDED.first_visit_location_name,
                is_not_deleted            = EXCLUDED.is_not_deleted,
                is_reachable_email        = EXCLUDED.is_reachable_email,
                is_reachable_sms          = EXCLUDED.is_reachable_sms,
                is_member                 = EXCLUDED.is_member,
                is_new_in_period          = EXCLUDED.is_new_in_period,
                is_returning_in_period    = EXCLUDED.is_returning_in_period,
                is_reactivated_in_period  = EXCLUDED.is_reactivated_in_period,
                at_risk_flag              = EXCLUDED.at_risk_flag,
                ltv_rank                  = EXCLUDED.ltv_rank,
                frequency_rank            = EXCLUDED.frequency_rank,
                points_rank               = EXCLUDED.points_rank,
                ltv_percentile_decile     = EXCLUDED.ltv_percentile_decile,
                etl_run_at                = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], r["client_id"],
                _parse_date(r.get("period")),
                r.get("age"), r.get("age_bracket"), r.get("points"),
                _parse_date(r.get("first_visit_ever_date")),
                _parse_date(r.get("last_visit_date")),
                r.get("days_since_last_visit"),
                r.get("total_visits_ever"), r.get("visits_in_period"),
                r.get("lifetime_revenue"), r.get("lifetime_tips"),
                r.get("lifetime_total_paid"),
                r.get("revenue_in_period"), r.get("avg_ticket"),
                r.get("home_location_id"), r.get("home_location_name"),
                r.get("first_visit_location_id"),
                r.get("first_visit_location_name"),
                bool(r.get("is_not_deleted")),
                bool(r.get("is_reachable_email")),
                bool(r.get("is_reachable_sms")),
                bool(r.get("is_member")),
                bool(r.get("is_new_in_period")),
                bool(r.get("is_returning_in_period")),
                bool(r.get("is_reactivated_in_period")),
                bool(r.get("at_risk_flag")),
                r.get("ltv_rank"), r.get("frequency_rank"),
                r.get("points_rank"), r.get("ltv_percentile_decile"),
            )

    async def _upsert_cohort(self, conn, rows: list[dict]) -> None:
        """Upsert into wh_client_cohort_monthly. PK = (business_id, period)."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_client_cohort_monthly (
                business_id, period,
                clients_total, new_clients, returning_clients,
                reactivated_clients, active_clients_in_period,
                at_risk_clients, active_members,
                reachable_email, reachable_sms,
                total_revenue_in_period, unique_visitors_in_period,
                prev_new_clients, prev_at_risk_clients, prev_active_clients,
                new_clients_mom_pct, at_risk_mom_pct,
                new_vs_returning_split, retention_rate_pct,
                churn_rate_pct, member_overlap_pct, top10pct_revenue_share,
                etl_run_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, NOW()
            )
            ON CONFLICT (business_id, period) DO UPDATE SET
                clients_total              = EXCLUDED.clients_total,
                new_clients                = EXCLUDED.new_clients,
                returning_clients          = EXCLUDED.returning_clients,
                reactivated_clients        = EXCLUDED.reactivated_clients,
                active_clients_in_period   = EXCLUDED.active_clients_in_period,
                at_risk_clients            = EXCLUDED.at_risk_clients,
                active_members             = EXCLUDED.active_members,
                reachable_email            = EXCLUDED.reachable_email,
                reachable_sms              = EXCLUDED.reachable_sms,
                total_revenue_in_period    = EXCLUDED.total_revenue_in_period,
                unique_visitors_in_period  = EXCLUDED.unique_visitors_in_period,
                prev_new_clients           = EXCLUDED.prev_new_clients,
                prev_at_risk_clients       = EXCLUDED.prev_at_risk_clients,
                prev_active_clients        = EXCLUDED.prev_active_clients,
                new_clients_mom_pct        = EXCLUDED.new_clients_mom_pct,
                at_risk_mom_pct            = EXCLUDED.at_risk_mom_pct,
                new_vs_returning_split     = EXCLUDED.new_vs_returning_split,
                retention_rate_pct         = EXCLUDED.retention_rate_pct,
                churn_rate_pct             = EXCLUDED.churn_rate_pct,
                member_overlap_pct         = EXCLUDED.member_overlap_pct,
                top10pct_revenue_share     = EXCLUDED.top10pct_revenue_share,
                etl_run_at                 = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], _parse_date(r.get("period")),
                r.get("clients_total"), r.get("new_clients"),
                r.get("returning_clients"), r.get("reactivated_clients"),
                r.get("active_clients_in_period"), r.get("at_risk_clients"),
                r.get("active_members"),
                r.get("reachable_email"), r.get("reachable_sms"),
                r.get("total_revenue_in_period"),
                r.get("unique_visitors_in_period"),
                r.get("prev_new_clients"), r.get("prev_at_risk_clients"),
                r.get("prev_active_clients"),
                r.get("new_clients_mom_pct"), r.get("at_risk_mom_pct"),
                r.get("new_vs_returning_split"), r.get("retention_rate_pct"),
                r.get("churn_rate_pct"), r.get("member_overlap_pct"),
                r.get("top10pct_revenue_share"),
            )

    async def _upsert_per_location(self, conn, rows: list[dict]) -> None:
        """Upsert into wh_client_per_location_monthly.
           PK = (business_id, period, location_id)."""
        if not rows:
            return
        sql = """
            INSERT INTO wh_client_per_location_monthly (
                business_id, period, location_id, location_name,
                new_clients_here, clients_homed_here, active_clients_here,
                revenue_here, rank_by_new_clients, rank_by_active_clients,
                etl_run_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW()
            )
            ON CONFLICT (business_id, period, location_id) DO UPDATE SET
                location_name           = EXCLUDED.location_name,
                new_clients_here        = EXCLUDED.new_clients_here,
                clients_homed_here      = EXCLUDED.clients_homed_here,
                active_clients_here     = EXCLUDED.active_clients_here,
                revenue_here            = EXCLUDED.revenue_here,
                rank_by_new_clients     = EXCLUDED.rank_by_new_clients,
                rank_by_active_clients  = EXCLUDED.rank_by_active_clients,
                etl_run_at              = NOW()
        """
        for r in rows:
            await conn.execute(
                sql,
                r["business_id"], _parse_date(r.get("period")),
                r["location_id"], r.get("location_name"),
                r.get("new_clients_here"), r.get("clients_homed_here"),
                r.get("active_clients_here"), r.get("revenue_here"),
                r.get("rank_by_new_clients"),
                r.get("rank_by_active_clients"),
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Small helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _first_of_month(d: date) -> date:
        return d.replace(day=1)

    @staticmethod
    def _last_of_month(d: date) -> date:
        if d.month == 12:
            nxt = d.replace(year=d.year + 1, month=1, day=1)
        else:
            nxt = d.replace(month=d.month + 1, day=1)
        return nxt - timedelta(days=1)


# ── module-level helper (outside class so it's picklable) ────────────────────
def _parse_date(v):
    """Accept date, string, or None; return date or None."""
    if v is None:
        return None
    if isinstance(v, date):
        return v
    # Handle '2026-03-01' or '2026-03-01T00:00:00...'
    s = str(v)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None