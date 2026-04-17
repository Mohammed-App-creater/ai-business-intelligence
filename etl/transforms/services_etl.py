"""
etl/transforms/services_etl.py
================================
Services domain ETL extractor.

Pulls all 5 service data slices from the analytics backend,
writes them to the warehouse (wh_svc_* tables), and returns the
same structured documents for immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  ServicesExtractor.run()
        ↓  _write_to_warehouse()
    wh_svc_monthly_summary
    wh_svc_booking_stats
    wh_svc_staff_matrix
    wh_svc_co_occurrence
    wh_svc_catalog
        ↓  returned to doc generator → pgvector

Usage (with warehouse write):
    extractor = ServicesExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — pgvector only, e.g. tests):
    extractor = ServicesExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class ServicesExtractor:
    """
    Pulls and transforms all services data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg pool — when provided, writes extracted
              rows to the warehouse before returning.
    """

    DOMAIN = "services"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client = client
        self.wh_pool = wh_pool

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        Main entry point. Fetches all 5 slices, optionally writes
        to warehouse, and returns result summary.

        Returns
        -------
        dict with keys:
            monthly_summary: list[dict]
            booking_stats:   list[dict]
            staff_matrix:    list[dict]
            co_occurrence:   list[dict]
            catalog:         list[dict]
            counts:          dict of row counts per slice
        """
        logger.info(
            "ServicesExtractor.run — business_id=%s, %s to %s",
            business_id, start_date, end_date,
        )

        # ── Fetch all 5 slices in parallel ────────────────────────────────
        import asyncio

        monthly_task   = self.client.get_service_monthly_summary(business_id, start_date, end_date)
        booking_task   = self.client.get_service_booking_stats(business_id, start_date, end_date)
        matrix_task    = self.client.get_service_staff_matrix(business_id, start_date, end_date)
        cooccur_task   = self.client.get_service_co_occurrence(business_id, start_date, end_date)
        catalog_task   = self.client.get_service_catalog(business_id)

        monthly_rows, booking_rows, matrix_rows, cooccur_rows, catalog_rows = (
            await asyncio.gather(
                monthly_task, booking_task, matrix_task, cooccur_task, catalog_task
            )
        )

        # ── Transform: add business_id to every row if missing ────────────
        for rows in [monthly_rows, booking_rows, matrix_rows, cooccur_rows, catalog_rows]:
            for row in rows:
                row.setdefault("business_id", business_id)

        # ── Compute ranks on monthly summary ──────────────────────────────
        monthly_rows = self._compute_ranks(monthly_rows)

        counts = {
            "monthly_summary": len(monthly_rows),
            "booking_stats":   len(booking_rows),
            "staff_matrix":    len(matrix_rows),
            "co_occurrence":   len(cooccur_rows),
            "catalog":         len(catalog_rows),
        }

        logger.info(
            "ServicesExtractor fetched — monthly=%d booking=%d matrix=%d "
            "cooccur=%d catalog=%d",
            counts["monthly_summary"], counts["booking_stats"],
            counts["staff_matrix"], counts["co_occurrence"],
            counts["catalog"],
        )

        # ── Write to warehouse if pool available ──────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(
                business_id,
                monthly_rows, booking_rows, matrix_rows,
                cooccur_rows, catalog_rows,
            )
            logger.info(
                "warehouse write complete — monthly=%d booking=%d "
                "matrix=%d cooccur=%d catalog=%d",
                *counts.values(),
            )

        return {
            "monthly_summary": monthly_rows,
            "booking_stats":   booking_rows,
            "staff_matrix":    matrix_rows,
            "co_occurrence":   cooccur_rows,
            "catalog":         catalog_rows,
            "counts":          counts,
        }

    # ─────────────────────────────────────────────────────────────────────
    # Rank computation
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _compute_ranks(rows: list[dict]) -> list[dict]:
        """Add revenue_rank and margin_rank within each period."""
        from collections import defaultdict

        by_period: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            by_period[r.get("period_start", "")].append(r)

        for period, period_rows in by_period.items():
            # Revenue rank
            sorted_rev = sorted(
                period_rows,
                key=lambda x: x.get("total_revenue", 0) or 0,
                reverse=True,
            )
            for i, r in enumerate(sorted_rev, 1):
                r["revenue_rank"] = i

            # Margin rank
            sorted_margin = sorted(
                period_rows,
                key=lambda x: x.get("gross_margin", 0) or 0,
                reverse=True,
            )
            for i, r in enumerate(sorted_margin, 1):
                r["margin_rank"] = i

        return rows

    # ─────────────────────────────────────────────────────────────────────
    # Warehouse write
    # ─────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(
        self,
        business_id: int,
        monthly_rows: list[dict],
        booking_rows: list[dict],
        matrix_rows: list[dict],
        cooccur_rows: list[dict],
        catalog_rows: list[dict],
    ):
        """
        Upsert rows into the 5 wh_svc_* tables.
        Uses ON CONFLICT (business_id, service_id, location_id, period_start)
        DO UPDATE for idempotent re-runs.
        """
        async with self.wh_pool.acquire() as conn:
            # ── wh_svc_monthly_summary ────────────────────────────────
            for r in monthly_rows:
                await conn.execute(
                    """
                    INSERT INTO wh_svc_monthly_summary (
                        business_id, service_id, service_name, category_name,
                        location_id, location_name, period_start,
                        performed_count, distinct_clients, repeat_visit_proxy,
                        total_revenue, avg_charged_price,
                        total_emp_commission, gross_margin,
                        commission_pct_of_revenue, mom_revenue_growth_pct,
                        revenue_rank, margin_rank
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
                    )
                    ON CONFLICT (business_id, service_id, location_id, period_start)
                    DO UPDATE SET
                        service_name=EXCLUDED.service_name,
                        category_name=EXCLUDED.category_name,
                        location_name=EXCLUDED.location_name,
                        performed_count=EXCLUDED.performed_count,
                        distinct_clients=EXCLUDED.distinct_clients,
                        repeat_visit_proxy=EXCLUDED.repeat_visit_proxy,
                        total_revenue=EXCLUDED.total_revenue,
                        avg_charged_price=EXCLUDED.avg_charged_price,
                        total_emp_commission=EXCLUDED.total_emp_commission,
                        gross_margin=EXCLUDED.gross_margin,
                        commission_pct_of_revenue=EXCLUDED.commission_pct_of_revenue,
                        mom_revenue_growth_pct=EXCLUDED.mom_revenue_growth_pct,
                        revenue_rank=EXCLUDED.revenue_rank,
                        margin_rank=EXCLUDED.margin_rank
                    """,
                    business_id, r["service_id"], r["service_name"],
                    r.get("category_name"), r["location_id"], r["location_name"],
                    r["period_start"], r["performed_count"], r["distinct_clients"],
                    r["repeat_visit_proxy"], r["total_revenue"], r["avg_charged_price"],
                    r["total_emp_commission"], r["gross_margin"],
                    r.get("commission_pct_of_revenue"), r.get("mom_revenue_growth_pct"),
                    r.get("revenue_rank"), r.get("margin_rank"),
                )

            # ── wh_svc_booking_stats ──────────────────────────────────
            for r in booking_rows:
                await conn.execute(
                    """
                    INSERT INTO wh_svc_booking_stats (
                        business_id, service_id, service_name,
                        location_id, location_name, period_start,
                        total_booked, completed_count, cancelled_count,
                        no_show_count, cancellation_rate_pct,
                        avg_actual_duration_min, distinct_clients,
                        morning_bookings, afternoon_bookings, evening_bookings,
                        mom_bookings_growth_pct
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
                    ON CONFLICT (business_id, service_id, location_id, period_start)
                    DO UPDATE SET
                        service_name=EXCLUDED.service_name,
                        location_name=EXCLUDED.location_name,
                        total_booked=EXCLUDED.total_booked,
                        completed_count=EXCLUDED.completed_count,
                        cancelled_count=EXCLUDED.cancelled_count,
                        no_show_count=EXCLUDED.no_show_count,
                        cancellation_rate_pct=EXCLUDED.cancellation_rate_pct,
                        avg_actual_duration_min=EXCLUDED.avg_actual_duration_min,
                        distinct_clients=EXCLUDED.distinct_clients,
                        morning_bookings=EXCLUDED.morning_bookings,
                        afternoon_bookings=EXCLUDED.afternoon_bookings,
                        evening_bookings=EXCLUDED.evening_bookings,
                        mom_bookings_growth_pct=EXCLUDED.mom_bookings_growth_pct
                    """,
                    business_id, r["service_id"], r["service_name"],
                    r["location_id"], r["location_name"], r["period_start"],
                    r["total_booked"], r["completed_count"], r["cancelled_count"],
                    r["no_show_count"], r.get("cancellation_rate_pct"),
                    r.get("avg_actual_duration_min"), r["distinct_clients"],
                    r["morning_bookings"], r["afternoon_bookings"], r["evening_bookings"],
                    r.get("mom_bookings_growth_pct"),
                )

            # ── wh_svc_staff_matrix ───────────────────────────────────
            for r in matrix_rows:
                await conn.execute(
                    """
                    INSERT INTO wh_svc_staff_matrix (
                        business_id, service_id, service_name,
                        staff_id, staff_name, period_start,
                        performed_count, revenue, commission_paid
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (business_id, service_id, staff_id, period_start)
                    DO UPDATE SET
                        service_name=EXCLUDED.service_name,
                        staff_name=EXCLUDED.staff_name,
                        performed_count=EXCLUDED.performed_count,
                        revenue=EXCLUDED.revenue,
                        commission_paid=EXCLUDED.commission_paid
                    """,
                    business_id, r["service_id"], r["service_name"],
                    r["staff_id"], r["staff_name"], r["period_start"],
                    r["performed_count"], r["revenue"], r["commission_paid"],
                )

            # ── wh_svc_co_occurrence ──────────────────────────────────
            for r in cooccur_rows:
                await conn.execute(
                    """
                    INSERT INTO wh_svc_co_occurrence (
                        business_id, period_start,
                        service_a_id, service_a_name,
                        service_b_id, service_b_name,
                        co_occurrence_count
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (business_id, service_a_id, service_b_id, period_start)
                    DO UPDATE SET
                        service_a_name=EXCLUDED.service_a_name,
                        service_b_name=EXCLUDED.service_b_name,
                        co_occurrence_count=EXCLUDED.co_occurrence_count
                    """,
                    business_id, r["period_start"],
                    r["service_a_id"], r["service_a_name"],
                    r["service_b_id"], r["service_b_name"],
                    r["co_occurrence_count"],
                )

            # ── wh_svc_catalog ────────────────────────────────────────
            for r in catalog_rows:
                await conn.execute(
                    """
                    INSERT INTO wh_svc_catalog (
                        business_id, service_id, service_name, category_name,
                        list_price, default_commission_rate, commission_type,
                        scheduled_duration_min, is_active, created_at,
                        home_location_id, last_sold_date, days_since_last_sale,
                        lifetime_performed_count, new_client_first_service_count,
                        dormant_flag, is_new_this_year,
                        avg_discount_pct, scheduled_vs_actual_delta_min
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                    ON CONFLICT (business_id, service_id)
                    DO UPDATE SET
                        service_name=EXCLUDED.service_name,
                        category_name=EXCLUDED.category_name,
                        list_price=EXCLUDED.list_price,
                        default_commission_rate=EXCLUDED.default_commission_rate,
                        commission_type=EXCLUDED.commission_type,
                        scheduled_duration_min=EXCLUDED.scheduled_duration_min,
                        is_active=EXCLUDED.is_active,
                        created_at=EXCLUDED.created_at,
                        home_location_id=EXCLUDED.home_location_id,
                        last_sold_date=EXCLUDED.last_sold_date,
                        days_since_last_sale=EXCLUDED.days_since_last_sale,
                        lifetime_performed_count=EXCLUDED.lifetime_performed_count,
                        new_client_first_service_count=EXCLUDED.new_client_first_service_count,
                        dormant_flag=EXCLUDED.dormant_flag,
                        is_new_this_year=EXCLUDED.is_new_this_year,
                        avg_discount_pct=EXCLUDED.avg_discount_pct,
                        scheduled_vs_actual_delta_min=EXCLUDED.scheduled_vs_actual_delta_min
                    """,
                    business_id, r["service_id"], r["service_name"],
                    r.get("category_name"), r["list_price"],
                    r.get("default_commission_rate"), r["commission_type"],
                    r["scheduled_duration_min"], r["is_active"],
                    r.get("created_at"), r.get("home_location_id"),
                    r.get("last_sold_date"), r.get("days_since_last_sale"),
                    r["lifetime_performed_count"], r["new_client_first_service_count"],
                    r["dormant_flag"], r["is_new_this_year"],
                    r.get("avg_discount_pct"), r.get("scheduled_vs_actual_delta_min"),
                )

            logger.info("warehouse upsert complete for business_id=%d", business_id)