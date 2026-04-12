"""
etl/transforms/appointments_etl.py
===================================
Appointments domain ETL extractor.

Pulls all 4 appointment data slices from the analytics backend,
computes warehouse-layer fields (MoM trends, trend slope, status
rates), and returns structured documents ready for embedding and
storage in pgvector.

Placement: etl/transforms/appointments_etl.py
Follows the same pattern as: etl/transforms/revenue_etl.py

Usage:
    from etl.transforms.appointments_etl import AppointmentsExtractor

    extractor = AppointmentsExtractor(client=analytics_client)
    docs = await extractor.run(
        business_id=42,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 6, 30),
    )
    # docs → list of dicts, each becomes one pgvector row
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class AppointmentsExtractor:
    """
    Pulls and transforms all appointments data for one tenant.
    Output: a list of structured embedding documents.

    Document types produced:
        appt_monthly_summary      — one per location per period (+ rollup)
        appt_staff_breakdown      — one per staff per period
        appt_service_breakdown    — one per service per period
        appt_staff_service_cross  — one per staff+service per period
    """

    DOMAIN = "appointments"

    def __init__(self, client: AnalyticsClient):
        self.client = client

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point
    # ─────────────────────────────────────────────────────────────────────────

    async def run(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Fetch all 4 appointment slices and return warehouse-ready docs.
        """
        logger.info(
            "AppointmentsExtractor: business_id=%s %s → %s",
            business_id, start_date, end_date,
        )

        # ── 1. Fetch all 4 slices in parallel ────────────────────────────────
        import asyncio
        monthly_raw, staff_raw, service_raw, cross_raw = await asyncio.gather(
            self.client.get_appointments_monthly_summary(
                business_id, start_date, end_date
            ),
            self.client.get_appointments_by_staff(
                business_id, start_date, end_date
            ),
            self.client.get_appointments_by_service(
                business_id, start_date, end_date
            ),
            self.client.get_appointments_staff_service_cross(
                business_id, start_date, end_date
            ),
        )

        # ── 2. Transform each slice into warehouse documents ──────────────────
        docs: list[dict] = []
        docs.extend(self._transform_monthly(business_id, monthly_raw))
        docs.extend(self._transform_staff(business_id, staff_raw))
        docs.extend(self._transform_service(business_id, service_raw))
        docs.extend(self._transform_cross(business_id, cross_raw))

        logger.info(
            "AppointmentsExtractor: produced %d documents for business_id=%s",
            len(docs), business_id,
        )
        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Transform: monthly summary
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_monthly(
        self, business_id: int, rows: list[dict]
    ) -> list[dict]:
        """
        One warehouse document per location per period.
        Rollup rows (location_id=0) become org-level summary docs.
        """
        docs = []
        for row in rows:
            period      = row.get("period", "")
            location_id = row.get("location_id", 0)
            loc_name    = row.get("location_name", "All Locations")
            loc_city    = row.get("location_city", "")
            is_rollup   = location_id == 0

            total       = row.get("total_booked", 0) or 0
            completed   = row.get("completed_count", 0) or 0
            cancelled   = row.get("cancelled_count", 0) or 0
            no_shows    = row.get("no_show_count", 0) or 0
            cancel_rate = row.get("cancellation_rate_pct", 0.0) or 0.0
            no_show_rate= row.get("no_show_rate_pct", 0.0) or 0.0
            mom         = row.get("mom_growth_pct")

            morning     = row.get("morning_count", 0) or 0
            afternoon   = row.get("afternoon_count", 0) or 0
            evening     = row.get("evening_count", 0) or 0
            weekend     = row.get("weekend_count", 0) or 0
            weekday     = row.get("weekday_count", 0) or 0
            avg_dur     = row.get("avg_actual_duration_min")

            walkin      = row.get("walkin_count", 0) or 0
            app_book    = row.get("app_booking_count", 0) or 0

            # Dominant time slot
            slot_counts = {"morning": morning, "afternoon": afternoon, "evening": evening}
            peak_slot   = max(slot_counts, key=slot_counts.get)

            # MoM narrative fragment
            if mom is None:
                mom_text = "first period on record"
            elif mom > 0:
                mom_text = f"up {mom:.1f}% vs previous period"
            elif mom < 0:
                mom_text = f"down {abs(mom):.1f}% vs previous period"
            else:
                mom_text = "flat vs previous period"

            # Location label for embedding text
            loc_label = (
                "All locations combined"
                if is_rollup
                else f"{loc_name}{' (' + loc_city + ')' if loc_city else ''}"
            )

            text = (
                f"Appointments — {loc_label} — {period}. "
                f"Total booked: {total}. "
                f"Completed: {completed}. Cancelled: {cancelled} ({cancel_rate:.1f}%). "
                f"No-shows: {no_shows} ({no_show_rate:.1f}%). "
                f"Booking trend: {mom_text}. "
                f"Time slots: {morning} morning, {afternoon} afternoon, {evening} evening. "
                f"Peak slot: {peak_slot}. "
                f"Weekday: {weekday}, Weekend: {weekend}. "
                + (f"Avg appointment duration: {avg_dur:.0f} min. " if avg_dur else "")
                + f"Walk-ins: {walkin}. App bookings: {app_book}."
            )

            docs.append({
                # Warehouse identity
                "tenant_id":          business_id,
                "doc_type":           "appt_monthly_summary",
                "domain":             self.DOMAIN,
                "period":             period,
                "location_id":        location_id,
                "location_name":      loc_name,
                "location_city":      loc_city,
                "is_rollup":          is_rollup,

                # Core metrics
                "total_booked":       total,
                "completed_count":    completed,
                "cancelled_count":    cancelled,
                "no_show_count":      no_shows,
                "cancellation_rate_pct": cancel_rate,
                "no_show_rate_pct":   no_show_rate,
                "mom_growth_pct":     mom,

                # Time distribution
                "morning_count":      morning,
                "afternoon_count":    afternoon,
                "evening_count":      evening,
                "weekend_count":      weekend,
                "weekday_count":      weekday,
                "peak_slot":          peak_slot,

                # Duration & booking source
                "avg_actual_duration_min": avg_dur,
                "walkin_count":        walkin,
                "app_booking_count":   app_book,

                # Embedding text
                "text": text,
            })

        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Transform: staff breakdown
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_staff(
        self, business_id: int, rows: list[dict]
    ) -> list[dict]:
        """
        One warehouse document per staff member per period.
        Preserves inactive staff — historical data must not be dropped.
        """
        docs = []
        for row in rows:
            period       = row.get("period", "")
            staff_id     = row.get("staff_id", 0)
            staff_name   = row.get("staff_name", "Unknown Staff")
            loc_id       = row.get("location_id", 0)
            loc_name     = row.get("location_name", "Unknown Location")
            total        = row.get("total_booked", 0) or 0
            completed    = row.get("completed_count", 0) or 0
            cancelled    = row.get("cancelled_count", 0) or 0
            no_shows     = row.get("no_show_count", 0) or 0
            no_show_rate = row.get("no_show_rate_pct", 0.0) or 0.0
            services     = row.get("distinct_services_handled", 0) or 0
            mom          = row.get("mom_growth_pct")

            # Completion rate
            completion_rate = round(completed / total * 100, 1) if total > 0 else 0.0

            # MoM fragment
            if mom is None:
                mom_text = "first period on record"
            elif mom > 5:
                mom_text = f"growing — up {mom:.1f}% vs previous period"
            elif mom < -5:
                mom_text = f"declining — down {abs(mom):.1f}% vs previous period"
            else:
                mom_text = f"stable ({mom:+.1f}% vs previous period)"

            text = (
                f"Staff appointments — {staff_name} at {loc_name} — {period}. "
                f"Total booked: {total}. Completed: {completed} ({completion_rate:.1f}%). "
                f"Cancelled: {cancelled}. No-shows: {no_shows} ({no_show_rate:.1f}%). "
                f"Booking trend: {mom_text}. "
                f"Services handled: {services} distinct service type(s)."
            )

            docs.append({
                "tenant_id":               business_id,
                "doc_type":                "appt_staff_breakdown",
                "domain":                  self.DOMAIN,
                "period":                  period,
                "staff_id":                staff_id,
                "staff_name":              staff_name,
                "location_id":             loc_id,
                "location_name":           loc_name,
                "total_booked":            total,
                "completed_count":         completed,
                "completion_rate_pct":     completion_rate,
                "cancelled_count":         cancelled,
                "no_show_count":           no_shows,
                "no_show_rate_pct":        no_show_rate,
                "distinct_services_handled": services,
                "mom_growth_pct":          mom,
                "text":                    text,
            })

        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Transform: service breakdown
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_service(
        self, business_id: int, rows: list[dict]
    ) -> list[dict]:
        """
        One warehouse document per service per period.
        """
        docs = []
        for row in rows:
            period        = row.get("period", "")
            service_id    = row.get("service_id", 0)
            service_name  = row.get("service_name", "Unknown Service")
            total         = row.get("total_booked", 0) or 0
            completed     = row.get("completed_count", 0) or 0
            cancelled     = row.get("cancelled_count", 0) or 0
            distinct      = row.get("distinct_clients", 0) or 0
            repeats       = row.get("repeat_visit_count", 0) or 0
            cancel_rate   = row.get("cancellation_rate_pct", 0.0) or 0.0
            sched_dur     = row.get("avg_scheduled_duration_min")
            actual_dur    = row.get("avg_actual_duration_min")
            morning       = row.get("morning_count", 0) or 0
            afternoon     = row.get("afternoon_count", 0) or 0
            evening       = row.get("evening_count", 0) or 0

            # Peak slot for this service
            slot_counts = {"morning": morning, "afternoon": afternoon, "evening": evening}
            peak_slot   = max(slot_counts, key=slot_counts.get)

            # Duration commentary
            if actual_dur and sched_dur:
                dur_diff = actual_dur - sched_dur
                if dur_diff > 5:
                    dur_note = f"running {dur_diff:.0f} min over the scheduled {sched_dur:.0f} min"
                elif dur_diff < -5:
                    dur_note = f"finishing {abs(dur_diff):.0f} min under the scheduled {sched_dur:.0f} min"
                else:
                    dur_note = f"on schedule at {sched_dur:.0f} min"
            elif actual_dur:
                dur_note = f"avg {actual_dur:.0f} min actual"
            elif sched_dur:
                dur_note = f"scheduled {sched_dur:.0f} min"
            else:
                dur_note = "duration not recorded"

            # Repeat client signal
            repeat_note = (
                f"{repeats} repeat visits from {distinct} unique clients"
                if distinct > 0
                else "no client breakdown available"
            )

            text = (
                f"Service appointments — {service_name} — {period}. "
                f"Total booked: {total}. Completed: {completed}. "
                f"Cancelled: {cancelled} ({cancel_rate:.1f}% cancellation rate). "
                f"Clients: {repeat_note}. "
                f"Duration: {dur_note}. "
                f"Peak booking slot: {peak_slot} "
                f"({morning} morning, {afternoon} afternoon, {evening} evening)."
            )

            docs.append({
                "tenant_id":                   business_id,
                "doc_type":                    "appt_service_breakdown",
                "domain":                      self.DOMAIN,
                "period":                      period,
                "service_id":                  service_id,
                "service_name":                service_name,
                "total_booked":                total,
                "completed_count":             completed,
                "cancelled_count":             cancelled,
                "distinct_clients":            distinct,
                "repeat_visit_count":          repeats,
                "cancellation_rate_pct":       cancel_rate,
                "avg_scheduled_duration_min":  sched_dur,
                "avg_actual_duration_min":     actual_dur,
                "morning_count":               morning,
                "afternoon_count":             afternoon,
                "evening_count":               evening,
                "peak_slot":                   peak_slot,
                "text":                        text,
            })

        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Transform: staff × service cross
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_cross(
        self, business_id: int, rows: list[dict]
    ) -> list[dict]:
        """
        One warehouse document per staff+service combination per period.
        These power Q16 (how many appointments per staff per service type).
        """
        docs = []
        for row in rows:
            period       = row.get("period", "")
            staff_id     = row.get("staff_id", 0)
            staff_name   = row.get("staff_name", "Unknown Staff")
            service_id   = row.get("service_id", 0)
            service_name = row.get("service_name", "Unknown Service")
            total        = row.get("total_booked", 0) or 0
            completed    = row.get("completed_count", 0) or 0

            completion_rate = round(completed / total * 100, 1) if total > 0 else 0.0

            text = (
                f"Staff-service appointments — {staff_name} performed {service_name} — {period}. "
                f"Bookings: {total}. Completed: {completed} ({completion_rate:.1f}%)."
            )

            docs.append({
                "tenant_id":          business_id,
                "doc_type":           "appt_staff_service_cross",
                "domain":             self.DOMAIN,
                "period":             period,
                "staff_id":           staff_id,
                "staff_name":         staff_name,
                "service_id":         service_id,
                "service_name":       service_name,
                "total_booked":       total,
                "completed_count":    completed,
                "completion_rate_pct": completion_rate,
                "text":               text,
            })

        return docs