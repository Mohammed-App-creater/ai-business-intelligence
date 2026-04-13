"""
etl/transforms/appointments_etl.py
===================================
Appointments domain ETL extractor.

Pulls all 4 appointment data slices from the analytics backend,
writes them to the warehouse (wh_appt_* tables), and returns the
same structured documents for immediate use by the doc generator.

Flow:
    Mock Server / Analytics Backend
        ↓  AppointmentsExtractor.run()
        ↓  _write_to_warehouse()  ← writes to 4 wh_appt_* tables
    wh_appt_monthly_summary
    wh_appt_staff_breakdown
    wh_appt_service_breakdown
    wh_appt_staff_service_cross
        ↓  docs returned to doc generator → pgvector

Usage (with warehouse write):
    extractor = AppointmentsExtractor(client=analytics_client, wh_pool=wh_pool)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)

Usage (without warehouse — pgvector only, e.g. tests):
    extractor = AppointmentsExtractor(client=analytics_client)
    docs = await extractor.run(business_id=42, start_date=..., end_date=...)
"""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date

from app.services.analytics_client import AnalyticsClient

logger = logging.getLogger(__name__)


class AppointmentsExtractor:
    """
    Pulls and transforms all appointments data for one tenant.

    Parameters
    ----------
    client:   AnalyticsClient — calls the analytics backend API.
    wh_pool:  Optional asyncpg/PGPool — when provided, writes extracted
              rows to the warehouse before returning. When None, the
              warehouse write is skipped (useful in tests).
    """

    DOMAIN = "appointments"

    def __init__(self, client: AnalyticsClient, wh_pool=None):
        self.client  = client
        self.wh_pool = wh_pool

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
        Fetch all 4 appointment slices, write to warehouse, return docs.
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

        # ── 3. Write to warehouse (if pool provided) ──────────────────────────
        if self.wh_pool is not None:
            await self._write_to_warehouse(docs)
        else:
            logger.debug(
                "AppointmentsExtractor: wh_pool not provided — skipping warehouse write"
            )

        logger.info(
            "AppointmentsExtractor: produced %d documents for business_id=%s",
            len(docs), business_id,
        )
        return docs

    # ─────────────────────────────────────────────────────────────────────────
    # Warehouse write — 4 upsert methods, one per table
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_to_warehouse(self, docs: list[dict]) -> None:
        """Upsert all docs into the 4 wh_appt_* tables. Idempotent."""
        by_type: dict[str, list[dict]] = {}
        for doc in docs:
            by_type.setdefault(doc.get("doc_type", ""), []).append(doc)

        async with self.wh_pool.acquire() as conn:
            await self._upsert_monthly_summary(
                conn, by_type.get("appt_monthly_summary", [])
            )
            await self._upsert_staff_breakdown(
                conn, by_type.get("appt_staff_breakdown", [])
            )
            await self._upsert_service_breakdown(
                conn, by_type.get("appt_service_breakdown", [])
            )
            await self._upsert_staff_service_cross(
                conn, by_type.get("appt_staff_service_cross", [])
            )

        logger.info(
            "AppointmentsExtractor: warehouse write complete — "
            "monthly=%d staff=%d service=%d cross=%d",
            len(by_type.get("appt_monthly_summary", [])),
            len(by_type.get("appt_staff_breakdown", [])),
            len(by_type.get("appt_service_breakdown", [])),
            len(by_type.get("appt_staff_service_cross", [])),
        )

    async def _upsert_monthly_summary(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_appt_monthly_summary (
    business_id, location_id, location_name, location_city,
    period_start, period_end, is_rollup,
    total_booked, confirmed_count, completed_count, cancelled_count, no_show_count,
    morning_count, afternoon_count, evening_count, weekend_count, weekday_count,
    avg_actual_duration_min, cancellation_rate_pct, no_show_rate_pct,
    mom_growth_pct, walkin_count, app_booking_count, peak_slot
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
)
ON CONFLICT (business_id, location_id, period_start) DO UPDATE SET
    location_name           = EXCLUDED.location_name,
    location_city           = EXCLUDED.location_city,
    period_end              = EXCLUDED.period_end,
    is_rollup               = EXCLUDED.is_rollup,
    total_booked            = EXCLUDED.total_booked,
    confirmed_count         = EXCLUDED.confirmed_count,
    completed_count         = EXCLUDED.completed_count,
    cancelled_count         = EXCLUDED.cancelled_count,
    no_show_count           = EXCLUDED.no_show_count,
    morning_count           = EXCLUDED.morning_count,
    afternoon_count         = EXCLUDED.afternoon_count,
    evening_count           = EXCLUDED.evening_count,
    weekend_count           = EXCLUDED.weekend_count,
    weekday_count           = EXCLUDED.weekday_count,
    avg_actual_duration_min = EXCLUDED.avg_actual_duration_min,
    cancellation_rate_pct   = EXCLUDED.cancellation_rate_pct,
    no_show_rate_pct        = EXCLUDED.no_show_rate_pct,
    mom_growth_pct          = EXCLUDED.mom_growth_pct,
    walkin_count            = EXCLUDED.walkin_count,
    app_booking_count       = EXCLUDED.app_booking_count,
    peak_slot               = EXCLUDED.peak_slot,
    updated_at              = now()
"""
        records = []
        for r in rows:
            y, m = int(r["period"][:4]), int(r["period"][5:7])
            ps = date(y, m, 1)
            pe = date(y, m, monthrange(y, m)[1])
            records.append((
                r["tenant_id"], r["location_id"], r["location_name"], r["location_city"],
                ps, pe, r["is_rollup"],
                r["total_booked"], r.get("confirmed_count", 0),
                r["completed_count"], r["cancelled_count"], r["no_show_count"],
                r["morning_count"], r["afternoon_count"], r["evening_count"],
                r["weekend_count"], r["weekday_count"],
                r.get("avg_actual_duration_min"),
                r["cancellation_rate_pct"], r["no_show_rate_pct"],
                r.get("mom_growth_pct"),
                r["walkin_count"], r["app_booking_count"], r.get("peak_slot"),
            ))
        await conn.executemany(sql, records)

    async def _upsert_staff_breakdown(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_appt_staff_breakdown (
    business_id, staff_id, staff_name, location_id, location_name,
    period_start, period_end,
    total_booked, completed_count, completion_rate_pct,
    cancelled_count, no_show_count, no_show_rate_pct,
    distinct_services_handled, mom_growth_pct
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (business_id, staff_id, location_id, period_start) DO UPDATE SET
    staff_name                = EXCLUDED.staff_name,
    period_end                = EXCLUDED.period_end,
    total_booked              = EXCLUDED.total_booked,
    completed_count           = EXCLUDED.completed_count,
    completion_rate_pct       = EXCLUDED.completion_rate_pct,
    cancelled_count           = EXCLUDED.cancelled_count,
    no_show_count             = EXCLUDED.no_show_count,
    no_show_rate_pct          = EXCLUDED.no_show_rate_pct,
    distinct_services_handled = EXCLUDED.distinct_services_handled,
    mom_growth_pct            = EXCLUDED.mom_growth_pct,
    updated_at                = now()
"""
        records = []
        for r in rows:
            y, m = int(r["period"][:4]), int(r["period"][5:7])
            ps = date(y, m, 1)
            pe = date(y, m, monthrange(y, m)[1])
            records.append((
                r["tenant_id"], r["staff_id"], r["staff_name"],
                r["location_id"], r["location_name"],
                ps, pe,
                r["total_booked"], r["completed_count"], r["completion_rate_pct"],
                r["cancelled_count"], r["no_show_count"], r["no_show_rate_pct"],
                r["distinct_services_handled"], r.get("mom_growth_pct"),
            ))
        await conn.executemany(sql, records)

    async def _upsert_service_breakdown(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_appt_service_breakdown (
    business_id, service_id, service_name,
    period_start, period_end,
    total_booked, completed_count, cancelled_count,
    distinct_clients, repeat_visit_count,
    avg_scheduled_duration_min, avg_actual_duration_min,
    cancellation_rate_pct,
    morning_count, afternoon_count, evening_count, peak_slot
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (business_id, service_id, period_start) DO UPDATE SET
    service_name               = EXCLUDED.service_name,
    period_end                 = EXCLUDED.period_end,
    total_booked               = EXCLUDED.total_booked,
    completed_count            = EXCLUDED.completed_count,
    cancelled_count            = EXCLUDED.cancelled_count,
    distinct_clients           = EXCLUDED.distinct_clients,
    repeat_visit_count         = EXCLUDED.repeat_visit_count,
    avg_scheduled_duration_min = EXCLUDED.avg_scheduled_duration_min,
    avg_actual_duration_min    = EXCLUDED.avg_actual_duration_min,
    cancellation_rate_pct      = EXCLUDED.cancellation_rate_pct,
    morning_count              = EXCLUDED.morning_count,
    afternoon_count            = EXCLUDED.afternoon_count,
    evening_count              = EXCLUDED.evening_count,
    peak_slot                  = EXCLUDED.peak_slot,
    updated_at                 = now()
"""
        records = []
        for r in rows:
            y, m = int(r["period"][:4]), int(r["period"][5:7])
            ps = date(y, m, 1)
            pe = date(y, m, monthrange(y, m)[1])
            records.append((
                r["tenant_id"], r["service_id"], r["service_name"],
                ps, pe,
                r["total_booked"], r["completed_count"], r["cancelled_count"],
                r["distinct_clients"], r["repeat_visit_count"],
                r.get("avg_scheduled_duration_min"), r.get("avg_actual_duration_min"),
                r["cancellation_rate_pct"],
                r["morning_count"], r["afternoon_count"], r["evening_count"],
                r.get("peak_slot"),
            ))
        await conn.executemany(sql, records)

    async def _upsert_staff_service_cross(self, conn, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
INSERT INTO wh_appt_staff_service_cross (
    business_id, staff_id, staff_name, service_id, service_name,
    period_start, period_end,
    total_booked, completed_count, completion_rate_pct
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (business_id, staff_id, service_id, period_start) DO UPDATE SET
    staff_name          = EXCLUDED.staff_name,
    service_name        = EXCLUDED.service_name,
    period_end          = EXCLUDED.period_end,
    total_booked        = EXCLUDED.total_booked,
    completed_count     = EXCLUDED.completed_count,
    completion_rate_pct = EXCLUDED.completion_rate_pct,
    updated_at          = now()
"""
        records = []
        for r in rows:
            y, m = int(r["period"][:4]), int(r["period"][5:7])
            ps = date(y, m, 1)
            pe = date(y, m, monthrange(y, m)[1])
            records.append((
                r["tenant_id"], r["staff_id"], r["staff_name"],
                r["service_id"], r["service_name"],
                ps, pe,
                r["total_booked"], r["completed_count"], r["completion_rate_pct"],
            ))
        await conn.executemany(sql, records)

    # ─────────────────────────────────────────────────────────────────────────
    # Transform methods (unchanged from original)
    # ─────────────────────────────────────────────────────────────────────────

    def _transform_monthly(self, business_id: int, rows: list[dict]) -> list[dict]:
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

            slot_counts = {"morning": morning, "afternoon": afternoon, "evening": evening}
            peak_slot   = max(slot_counts, key=slot_counts.get)

            if mom is None:
                mom_text = "first period on record"
            elif mom > 0:
                mom_text = f"up {mom:.1f}% vs previous period"
            elif mom < 0:
                mom_text = f"down {abs(mom):.1f}% vs previous period"
            else:
                mom_text = "flat vs previous period"

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
                f"Peak slot: {peak_slot}. Weekday: {weekday}, Weekend: {weekend}. "
                + (f"Avg appointment duration: {avg_dur:.0f} min. " if avg_dur else "")
                + f"Walk-ins: {walkin}. App bookings: {app_book}."
            )

            docs.append({
                "tenant_id": business_id, "doc_type": "appt_monthly_summary",
                "domain": self.DOMAIN, "period": period,
                "location_id": location_id, "location_name": loc_name,
                "location_city": loc_city, "is_rollup": is_rollup,
                "total_booked": total, "confirmed_count": row.get("confirmed_count", 0) or 0,
                "completed_count": completed, "cancelled_count": cancelled,
                "no_show_count": no_shows, "cancellation_rate_pct": cancel_rate,
                "no_show_rate_pct": no_show_rate, "mom_growth_pct": mom,
                "morning_count": morning, "afternoon_count": afternoon,
                "evening_count": evening, "weekend_count": weekend,
                "weekday_count": weekday, "peak_slot": peak_slot,
                "avg_actual_duration_min": avg_dur,
                "walkin_count": walkin, "app_booking_count": app_book,
                "text": text,
            })
        return docs

    def _transform_staff(self, business_id: int, rows: list[dict]) -> list[dict]:
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
            completion_rate = round(completed / total * 100, 1) if total > 0 else 0.0

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
                "tenant_id": business_id, "doc_type": "appt_staff_breakdown",
                "domain": self.DOMAIN, "period": period,
                "staff_id": staff_id, "staff_name": staff_name,
                "location_id": loc_id, "location_name": loc_name,
                "total_booked": total, "completed_count": completed,
                "completion_rate_pct": completion_rate, "cancelled_count": cancelled,
                "no_show_count": no_shows, "no_show_rate_pct": no_show_rate,
                "distinct_services_handled": services, "mom_growth_pct": mom,
                "text": text,
            })
        return docs

    def _transform_service(self, business_id: int, rows: list[dict]) -> list[dict]:
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

            slot_counts = {"morning": morning, "afternoon": afternoon, "evening": evening}
            peak_slot   = max(slot_counts, key=slot_counts.get)

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

            repeat_note = (
                f"{repeats} repeat visits from {distinct} unique clients"
                if distinct > 0 else "no client breakdown available"
            )

            text = (
                f"Service appointments — {service_name} — {period}. "
                f"Total booked: {total}. Completed: {completed}. "
                f"Cancelled: {cancelled} ({cancel_rate:.1f}% cancellation rate). "
                f"Clients: {repeat_note}. Duration: {dur_note}. "
                f"Peak booking slot: {peak_slot} "
                f"({morning} morning, {afternoon} afternoon, {evening} evening)."
            )

            docs.append({
                "tenant_id": business_id, "doc_type": "appt_service_breakdown",
                "domain": self.DOMAIN, "period": period,
                "service_id": service_id, "service_name": service_name,
                "total_booked": total, "completed_count": completed,
                "cancelled_count": cancelled, "distinct_clients": distinct,
                "repeat_visit_count": repeats, "cancellation_rate_pct": cancel_rate,
                "avg_scheduled_duration_min": sched_dur, "avg_actual_duration_min": actual_dur,
                "morning_count": morning, "afternoon_count": afternoon,
                "evening_count": evening, "peak_slot": peak_slot,
                "text": text,
            })
        return docs

    def _transform_cross(self, business_id: int, rows: list[dict]) -> list[dict]:
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
                "tenant_id": business_id, "doc_type": "appt_staff_service_cross",
                "domain": self.DOMAIN, "period": period,
                "staff_id": staff_id, "staff_name": staff_name,
                "service_id": service_id, "service_name": service_name,
                "total_booked": total, "completed_count": completed,
                "completion_rate_pct": completion_rate, "text": text,
            })
        return docs