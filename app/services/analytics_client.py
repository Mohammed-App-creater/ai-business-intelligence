"""
LEO AI BI — Analytics Backend Client
Revenue Domain — Step 4: ETL Wire-Up
 
All methods follow the standard contract:
    async def get_{domain}_data(self, business_id, start_date, end_date, **kwargs) -> list[dict]
 
Base URL points to the analytics backend built from the Step 3 API spec.
All endpoints are POST and require business_id + date range.
"""

import httpx
from datetime import date
from typing import Optional

from app.core.config import settings
import logging
 
 
 
logger = logging.getLogger(__name__)
 


class AnalyticsClientError(Exception):
    pass


class _RevenueClient:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def get_monthly_revenue(self, business_id: int, start: date, end: date) -> list[dict]:
        return await _get(self._http, "/revenue/monthly", business_id, start, end)

    async def get_monthly_revenue_totals(self, business_id: int, start: date, end: date) -> list[dict]:
        return await _get(self._http, "/revenue/monthly-totals", business_id, start, end)

    async def get_revenue_by_payment_type(self, business_id: int, start: date, end: date) -> list[dict]:
        return await _get(self._http, "/revenue/by-payment-type", business_id, start, end)

    async def get_revenue_by_staff(self, business_id: int, start: date, end: date) -> list[dict]:
        return await _get(self._http, "/revenue/by-staff", business_id, start, end)

    async def get_daily_revenue(self, business_id: int, start: date, end: date) -> list[dict]:
        return await _get(self._http, "/revenue/daily", business_id, start, end)

    async def get_promo_usage(self, business_id: int, start: date, end: date) -> list[dict]:
        return await _get(self._http, "/revenue/promo-usage", business_id, start, end)


async def _get(
    http: httpx.AsyncClient,
    path: str,
    business_id: int,
    start: date,
    end: date,
    **extra_params,
) -> list[dict]:
    try:
        resp = await http.get(
            path,
            params={
                "business_id": business_id,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                **extra_params,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except httpx.HTTPStatusError as e:
        raise AnalyticsClientError(
            f"Analytics backend error {e.response.status_code} for {path}: {e.response.text}"
        ) from e
    except httpx.RequestError as e:
        raise AnalyticsClientError(f"Analytics backend unreachable at {path}: {e}") from e


class AnalyticsClient:
    """
    HTTP client for the analytics backend.
    Replaces DBClient (MySQL) as the ETL data source.
    Add one sub-client per domain as each sprint completes.

    Sprint 1  — revenue      ✅
    Sprint 2  — appointments ✅
    Sprint 3  — staff        ✅
    Sprint 4  — services     ✅
    Sprint 5  — clients      ⬜
    Sprint 6  — marketing    ⬜
    Sprint 7  — memberships  ⬜
    Sprint 8  — giftcards    ⬜
    Sprint 9  — promos       ⬜
    Sprint 10 — expenses     ⬜
    Sprint 11 — forms        ⬜
    """
    
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        
        

    # -------------------------------------------------------------------------
    # REVENUE DOMAIN — 6 endpoints
    # -------------------------------------------------------------------------
 
    async def get_revenue_monthly_summary(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        group_by: str = "month",  # "month" | "week" | "day"
    ) -> list[dict]:
        """
        Monthly (or weekly/daily) revenue summary.
 
        Powers: Q1–Q7, Q13–Q14, Q16–Q19
        Key fields returned:
            period, visit_count, service_revenue, total_tips, total_tax,
            total_collected, total_discounts, gc_redemptions, avg_ticket,
            mom_growth_pct, refund_count, cancel_count
        Meta fields:
            total_service_revenue, total_visits, best_period,
            worst_period, trend_slope
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "group_by": group_by,
        }
        return await self._post("/api/v1/leo/revenue/monthly-summary", payload)
 
    async def get_revenue_payment_types(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Revenue breakdown by payment type (Cash / Card / GiftCard / etc.)
 
        Powers: Q10
        Key fields returned:
            payment_type, visit_count, revenue, pct_of_total
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/revenue/payment-types", payload)
 
    async def get_revenue_by_staff(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        limit: int = 10,
    ) -> list[dict]:
        """
        Staff revenue ranking — top N staff by service revenue.
 
        Powers: Q8
        Key fields returned:
            emp_id, staff_name, visit_count, service_revenue,
            tips_collected, avg_ticket, revenue_rank
        Note: includes inactive staff with visits in the date range.
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "limit": limit,
        }
        return await self._post("/api/v1/leo/revenue/by-staff", payload)
 
    async def get_revenue_by_location(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        group_by: str = "month",  # "month" | "total"
    ) -> list[dict]:
        """
        Revenue breakdown per location per period.
 
        Powers: Q9, LQ1–LQ10
        Key fields returned:
            location_id, location_name, period, visit_count,
            service_revenue, total_tips, avg_ticket, total_discounts,
            gc_redemptions, pct_of_total_revenue, mom_growth_pct
        Note: single-location businesses return 1 row — not an error.
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "group_by": group_by,
        }
        return await self._post("/api/v1/leo/revenue/by-location", payload)
 
    async def get_revenue_promo_impact(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Promo code discount impact — what did promos actually cost?
 
        Powers: Q12, LQ9
        Key fields returned:
            promo_code, promo_description, location_id, location_name,
            times_used, total_discount_given, revenue_after_discount
        Meta:
            total_discount_all_promos, promo_visit_count
        Note: uses tbl_visit.Discount as ground truth, not tbl_promo.Amount.
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/revenue/promo-impact", payload)
 
    async def get_revenue_failed_refunds(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Failed, refunded, and canceled visit revenue loss.
 
        Powers: Q15 (partial), Q20
        Key fields returned:
            status_code, status_label, visit_count,
            lost_revenue, avg_lost_per_visit
        Meta:
            total_lost_revenue, total_affected_visits
 
        KNOWN GAP: Q15 (no-show cost) requires tbl_calendarevent.
        Defer to Appointments domain sprint — /no-show-cost endpoint TBD.
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/revenue/failed-refunds", payload)

    # ── APPOINTMENTS DOMAIN ───────────────────────────────────────────────────
    async def get_appointments_monthly_summary(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        location_id: Optional[int] = None,
        group_by: str = "month",
    ) -> list[dict]:
        """
        Monthly appointment KPIs per location + org rollup.

        Returns per-location rows (location_id > 0) AND one org-level
        rollup row per period (location_id = 0, location_name = "__ALL__").

        Powers: Q1–Q8, Q11–Q12, Q24–Q25, Q27–Q29

        Key fields returned per row:
            period, location_id, location_name, location_city,
            total_booked, confirmed_count, completed_count,
            cancelled_count, no_show_count,
            morning_count, afternoon_count, evening_count,
            weekend_count, weekday_count,
            avg_actual_duration_min,
            cancellation_rate_pct, no_show_rate_pct,
            mom_growth_pct,
            walkin_count, app_booking_count

        Meta:
            total_booked, total_completed, total_cancelled,
            total_no_shows, avg_cancellation_rate_pct,
            best_period, worst_period, trend_slope
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "group_by": group_by,
        }
        if location_id is not None:
            payload["location_id"] = location_id

        return await self._post(
            "/api/v1/leo/appointments/monthly-summary", payload
        )

    async def get_appointments_by_staff(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        staff_id: Optional[int] = None,
        location_id: Optional[int] = None,
        limit: int = 50,
    ) -> list[dict]:
        """
        Per-staff monthly appointment statistics.

        Includes all staff with appointments in the date range,
        even if now inactive — historical data must be preserved.
        Do NOT filter by employee Active status.

        Powers: Q10, Q13–Q18

        Key fields returned per row:
            staff_id, staff_name, location_id, location_name, period,
            total_booked, completed_count, cancelled_count,
            no_show_count, no_show_rate_pct,
            distinct_services_handled, mom_growth_pct
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "limit": limit,
        }
        if staff_id is not None:
            payload["staff_id"] = staff_id
        if location_id is not None:
            payload["location_id"] = location_id

        return await self._post(
            "/api/v1/leo/appointments/by-staff", payload
        )

    async def get_appointments_by_service(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        service_id: Optional[int] = None,
        group_by: str = "month",
    ) -> list[dict]:
        """
        Per-service monthly appointment statistics.

        repeat_visit_count is a within-period proxy:
            total_booked - distinct_clients
        Full lifetime cohort analysis is in the Client Retention domain.

        Powers: Q9, Q19–Q23, Q26

        Key fields returned per row:
            service_id, service_name, period,
            total_booked, completed_count, cancelled_count,
            distinct_clients, repeat_visit_count,
            avg_scheduled_duration_min, avg_actual_duration_min,
            cancellation_rate_pct,
            morning_count, afternoon_count, evening_count
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "group_by": group_by,
        }
        if service_id is not None:
            payload["service_id"] = service_id

        return await self._post(
            "/api/v1/leo/appointments/by-service", payload
        )

    async def get_appointments_staff_service_cross(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        staff_id: Optional[int] = None,
        service_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Staff × Service cross-dimensional breakdown.

        Shows how many appointments each staff member handled per
        service type in the period. Used to build staff specialisation
        profiles and answer Q16.

        Powers: Q16

        Key fields returned per row:
            staff_id, staff_name,
            service_id, service_name,
            period,
            total_booked, completed_count
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        if staff_id is not None:
            payload["staff_id"] = staff_id
        if service_id is not None:
            payload["service_id"] = service_id

        return await self._post(
            "/api/v1/leo/appointments/staff-service-cross", payload
        )


    # ── STAFF PERFORMANCE DOMAIN ──────────────────────────────────────────────
    # 3 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 3 — staff 

    async def get_staff_performance_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        location_id: Optional[int] = None,
        staff_id: Optional[int] = None,
        include_inactive: bool = True,
    ) -> list[dict]:
        """
        Monthly staff KPIs per (staff × location × period).

        Returns one row per staff member per location per month in the
        date range. Staff working across two locations return two rows
        for the same month — one per location.

        Includes inactive staff (is_active=False) by default so historical
        data is preserved for deactivated employees. Pass include_inactive=False
        only for "current team" views.

        Powers: Q1–Q8, Q11–Q22, Q25–Q32, Q34–Q35, Q37–Q39

        Key fields returned per row:
            business_id, staff_id, staff_full_name,
            staff_first_name, staff_last_name,
            is_active, hire_date,
            location_id, location_name,
            year, month, period_label,
            completed_visit_count, unique_customer_count,
            revenue, tips, total_pay, avg_revenue_per_visit,
            commission_earned,
            cancelled_payment_count, refunded_payment_count, revoked_payment_count,
            review_count, avg_rating (NULL when no reviews — not 0)
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
            "mode":        "monthly",
        }
        if location_id is not None:
            payload["location_id"] = location_id
        if staff_id is not None:
            payload["staff_id"] = staff_id
        if not include_inactive:
            payload["include_inactive"] = False

        return await self._post("/api/v1/leo/staff-performance", payload)

    async def get_staff_performance_summary(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        All-time / YTD aggregated staff KPIs — one row per staff member.

        Used for overall team rankings, all-time revenue leaders, and
        aggregate team digests. Inactive staff are included so their
        historical contribution is visible.

        Powers: Q9 (rank all staff by revenue), Q10 (lowest rating),
                Q29 (team digest), Q31 (give me team numbers)

        Key fields returned per row:
            business_id, staff_id, staff_full_name,
            staff_first_name, staff_last_name,
            is_active, hire_date,
            total_visits_ytd, total_revenue_ytd,
            total_tips_ytd, total_commission_ytd,
            total_customers_served,
            total_cancelled_ytd, total_refunded_ytd,
            overall_avg_rating (NULL when no reviews),
            total_review_count,
            lifetime_avg_revenue_per_visit,
            first_active_period, last_active_period,
            revenue_pct_of_org_latest (NULL for inactive with no recent data)
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
            "mode":        "summary",
        }
        return await self._post("/api/v1/leo/staff-performance", payload)

    async def get_staff_attendance(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        location_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Monthly attendance hours per staff member per location.

        Time format confirmed by team (2026-04-13): '10:44:15 PM'.
        No-time value '0' excluded from duration calculation.
        Overnight shifts handled server-side (+1440 min guard).
        Duration capped at 24h/day as data sanity check.

        Inactive staff (is_active=False) are included — deactivated staff
        attendance data must be preserved for historical questions.

        Powers: Q33 (who clocked the most hours)

        Key fields returned per row:
            business_id, staff_id, staff_full_name, is_active,
            location_id, location_name,
            year, month, period_label,
            days_with_signin,       -- signed in at least once
            days_fully_recorded,    -- both sign-in and sign-out (denominator for avg)
            days_missing_signout,   -- data quality indicator
            total_hours_worked,
            avg_hours_per_day (NULL when days_fully_recorded = 0)
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        if location_id is not None:
            payload["location_id"] = location_id

        return await self._post("/api/v1/leo/staff-attendance", payload)

    # ── SERVICES DOMAIN ───────────────────────────────────────────────────────
    # 5 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 4 — services

    async def get_service_monthly_summary(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        service_id: Optional[int] = None,
        location_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Per-service per-location monthly aggregations from paid visits.
        Revenue, margin, commission, distinct clients.

        Source: tbl_service_visit × tbl_visit (PaymentStatus=1)

        Powers: Q2,3,5,6,7,8,9,10,11,12,13,14,16,17,18,24,26,30

        Key fields returned per row:
            service_id, service_name, category_name,
            location_id, location_name, period_start,
            performed_count, distinct_clients, repeat_visit_proxy,
            total_revenue, avg_charged_price,
            total_emp_commission, gross_margin,
            commission_pct_of_revenue, mom_revenue_growth_pct,
            revenue_rank, margin_rank
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        if service_id is not None:
            payload["service_id"] = service_id
        if location_id is not None:
            payload["location_id"] = location_id

        return await self._post(
            "/api/v1/leo/services/monthly-summary", payload
        )

    async def get_service_booking_stats(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        service_id: Optional[int] = None,
        location_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Per-service per-location monthly booking counts from tbl_calendarevent.
        Includes cancellations, no-shows, duration analysis, time-slot distribution.

        Powers: Q1,4,14,15,16,24,25,26,27,28

        Key fields returned per row:
            service_id, service_name, location_id, location_name, period_start,
            total_booked, completed_count, cancelled_count, no_show_count,
            cancellation_rate_pct, avg_actual_duration_min, distinct_clients,
            morning_bookings, afternoon_bookings, evening_bookings,
            mom_bookings_growth_pct
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        if service_id is not None:
            payload["service_id"] = service_id
        if location_id is not None:
            payload["location_id"] = location_id

        return await self._post(
            "/api/v1/leo/services/booking-stats", payload
        )

    async def get_service_staff_matrix(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        service_id: Optional[int] = None,
        staff_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Who performs which services, how often, how much revenue.
        Per staff per service per month.

        Powers: Q21,22,23

        Key fields returned per row:
            service_id, service_name, staff_id, staff_name, period_start,
            performed_count, revenue, commission_paid
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        if service_id is not None:
            payload["service_id"] = service_id
        if staff_id is not None:
            payload["staff_id"] = staff_id

        return await self._post(
            "/api/v1/leo/services/staff-matrix", payload
        )

    async def get_service_co_occurrence(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        min_occurrences: int = 2,
    ) -> list[dict]:
        """
        Pairs of services performed together in the same paid visit.

        Powers: Q19

        Key fields returned per row:
            period_start, service_a_id, service_a_name,
            service_b_id, service_b_name, co_occurrence_count
        """
        payload = {
            "business_id": business_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "min_occurrences": min_occurrences,
        }

        return await self._post(
            "/api/v1/leo/services/co-occurrence", payload
        )

    async def get_service_catalog(
        self,
        business_id: int,
        active_only: bool = False,
    ) -> list[dict]:
        """
        Current catalog snapshot — one row per service.
        Not period-based. Includes lifecycle signals.

        Powers: Q7,9,10,20,27,29,30

        Key fields returned per row:
            service_id, service_name, category_name,
            list_price, default_commission_rate, commission_type,
            scheduled_duration_min, is_active, created_at,
            home_location_id, last_sold_date, days_since_last_sale,
            lifetime_performed_count, new_client_first_service_count,
            dormant_flag, is_new_this_year,
            avg_discount_pct, scheduled_vs_actual_delta_min
        """
        payload = {
            "business_id": business_id,
            "active_only": active_only,
        }

        return await self._post(
            "/api/v1/leo/services/catalog", payload
        )

    # ── CLIENTS DOMAIN ────────────────────────────────────────────────────────
    # 3 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 5 — clients

    async def get_clients_retention_snapshot(
        self,
        business_id: int,
        period_start: date,
        period_end: date,
        ref_date: date,
        churn_threshold_days: int = 60,
        only_reachable: bool = False,
        only_not_deleted: bool = False,
        include_names: bool = False,          # ← default False for AI path (PII)
        sort_by: str = "lifetime_revenue",
        sort_order: str = "desc",
        limit: int = 1000,
        offset: int = 0,
    ) -> list[dict]:
        """
        EP1 — Per-client retention snapshot. One row per client linked to the
        business. Includes ranks, flags, LTV decile, and (optionally) names.

        Powers per-client questions: Q7, Q8, Q9, Q11, Q14, Q18, Q21, Q22.

        CRITICAL PII NOTE:
          `include_names` defaults to False on the Python client. The AI
          path must NEVER flip this to True. Only ops/CSV export tools
          should override. Keeps first_name/last_name out of RAG chunks
          by default (defense in depth — Step 1 decision D10).
        """
        payload = {
            "business_id":          business_id,
            "period_start":         period_start.isoformat(),
            "period_end":           period_end.isoformat(),
            "ref_date":             ref_date.isoformat(),
            "churn_threshold_days": churn_threshold_days,
            "only_reachable":       only_reachable,
            "only_not_deleted":     only_not_deleted,
            "include_names":        include_names,
            "sort_by":              sort_by,
            "sort_order":           sort_order,
            "limit":                limit,
            "offset":               offset,
        }
        body = await self._post_full(
            "/api/v1/leo/clients/retention-snapshot",
            payload,
        )
        return body.get("data", [])

    async def get_clients_cohort_monthly(
        self,
        business_id: int,
        start_month: date,
        end_month: date,
        ref_date: date,
        churn_threshold_days: int = 60,
    ) -> list[dict]:
        """
        EP2 — Per-period cohort summary with MoM deltas and cohort retention.
        One row per (business, month).

        Powers aggregate/trend questions: Q1, Q2, Q3, Q4, Q5, Q6, Q12,
        Q15, Q16, Q17, Q19, Q23.

        Cohort retention (retention_rate_pct) = Option A — % of prior
        period's actives who returned this period. Not a simple
        aggregate ratio. See Step 3 spec §3.4.
        """
        payload = {
            "business_id":          business_id,
            "start_month":          start_month.isoformat(),
            "end_month":            end_month.isoformat(),
            "ref_date":             ref_date.isoformat(),
            "churn_threshold_days": churn_threshold_days,
        }
        body = await self._post_full(
            "/api/v1/leo/clients/cohort-monthly",
            payload,
        )
        return body.get("data", [])

    async def get_clients_per_location_monthly(
        self,
        business_id: int,
        start_month: date,
        end_month: date,
        location_id: Optional[int] = None,
    ) -> list[dict]:
        """
        EP3 — Per-location per-month breakdown. One row per (business,
        location, month).

        Powers branch-comparison questions: Q20 (which branch got most
        new clients), Q10 (per-location decomposition of new-client drop).

        Attribution is by first-visit location (where the client was
        acquired), not home location.
        """
        payload = {
            "business_id": business_id,
            "start_month": start_month.isoformat(),
            "end_month":   end_month.isoformat(),
        }
        if location_id is not None:
            payload["location_id"] = location_id
        body = await self._post_full(
            "/api/v1/leo/clients/per-location-monthly",
            payload,
        )
        return body.get("data", [])

    # -------------------------------------------------------------------------
    # Internal HTTP helper
    # -------------------------------------------------------------------------

    async def _post(self, path: str, payload: dict) -> list[dict]:
        url = f"{self.base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                body = response.json()
                # All analytics endpoints return { "data": [...], "meta": {...} }
                return body.get("data", [])
        except httpx.HTTPStatusError as e:
            logger.error(
                "Analytics API HTTP error: %s %s → %s",
                e.request.method,
                e.request.url,
                e.response.status_code,
            )
            raise
        except httpx.RequestError as e:
            logger.error("Analytics API request failed: %s", e)
            raise

    async def _post_full(self, path: str, payload: dict) -> dict:
        """Returns the full response body (includes data[] + meta fields)."""
        url = f"{self.base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                "Analytics API HTTP error: %s %s → %s",
                e.request.method,
                e.request.url,
                e.response.status_code,
            )
            raise
        except httpx.RequestError as e:
            logger.error("Analytics API request failed: %s", e)
            raise
