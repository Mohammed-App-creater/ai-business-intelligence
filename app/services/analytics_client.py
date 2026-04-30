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
    Sprint 5  — clients      ✅
    Sprint 6  — marketing    ✅
    Sprint 7  — memberships  ✅
    Sprint 8  — giftcards    ✅
    Sprint 9  — promos       ✅
    Sprint 10 — expenses     ✅
    Sprint 11 — forms        ✅
    """
    
    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        *,
        api_key: str | None = None,
        bearer_token: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        # Resolve auth — explicit args win, else fall back to settings.
        # Both headers are sent if both are set. Backend team will confirm
        # which one they want; once confirmed we drop the unused one.
        resolved_key   = api_key      if api_key      is not None else settings.ANALYTICS_BACKEND_API_KEY
        resolved_token = bearer_token if bearer_token is not None else settings.ANALYTICS_BACKEND_BEARER_TOKEN

        self._auth_headers: dict[str, str] = {}
        if resolved_token:
            self._auth_headers["Authorization"] = f"Bearer {resolved_token}"
        if resolved_key:
            self._auth_headers["X-API-Key"] = resolved_key

        logger.debug(
            "AnalyticsClient auth: bearer=%s api_key=%s",
            "yes" if resolved_token else "no",
            "yes" if resolved_key else "no",
        )


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

    # ── MARKETING DOMAIN ──────────────────────────────────────────────────────
    # 3 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 6 — marketing

    async def get_marketing_campaign_summary(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        channel: Optional[str] = None,
        recurring_only: bool = False,
        include_inactive: bool = False,
    ) -> list[dict]:
        """
        EP1 — Per-campaign-per-execution performance rollup.

        Returns one row per (campaign × execution_date) with rates,
        ranks, and the is_expired_but_active workflow-health flag.
        Campaigns with no executions still appear (execution_date=None)
        so the caller can detect expired-but-active workflow issues.

        Powers Q1, Q4, Q5–Q14, Q25, Q26, Q31, Q33 of the Marketing domain.

        Parameters
        ----------
        business_id : int
            Tenant scope. Validated against X-API-Key on the backend.
        start_date, end_date : date
            Filter on execution_date. ISO YYYY-MM-DD.
        channel : Optional[str]
            Filter by channel: "email" | "mobile" | "sms". None = all.
        recurring_only : bool
            If True, only return campaigns where is_recurring = 1.
        include_inactive : bool
            Default False. If True, include campaigns with is_active = 0.
        """
        payload: dict = {
            "business_id":      business_id,
            "period_start":     start_date.isoformat(),
            "period_end":       end_date.isoformat(),
            "recurring_only":   recurring_only,
            "include_inactive": include_inactive,
        }
        if channel is not None:
            payload["channel"] = channel

        body = await self._post_full(
            "/api/v1/leo/marketing/campaign-summary",
            payload,
        )
        return body.get("data", [])

    async def get_marketing_channel_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP2 — Per-period rollup fusing email/SMS volume, perf, and unsubscribe snapshot.

        Returns one row per period. Combines three data streams:
          - Send volume (emails_sent, sms_sent from tbl_smsemailcount)
          - Campaign performance aggregated by channel
          - Unsubscribe snapshot (email/sms_unsubscribed_count, contactable counts)
          - Derived deltas (email_net_unsub_delta, contactable_mom_pct)

        NOTE: sms_open_rate_pct is structurally NULL — SMS has no open tracking.
        AI prompt must render NULL as "no data", not as "0%".

        Powers Q2, Q3, Q19–Q24, Q27, Q28, Q32, Q34.
        """
        payload: dict = {
            "business_id":  business_id,
            "period_start": start_date.isoformat(),
            "period_end":   end_date.isoformat(),
        }
        body = await self._post_full(
            "/api/v1/leo/marketing/channel-monthly",
            payload,
        )
        return body.get("data", [])

    async def get_marketing_promo_attribution_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        location_id: Optional[int] = None,
        campaign_id: Optional[int] = None,
    ) -> list[dict]:
        """
        EP3 — Campaign revenue attribution per (campaign, period, location).

        Links a campaign's promo code to actual paid visits via the 3-hop chain:
            campaign.PromoCode (varchar)
              → tbl_promo.PromoCode (varchar match)
              → tbl_promo.Id (int)
              → tbl_visit.PromoCode (int FK)

        DOUBLE TENANT BIND (Step 2 DD4): backend enforces filters on BOTH
        campaign.TenantID AND visit.OrganizationId because tbl_promo has no
        OrganizationId column. Removing either filter is a tenant-isolation
        violation — do not override on the AI side.

        Powers Q15–Q18, Q29, Q30.

        Parameters
        ----------
        location_id : Optional[int]
            Filter to a single branch. None = all locations.
        campaign_id : Optional[int]
            Filter to a single campaign. None = all campaigns.
        """
        payload: dict = {
            "business_id":  business_id,
            "period_start": start_date.isoformat(),
            "period_end":   end_date.isoformat(),
        }
        if location_id is not None:
            payload["location_id"] = location_id
        if campaign_id is not None:
            payload["campaign_id"] = campaign_id

        body = await self._post_full(
            "/api/v1/leo/marketing/promo-attribution-monthly",
            payload,
        )
        return body.get("data", [])

    # ── MEMBERSHIPS DOMAIN ────────────────────────────────────────────────────
    # 2 endpoints, GET + query string (different shape from POST domains).
    # Sprint 7 — memberships

    async def get_memberships(
        self,
        business_id: int,
        as_of_date: Optional[date] = None,
        include_canceled: bool = True,
    ) -> dict:
        """
        Set A — unit-grain memberships snapshot.
        GET /api/v1/analytics/memberships

        Returns the full payload:
            {
              "business_id":  int,
              "as_of_date":   "YYYY-MM-DD",
              "generated_at": ISO-8601 Zulu,
              "row_count":    int,
              "data":         [ {membership row}, ... ]
            }

        Powers questions: Q1, Q3, Q8, Q9, Q10, Q11, Q13, Q15, Q17, Q18, Q19,
                          Q20, Q21, M-LQ1, M-LQ2, M-LQ6
        """
        params: dict = {
            "business_id":      business_id,
            "include_canceled": str(include_canceled).lower(),
        }
        if as_of_date is not None:
            params["as_of_date"] = as_of_date.isoformat()
        return await self._get(
            f"{self.base_url}/api/v1/analytics/memberships",
            params=params,
        )

    async def get_memberships_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        Set B — location-month rollup.
        GET /api/v1/analytics/memberships/monthly

        Returns the full payload:
            {
              "business_id":  int,
              "period_start": "YYYY-MM-DD",
              "period_end":   "YYYY-MM-DD",
              "generated_at": ISO-8601 Zulu,
              "row_count":    int,
              "data":         [ {monthly row}, ... ]
            }

        Powers questions: Q2, Q4, Q5, Q6, Q7, Q12, Q14,
                          M-LQ3, M-LQ4, M-LQ5, M-LQ7, M-LQ8

        Max date range: 36 months. Backend returns 400 if exceeded.
        """
        return await self._get(
            f"{self.base_url}/api/v1/analytics/memberships/monthly",
            params={
                "business_id": business_id,
                "start_date":  start_date.isoformat(),
                "end_date":    end_date.isoformat(),
            },
        )

    # ── PROMOS DOMAIN ─────────────────────────────────────────────────────────
    # 6 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 8 — promos

    async def get_promos_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP1 — Per-period promo rollup.

        Returns one row per month in window with org-wide redemption totals,
        discount totals, distinct codes used, and promo-visit-pct of all visits.

        Powers Q1, Q2, Q4-Q8, Q12, Q26.

        Key fields returned per row:
            period_month (YYYY-MM-DD), total_visits, promo_redemptions,
            distinct_codes_used, promo_visit_pct, total_discount_given,
            avg_discount_per_redemption,
            prev_month_redemptions, prev_month_discount
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/promos/monthly", payload)

    async def get_promos_codes_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP2A — Per-code per-period redemption stats.

        Returns one row per (period × code) where activity exists. Each row
        carries promo metadata (code string, label, Amount metadata, active
        flag, expiration) plus redemption metrics.

        Powers Q9, Q11, Q13, Q14, Q15, Q24, Q25.

        Orphan handling (Step 2 N1): rows for promo IDs that exist in
        tbl_visit but NOT in tbl_promo arrive with promo_code_string=NULL,
        promo_label=NULL. Pass through unchanged — the doc generator
        renders these as 'unknown promo (ID #N)'.

        Key fields returned per row:
            period_month, promo_id, promo_code_string, promo_label,
            promo_amount_metadata, is_active, expiration_date,
            redemptions, total_discount, avg_discount, max_single_discount
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/promos/codes", payload)

    async def get_promos_codes_window(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP2B — Per-code window-total snapshot (no period grain).

        Returns one row per code with full-window aggregates. Includes
        is_expired_now (snapshot of expiration vs ref date) for the
        active-but-expired data quality flag.

        Powers Q3, Q10.

        Key fields returned per row:
            promo_id, promo_code_string, promo_label, promo_amount_metadata,
            is_active, expiration_date, is_expired_now,
            redemptions, total_discount, avg_discount, max_single_discount
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/promos/codes-window", payload)

    async def get_promos_locations_rollup(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP3A — Per-location per-period rollup (codes aggregated).

        Returns one row per (period × location) — total promo activity
        at that location for that month, summed across all promo codes.

        Powers Q18, Q19 (count side), Q20, Q21 (amount side).

        Key fields returned per row:
            period_month, location_id, location_name,
            total_promo_redemptions, distinct_codes_used,
            total_discount_given, avg_discount_per_redemption
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/promos/locations", payload)

    async def get_promos_locations_by_code(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP3B — Per-location per-code per-period detail.

        Returns one row per (period × location × code). Used for "which
        promo runs heavy at which branch" type questions.

        Powers Q18-Q21 (per-code variants).

        Key fields returned per row:
            period_month, location_id, location_name,
            promo_id, promo_code_string, promo_label,
            redemptions, total_discount, avg_discount
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/promos/locations-by-code", payload)

    async def get_promos_catalog_health(
        self,
        business_id: int,
    ) -> list[dict]:
        """
        EP4 — Catalog-level snapshot (point-in-time, no date range).

        Returns one row per catalog code with health flags:
          - is_expired           : expiration_date < snapshot_date
          - active_but_expired   : is_active=1 AND is_expired=1 (data quality issue)
          - is_dormant           : zero redemptions in last 90d

        Powers Q22, Q23.

        Key fields returned per row:
            promo_id, promo_code_string, promo_label,
            is_active, expiration_date,
            is_expired, active_but_expired,
            redemptions_last_90d, is_dormant,
            snapshot_date
        """
        payload = {
            "business_id": business_id,
        }
        return await self._post("/api/v1/leo/promos/catalog-health", payload)

    # ── GIFT CARDS DOMAIN ─────────────────────────────────────────────────────
    # 8 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 8 — giftcards

    async def get_giftcard_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP1 — Per-month gift card redemption + activation summary.

        Powers: Q1, Q4, Q5, Q7, Q18, Q21, Q27, Q29, S1, S2
        Key fields per row:
            period_start, redemption_count, redemption_amount_total,
            distinct_cards_redeemed, activation_count,
            weekend_redemption_count, weekday_redemption_count,
            avg_uplift_per_visit, uplift_total,
            mom_redemption_pct, mom_activation_pct, yoy_redemption_pct
        Months with zero redemption AND zero activation are NOT emitted.
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/monthly", payload)

    async def get_giftcard_liability_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> dict:
        """
        EP2 — Outstanding liability snapshot at a given date.

        Powers: Q2, Q3, Q6, Q19, Q22
        Returns a SINGLE OBJECT (not a list):
            snapshot_date, active_card_count, outstanding_liability_total,
            avg_remaining_balance_excl_drained,
            avg_remaining_balance_incl_drained,
            drained_active_count, median_remaining_balance
        """
        payload = {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/liability-snapshot", payload)

    async def get_giftcard_by_staff(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP3 — Per-staff per-month redemption breakdown with rank.

        Powers: Q8
        Key fields per row:
            staff_id, staff_name, is_active, period_start,
            redemption_count, redemption_amount_total,
            distinct_cards_redeemed, rank_in_period
        Inactive staff (is_active=0) are kept — Tom Rivera-class fix per L2.
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/by-staff", payload)

    async def get_giftcard_by_location(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP4 — Per-location per-month redemption with within-org share + MoM.

        Powers: Q9, Q10, S3
        Key fields per row:
            location_id, location_name, period_start,
            redemption_count, redemption_amount_total,
            distinct_cards_redeemed, pct_of_org_redemption,
            mom_redemption_pct
        NOTE: Per-location ACTIVATION not available — tbl_giftcard has no
        LocationID column. Activation is org-rollup only (Risk R6).
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/by-location", payload)

    async def get_giftcard_aging_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> list[dict]:
        """
        EP5 — Aging buckets + dormancy summary.

        Powers: Q14, Q15, Q26, Q28
        Returns 5 rows: 4 aging_bucket rows + 1 dormancy_summary row.
        Each row has:
            row_type ('aging_bucket' | 'dormancy_summary'),
            age_bucket ('0-30' | '31-90' | '91-180' | '181+' | 'all'),
            card_count, liability_amount, pct_of_total_liability,
            never_redeemed_in_bucket, avg_days_to_first_redemption,
            longest_dormant_card_id, longest_dormant_days
        """
        payload = {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/aging-snapshot", payload)

    async def get_giftcard_anomalies_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
        start_date: date,
        end_date: date,
    ) -> dict:
        """
        EP6 — Anomalies snapshot (ALWAYS-EMIT — Q31 acceptance contract).

        Powers: Q24, Q25, Q31
        Returns a SINGLE OBJECT even when all counts are zero. Fields:
            snapshot_date, drained_active_count, drained_active_card_ids,
            deactivated_count, deactivated_value_total_derived,
            refunded_redemption_count, refunded_redemption_amount
        Refunded counts are computed within [start_date, end_date].
        """
        payload = {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
            "start_date":    start_date.isoformat(),
            "end_date":      end_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/anomalies-snapshot", payload)

    async def get_giftcard_denomination_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> list[dict]:
        """
        EP7 — Distribution of cards by derived face value bucket.

        Powers: Q12
        Returns 6 bucket rows (always all 6, even if card_count = 0):
            "$25 or less" | "$26-$50" | "$51-$100"
            "$101-$200"   | "$201-$500" | "$500+"
        Each row has: denomination_bucket, card_count, total_value_issued,
                      avg_face_value, pct_of_cards
        """
        payload = {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/denomination-snapshot", payload)

    async def get_giftcard_health_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> dict:
        """
        EP8 — Card population health (lifetime redemption rate + visit pattern).

        Powers: Q23, Q30
        Returns a SINGLE OBJECT:
            snapshot_date, total_cards_issued, cards_with_redemption,
            redemption_rate_pct, single_visit_drained_count,
            multi_visit_redeemed_count, single_visit_drained_pct_of_redeemed,
            multi_visit_redeemed_pct_of_redeemed, distinct_customer_redeemers
        """
        payload = {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        }
        return await self._post("/api/v1/leo/giftcards/health-snapshot", payload)

    # ── EXPENSES DOMAIN ───────────────────────────────────────────────────────
    # 6 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 10 — expenses

    async def get_expenses_monthly_summary(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP1 — Monthly expense rollup with MoM, QoQ, YTD, outlier flags.

        Returns one row per month in the window.

        Powers Q1–Q8, Q20, Q21, Q25, S1, S4.

        Key fields returned per row:
            period, total_expenses, transaction_count,
            avg_transaction, min_transaction, max_transaction,
            prev_month_expenses, mom_change_pct, mom_direction,
            ytd_total, window_cumulative,
            current_quarter_total, prev_quarter_total, qoq_change_pct,
            expense_rank_in_window, avg_monthly_in_window, months_in_window,
            large_txn_count, huge_txn_count

        Notes:
          - quarter fields are NULL when the quarter is incomplete (<3 months
            in window) — forces honest "insufficient data" from the AI.
          - ytd_total resets every January 1 (calendar year).
          - large_txn_count / huge_txn_count flag >$100K / >$1M single txns
            so the AI can caveat (not filter) suspicious values.
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post(
            "/api/v1/leo/expenses/monthly-summary", payload
        )

    async def get_expenses_category_breakdown(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
        include_subcategories: bool = False,
    ) -> list[dict]:
        """
        EP2 — Category-level breakdown per month with 3-month baseline + anomaly flag.

        Returns one row per (period × category). Dormant categories (zero
        activity in the period) are ABSENT from the response — dormant
        detection is a doc-layer responsibility (see expenses doc generator).

        Powers Q9–Q13, Q20–Q24, Q28 (via doc-layer logic).

        Key fields returned per row:
            period, category_id, category_name, category_total,
            transaction_count, month_total, pct_of_month, rank_in_month,
            prev_month_total, mom_change_pct,
            baseline_3mo_avg, baseline_months_available,
            pct_vs_baseline, anomaly_flag,
            subcategory_breakdown  (present only when include_subcategories=True)

        anomaly_flag values (for RAG retrieval against natural-language
        questions like "spiked", "more than usual"):
            spike (>= +50% vs baseline)
            elevated (>= +20%)
            normal (±20%)
            low (-20% to -50%)
            unusual_low (<= -50%)
            null (insufficient baseline)
        """
        payload = {
            "business_id":           business_id,
            "start_date":            start_date.isoformat(),
            "end_date":              end_date.isoformat(),
            "include_subcategories": include_subcategories,
        }
        return await self._post(
            "/api/v1/leo/expenses/category-breakdown", payload
        )

    async def get_expenses_location_breakdown(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP3 — Per-location monthly totals with MoM.

        The org-level rollup is NOT returned here — that's in monthly-summary.
        Keeping them separate matters for RAG retrieval (exclude_rollup on
        location comparison queries — Appointments sprint lesson).

        Powers Q16, Q17, Q18, Q19 (with cross), S3.

        Key fields returned per row:
            period, location_id, location_name, location_total,
            transaction_count, month_total, pct_of_month, rank_in_month,
            prev_month_total, mom_change_pct
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post(
            "/api/v1/leo/expenses/location-breakdown", payload
        )

    async def get_expenses_payment_type_breakdown(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP4 — Monthly Cash/Check/Card split.

        PaymentType enum (confirmed 2026-04-21 from frontend source):
            1 = Cash
            2 = Check
            3 = Card

        Unknown codes (future drift) return payment_type_label="Type {N}"
        as a visible drift alarm.

        Powers Q14, Q15.

        Key fields returned per row:
            period, payment_type_code, payment_type_label,
            type_total, transaction_count, month_total, pct_of_month
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post(
            "/api/v1/leo/expenses/payment-type-breakdown", payload
        )

    async def get_expenses_staff_attribution(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP5 — Aggregate staff entry-count ranking. PII-hardened.

        Only employees with >= 3 entries in the month are returned
        (k-anonymity guard). In typical tenants this returns 1 row/month
        (one admin logs all expenses) — that's expected and handled
        by the doc generator narrative.

        Powers Q26 (aggregate). Q27 (individual lookup by name) is blocked
        at the AI router and never reaches this call.

        Key fields returned per row:
            period, employee_id, employee_name,
            entries_logged, total_amount_logged, rank_in_month

        NOTE: total_amount_logged is returned for ops dashboards but
        the doc generator does NOT embed it into RAG chunks — per-
        individual dollar totals are borderline surveillance.
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post(
            "/api/v1/leo/expenses/staff-attribution", payload
        )

    async def get_expenses_category_location_cross(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        EP6 — Category mix per location per month — the heaviest query.

        Returns one row per (period × location × category) where the
        intersection has activity. For a multi-location salon with
        many categories over 6 months this can be 400+ rows.

        Powers Q19 (category mix by location).

        Key fields returned per row:
            period, location_id, location_name,
            category_id, category_name,
            cross_total, transaction_count,
            pct_of_location_month, rank_in_location_month
        """
        payload = {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        }
        return await self._post(
            "/api/v1/leo/expenses/category-location-cross", payload
        )

    # ── FORMS DOMAIN ──────────────────────────────────────────────────────────
    # 4 endpoints, same POST + JSON body pattern as all other domains.
    # Sprint 11 — forms

    async def get_forms_catalog_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> dict | None:
        """
        FQ1 — Catalog snapshot: template counts, dormancy splits.

        Returns single dict or None if biz unknown.
        """
        return await self._post(
            "/api/v1/leo/forms/catalog-snapshot",
            {
                "business_id":   business_id,
                "snapshot_date": snapshot_date.isoformat(),
            },
        )

    async def get_forms_monthly(
        self,
        business_id: int,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        FQ2 — Monthly submission summary across the requested window.

        Months with zero activity are NOT emitted (R7) — doc gen handles that.
        Returns list of dicts in DESC period order.
        """
        return await self._post(
            "/api/v1/leo/forms/monthly",
            {
                "business_id": business_id,
                "start_date":  start_date.isoformat(),
                "end_date":    end_date.isoformat(),
            },
        ) or []

    async def get_forms_per_form_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> list[dict]:
        """
        FQ3 — Per-form snapshot with rank and dormancy flags.

        Returns one row per template (active + inactive). Ordered by
        lifetime_submission_count DESC (rank 1 first).
        """
        return await self._post(
            "/api/v1/leo/forms/per-form-snapshot",
            {
                "business_id":   business_id,
                "snapshot_date": snapshot_date.isoformat(),
            },
        ) or []

    async def get_forms_lifecycle_snapshot(
        self,
        business_id: int,
        snapshot_date: date,
    ) -> dict:
        """
        FQ4 — Lifecycle status snapshot (ALWAYS-EMIT).

        Returns single dict — never None, even when biz has no submissions.
        Doc gen relies on this contract for F10 / F13 / "stuck" answers.
        """
        return await self._post(
            "/api/v1/leo/forms/lifecycle-snapshot",
            {
                "business_id":   business_id,
                "snapshot_date": snapshot_date.isoformat(),
            },
        )

    # -------------------------------------------------------------------------
    # Internal HTTP helper
    # -------------------------------------------------------------------------

    async def _post(self, path: str, payload: dict) -> list[dict]:
        url = f"{self.base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload, headers=self._auth_headers)
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
                response = await client.post(url, json=payload, headers=self._auth_headers)
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

    async def _get(self, url: str, *, params: dict | None = None) -> dict:
        """Returns the full response body for GET endpoints (memberships domain)."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params, headers=self._auth_headers)
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
