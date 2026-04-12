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
    Sprint 2  — appointments ⬜
    Sprint 3  — staff        ⬜
    Sprint 4  — services     ⬜
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
