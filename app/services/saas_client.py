"""
SaaS REST API client for live data lookups.

Async HTTP client wrapping the SaaS platform's .NET REST API.
Shared service — used by ETL pipeline (data extraction) and Agent layer (V2).

All methods are read-only. Never creates, updates, or deletes data.
All public methods return data or empty results on failure — never raise.

Usage::

    client = SaasClient.from_env()
    employees = await client.search_employees(org_id=42)
    await client.close()

Or as async context manager::

    async with SaasClient.from_env() as client:
        employees = await client.search_employees(org_id=42)

Environment variables::

    SAAS_API_BASE_URL   — required, e.g. https://api.example.com
    SAAS_API_TOKEN      — required, Bearer token
    SAAS_API_TIMEOUT    — optional, default 15.0 seconds
    SAAS_API_MAX_RETRIES — optional, default 2
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class SaasClientError(Exception):
    """Base exception for SaaS API errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        endpoint: str = "",
        org_id: int | None = None,
    ) -> None:
        self.message = message
        self.status_code = status_code
        self.endpoint = endpoint
        self.org_id = org_id
        super().__init__(message)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SaasClient:
    """Async HTTP client for the SaaS REST API."""

    # ── Constructor & factories ──────────────────────────────────────────

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: float = 15.0,
        max_retries: int = 2,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    @classmethod
    def from_env(cls) -> SaasClient:
        """Create a client from environment variables."""
        base_url = os.environ["SAAS_API_BASE_URL"]
        token = os.environ["SAAS_API_TOKEN"]
        timeout = float(os.environ.get("SAAS_API_TIMEOUT", "15.0"))
        max_retries = int(os.environ.get("SAAS_API_MAX_RETRIES", "2"))
        return cls(base_url, token, timeout, max_retries)

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def close(self) -> None:
        """Close the underlying httpx client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> SaasClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ── Internal HTTP ────────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        """Lazy-initialise the httpx client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=httpx.Timeout(self._timeout),
            )
        return self._client

    async def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Core HTTP request with retry and error handling.

        Retries on 5xx, timeout, and connection errors.
        Does NOT retry on 4xx (that indicates a caller bug).

        Returns the parsed JSON as a dict.
        Raises ``SaasClientError`` on all failures.
        """
        client = await self._get_client()
        last_error: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                response = await client.request(
                    method, path, json=json, params=params,
                )
                response.raise_for_status()

                data = response.json()
                if not isinstance(data, dict):
                    # Some endpoints may return a raw list — wrap it
                    return {"data": data}

                # --- Envelope check ---
                # Dashboard endpoints: {"success": bool, "data": ..., "errorMessage": ...}
                if "success" in data and data["success"] is False:
                    msg = data.get("errorMessage") or "API returned success=false"
                    raise SaasClientError(
                        msg, status_code=response.status_code, endpoint=path,
                    )
                # Standard CRUD endpoints: {"isSuccess": bool, "message": ...}
                if "isSuccess" in data and data["isSuccess"] is False:
                    msg = data.get("message") or "API returned isSuccess=false"
                    raise SaasClientError(
                        msg, status_code=response.status_code, endpoint=path,
                    )

                return data

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status < 500:
                    raise SaasClientError(
                        f"HTTP {status}: {exc.response.text[:200]}",
                        status_code=status,
                        endpoint=path,
                    ) from exc
                last_error = exc

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = exc

            if attempt < self._max_retries:
                await asyncio.sleep(0.5 * attempt)

        raise SaasClientError(
            f"Failed after {self._max_retries} attempts: {last_error}",
            endpoint=path,
        )

    async def _get(
        self, path: str, params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Convenience wrapper for GET requests."""
        return await self._request("GET", path, params=params)

    async def _post(
        self, path: str, json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Convenience wrapper for POST requests."""
        return await self._request("POST", path, json=json or {})

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _extract_list(data: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
        """Try multiple keys to find a list in the response dict."""
        for key in keys:
            val = data.get(key)
            if isinstance(val, list):
                return val
        # Dashboard envelope: data is nested under "data"
        nested = data.get("data")
        if isinstance(nested, list):
            return nested
        if isinstance(nested, dict):
            for key in keys:
                val = nested.get(key)
                if isinstance(val, list):
                    return val
        return []

    # =====================================================================
    # Public API — 16 methods, grouped by domain
    # =====================================================================

    # ── Dashboard (3 methods) ────────────────────────────────────────────

    async def get_dashboard_overview(self, org_id: int) -> dict[str, Any] | None:
        """
        GET /api/DashboardNew/overview/{orgId}

        Returns daily KPI snapshot with today/week/month comparisons.

        Response structure::

            {
              "success": true,
              "data": {
                "employee": {...},
                "inventory": [...],
                "topServices": {"today": [], "week": [], "month": []},
                "custQueue": [...],
                "appointmentsToday": [...],
                "upcoming24Hours": {
                    "totalAppointments", "expectedEarnings", "expectedCommission"
                },
                "employeeReviews": [...],
                "reports": {
                    "today":  {totalVisits, totalCustomers, totalRevenue,
                               noShows, avgSpendValue, totalAppointments,
                               walkInCount, appointmentCount,
                               expectedEarnings, expectedCommission,
                               ...Vs variants for yesterday/lastWeek/lastMonth},
                    "week":   { ... same fields ... },
                    "month":  { ... same fields ... }
                }
              },
              "referenceDate": "2026-03-29",
              "errorMessage": null
            }
        """
        try:
            resp = await self._get(f"/api/DashboardNew/overview/{org_id}")
            return resp.get("data") if "data" in resp else resp
        except SaasClientError as e:
            logger.warning("get_dashboard_overview failed org=%s: %s", org_id, e)
            return None

    async def get_dashboard_overview_by_date(
        self, org_id: int, client_date: str,
    ) -> dict[str, Any] | None:
        """
        GET /api/DashboardNew/overview/{orgId}/{clientDate}

        Same response shape as ``get_dashboard_overview`` but for a specific
        date. *client_date* should be an ISO datetime string,
        e.g. ``"2026-03-29T00:00:00"``.
        """
        try:
            resp = await self._get(
                f"/api/DashboardNew/overview/{org_id}/{client_date}",
            )
            return resp.get("data") if "data" in resp else resp
        except SaasClientError as e:
            logger.warning(
                "get_dashboard_overview_by_date failed org=%s date=%s: %s",
                org_id, client_date, e,
            )
            return None

    async def get_employee_reviews(self, org_id: int) -> list[dict[str, Any]]:
        """
        GET /api/DashboardNew/employee-reviews/{orgId}

        Returns list of employee review records.
        Response envelope: ``{"success": true, "data": [...]}``.
        """
        try:
            resp = await self._get(
                f"/api/DashboardNew/employee-reviews/{org_id}",
            )
            data = resp.get("data")
            if isinstance(data, list):
                return data
            return []
        except SaasClientError as e:
            logger.warning(
                "get_employee_reviews failed org=%s: %s", org_id, e,
            )
            return []

    # ── Calendar / Appointments (1 method) ───────────────────────────────

    async def get_calendar_events(
        self,
        org_id: int,
        start_date: str | None = None,
        end_date: str | None = None,
        employee_id: int | None = None,
        location_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        POST /api/calendar/getevents

        Returns list of calendar event dicts. Each event contains:
        ``id``, ``title``, ``start``, ``end``, ``employeeid``, ``serviceid``,
        ``customerid``, ``customername``, ``servicename``, ``employeename``,
        ``branchname``, ``confirmed``, ``notes``, ``active``.
        """
        body: dict[str, Any] = {"organizationId": org_id}
        if start_date is not None:
            body["startDate"] = start_date
        if end_date is not None:
            body["endDate"] = end_date
        if employee_id is not None:
            body["employeeId"] = employee_id
        if location_id is not None:
            body["locationId"] = location_id
        try:
            resp = await self._post("/api/calendar/getevents", body)
            return self._extract_list(resp, "calendarEvents")
        except SaasClientError as e:
            logger.warning(
                "get_calendar_events failed org=%s: %s", org_id, e,
            )
            return []

    # ── Employee / Staff (2 methods) ─────────────────────────────────────

    async def search_employees(
        self, org_id: int, search: str = "",
    ) -> list[dict[str, Any]]:
        """
        POST /api/employee/search

        Returns list of employee dicts. Each employee contains:
        ``id``, ``firstName``, ``lastName``, ``mobilePhone``, ``commission``,
        ``active``, ``hireDate``, ``role``, ``baseSalary``, ``organizationId``,
        ``locationId``, ``locationName``, ``publicAvailable``, ``kioskAvailable``.
        """
        try:
            resp = await self._post("/api/employee/search", {
                "organizationId": org_id,
                "search": search,
            })
            return self._extract_list(resp, "employees")
        except SaasClientError as e:
            logger.warning(
                "search_employees failed org=%s: %s", org_id, e,
            )
            return []

    async def get_employee(self, employee_id: int) -> dict[str, Any] | None:
        """
        GET /api/employee/get/{id}

        Returns a single employee dict or ``None``.
        """
        try:
            resp = await self._get(f"/api/employee/get/{employee_id}")
            return resp.get("employee")
        except SaasClientError as e:
            logger.warning("get_employee failed id=%s: %s", employee_id, e)
            return None

    # ── Customer / Clients (2 methods) ───────────────────────────────────

    async def search_customers(
        self, org_id: int, search: str = "",
    ) -> list[dict[str, Any]]:
        """
        POST /api/customer/search

        Returns list of customer dicts. Each customer contains:
        ``id``, ``firstName``, ``lastName``, ``email``, ``mobilePhone``,
        ``points``, ``city``, ``state``, ``zip``, ``dob``, ``active``,
        ``subscriptionID``, ``subscriptionStatus``, ``notes``.
        """
        try:
            resp = await self._post("/api/customer/search", {
                "organizationId": org_id,
                "search": search,
            })
            return self._extract_list(resp, "customers")
        except SaasClientError as e:
            logger.warning(
                "search_customers failed org=%s: %s", org_id, e,
            )
            return []

    async def get_customer(self, customer_id: int) -> dict[str, Any] | None:
        """
        GET /api/customer/get/{id}

        Returns a single customer dict or ``None``.
        """
        try:
            resp = await self._get(f"/api/customer/get/{customer_id}")
            return resp.get("customer")
        except SaasClientError as e:
            logger.warning("get_customer failed id=%s: %s", customer_id, e)
            return None

    # ── Services (1 method) ──────────────────────────────────────────────

    async def get_services(self) -> list[dict[str, Any]]:
        """
        GET /api/service/get

        Returns list of service dicts. Each service contains:
        ``id``, ``name``, ``price``, ``commission``, ``commissionType``,
        ``active``, ``points``, ``product``, ``quantity``, ``categoryid``,
        ``duration``, ``desc``, ``deposit``, ``calColor``, ``publicService``,
        ``organizationId``.

        Note: scoped to authenticated org via Bearer token.
        """
        try:
            resp = await self._get("/api/service/get")
            return self._extract_list(resp, "services")
        except SaasClientError as e:
            logger.warning("get_services failed: %s", e)
            return []

    # ── Visits / Revenue (2 methods) ─────────────────────────────────────

    async def search_visits(
        self, org_id: int, search: str = "",
    ) -> list[dict[str, Any]]:
        """
        POST /api/visit/search

        Returns list of visit dicts. Each visit contains:
        ``id``, ``payment``, ``tips``, ``totalPay``, ``discount``, ``tax``,
        ``paymentType``, ``customerId``, ``employeeId``, ``locationId``,
        ``recDateTime``, ``gcAmount``, ``usedPoints``, ``notes``,
        ``paymentStatus``, ``promoCode``.
        """
        try:
            resp = await self._post("/api/visit/search", {
                "organizationId": org_id,
                "search": search,
            })
            return self._extract_list(resp, "visits")
        except SaasClientError as e:
            logger.warning("search_visits failed org=%s: %s", org_id, e)
            return []

    async def get_visits_by_date_range(
        self,
        org_id: int,
        from_date: str,
        to_date: str,
        location_id: int | None = None,
        employee_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        GET /api/CustomerReportVisit/GetAllVisits

        Date-filtered visits. Response schema discovered at runtime —
        tries common key patterns.
        """
        params: dict[str, Any] = {
            "fromDate": from_date,
            "toDate": to_date,
            "organizationId": org_id,
        }
        if location_id is not None:
            params["locationId"] = location_id
        if employee_id is not None:
            params["empId"] = employee_id
        try:
            resp = await self._get(
                "/api/CustomerReportVisit/GetAllVisits", params=params,
            )
            return self._extract_list(resp, "visits", "data")
        except SaasClientError as e:
            logger.warning(
                "get_visits_by_date_range failed org=%s: %s", org_id, e,
            )
            return []

    # ── Expenses (2 methods) ─────────────────────────────────────────────

    async def search_expenses(self, org_id: int) -> list[dict[str, Any]]:
        """
        POST /api/expense/SearchAllExpense

        Returns list of expense dicts. Each expense contains:
        ``id``, ``amount``, ``paymentType``, ``categoryName``,
        ``subCategoryName``, ``description``, ``notes``, ``locationID``,
        ``recDateTime``, ``organizationId``, ``categoryID``,
        ``subCategoryID``, ``isDeleted``.
        """
        try:
            resp = await self._post("/api/expense/SearchAllExpense", {
                "organizationId": org_id,
            })
            return self._extract_list(resp, "expenses")
        except SaasClientError as e:
            logger.warning("search_expenses failed org=%s: %s", org_id, e)
            return []

    async def filter_expenses(
        self,
        org_id: int,
        from_date: str | None = None,
        to_date: str | None = None,
        category_id: int = 0,
        location_id: int = 0,
    ) -> list[dict[str, Any]]:
        """
        POST /api/expense/expensefileter

        Filters expenses by date range, category, and location.
        Returns same shape as ``search_expenses``.
        """
        body: dict[str, Any] = {
            "organizationId": org_id,
            "category": category_id,
            "locationId": location_id,
        }
        if from_date is not None:
            body["from"] = from_date
        if to_date is not None:
            body["to"] = to_date
        try:
            resp = await self._post("/api/expense/expensefileter", body)
            return self._extract_list(resp, "expenses")
        except SaasClientError as e:
            logger.warning("filter_expenses failed org=%s: %s", org_id, e)
            return []

    # ── Attendance (1 method) ────────────────────────────────────────────

    async def get_attendance_by_date_range(
        self, org_id: int, start_date: str, end_date: str,
    ) -> list[dict[str, Any]]:
        """
        POST /api/attendance/GetByStartandEnddate

        Returns attendance records for the date range.  Avoids the
        ``tbl_attendance`` no-org-column gotcha — the API handles the
        employee JOIN internally.

        Response key is discovered at runtime — tries common patterns.
        """
        try:
            resp = await self._post("/api/attendance/GetByStartandEnddate", {
                "organizationId": org_id,
                "startDate": start_date,
                "endDate": end_date,
            })
            return self._extract_list(
                resp, "attendances", "attendance", "data",
            )
        except SaasClientError as e:
            logger.warning(
                "get_attendance_by_date_range failed org=%s: %s", org_id, e,
            )
            return []

    # ── Subscriptions (1 method) ─────────────────────────────────────────

    async def get_subscriptions(self, org_id: int) -> list[dict[str, Any]]:
        """
        POST /api/CustSubscription/get

        Returns list of customer subscription dicts. Each contains:
        ``id``, ``custId``, ``orgId``, ``serviceID``, ``amount``,
        ``discount``, ``active``, ``interval``, ``locId``,
        ``subCreateDate``, ``recDatetime``, ``subExecutionDate``,
        ``serviceName``, ``custprofileid``.

        Note: avoids the ``OrgId`` vs ``OrganizationId`` column gotcha.
        """
        try:
            resp = await self._post("/api/CustSubscription/get", {
                "organizationId": org_id,
            })
            return self._extract_list(resp, "subscription", "subscriptions")
        except SaasClientError as e:
            logger.warning("get_subscriptions failed org=%s: %s", org_id, e)
            return []

    # ── Marketing / Campaigns (1 method) ─────────────────────────────────

    async def get_campaigns(self, tenant_id: int) -> list[dict[str, Any]]:
        """
        POST /api/marketing/GetMarketingCampaignstenantid/{id}

        Returns list of marketing campaign dicts.  Response key is
        discovered at runtime.

        Note: uses ``TenantID`` (not ``OrganizationId``).  The
        *tenant_id* value is the same as ``org_id`` on this platform.
        Avoids the ``tbl_mrkcampaign.TenantID`` and
        ``tbl_executecampaign.Successed`` typo gotchas.
        """
        try:
            resp = await self._post(
                f"/api/marketing/GetMarketingCampaignstenantid/{tenant_id}",
            )
            return self._extract_list(
                resp, "campaigns", "marketingCampaigns", "data",
            )
        except SaasClientError as e:
            logger.warning(
                "get_campaigns failed tenant=%s: %s", tenant_id, e,
            )
            return []