"""
Unit tests for app/services/saas_client.py

All tests mock httpx — no real API calls.
Follows project conventions: pytest + pytest-asyncio, all async.
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio

from app.services.saas_client import SaasClient, SaasClientError


# ═══════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def client() -> SaasClient:
    """Client with test defaults."""
    return SaasClient(
        base_url="https://api.test.com",
        token="test-token-123",
        timeout=5.0,
        max_retries=2,
    )


def _mock_response(
    status_code: int = 200,
    json_data: dict | list | None = None,
    text: str = "",
) -> httpx.Response:
    """Build a fake httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = text
    resp.is_success = 200 <= status_code < 300

    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
    else:
        resp.raise_for_status.return_value = None

    return resp


# ═══════════════════════════════════════════════════════════════════════════
# 1. Constructor & factories
# ═══════════════════════════════════════════════════════════════════════════

class TestConstructor:

    def test_base_url_trailing_slash_stripped(self):
        c = SaasClient("https://api.test.com/", "tok")
        assert c._base_url == "https://api.test.com"

    def test_defaults(self):
        c = SaasClient("https://api.test.com", "tok")
        assert c._timeout == 15.0
        assert c._max_retries == 2
        assert c._client is None

    def test_custom_values(self):
        c = SaasClient("https://x.com", "t", timeout=30.0, max_retries=5)
        assert c._timeout == 30.0
        assert c._max_retries == 5


class TestFromEnv:

    def test_reads_required_vars(self):
        env = {
            "SAAS_API_BASE_URL": "https://api.example.com",
            "SAAS_API_TOKEN": "my-token",
        }
        with patch.dict(os.environ, env, clear=False):
            c = SaasClient.from_env()
        assert c._base_url == "https://api.example.com"
        assert c._token == "my-token"
        assert c._timeout == 15.0
        assert c._max_retries == 2

    def test_reads_optional_vars(self):
        env = {
            "SAAS_API_BASE_URL": "https://api.example.com",
            "SAAS_API_TOKEN": "my-token",
            "SAAS_API_TIMEOUT": "30.0",
            "SAAS_API_MAX_RETRIES": "5",
        }
        with patch.dict(os.environ, env, clear=False):
            c = SaasClient.from_env()
        assert c._timeout == 30.0
        assert c._max_retries == 5

    def test_missing_base_url_raises(self):
        env = {"SAAS_API_TOKEN": "tok"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(KeyError):
                SaasClient.from_env()

    def test_missing_token_raises(self):
        env = {"SAAS_API_BASE_URL": "https://x.com"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(KeyError):
                SaasClient.from_env()


class TestLifecycle:

    @pytest.mark.asyncio
    async def test_close_when_no_client(self, client: SaasClient):
        await client.close()  # should not raise

    @pytest.mark.asyncio
    async def test_close_closes_httpx(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        client._client = mock_httpx
        await client.close()
        mock_httpx.aclose.assert_awaited_once()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_context_manager(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        client._client = mock_httpx
        async with client:
            pass
        mock_httpx.aclose.assert_awaited_once()


# ═══════════════════════════════════════════════════════════════════════════
# 2. Internal _request
# ═══════════════════════════════════════════════════════════════════════════

class TestRequest:

    @pytest.mark.asyncio
    async def test_successful_get(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(
            json_data={"employees": [{"id": 1}]},
        )
        client._client = mock_httpx

        result = await client._get("/api/test")
        assert result == {"employees": [{"id": 1}]}

    @pytest.mark.asyncio
    async def test_successful_post(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(
            json_data={"isSuccess": True, "customers": []},
        )
        client._client = mock_httpx

        result = await client._post("/api/test", {"orgId": 1})
        assert result["isSuccess"] is True

    @pytest.mark.asyncio
    async def test_4xx_raises_no_retry(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(
            status_code=400, text="Bad request",
        )
        client._client = mock_httpx

        with pytest.raises(SaasClientError) as exc_info:
            await client._get("/api/test")
        assert exc_info.value.status_code == 400
        assert mock_httpx.request.await_count == 1  # no retry

    @pytest.mark.asyncio
    async def test_5xx_retries(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(
            status_code=500, text="Server error",
        )
        client._client = mock_httpx

        with pytest.raises(SaasClientError):
            await client._get("/api/test")
        assert mock_httpx.request.await_count == 2  # max_retries=2

    @pytest.mark.asyncio
    async def test_timeout_retries(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        with pytest.raises(SaasClientError, match="Failed after 2 attempts"):
            await client._get("/api/test")
        assert mock_httpx.request.await_count == 2

    @pytest.mark.asyncio
    async def test_connect_error_retries(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.ConnectError("refused")
        client._client = mock_httpx

        with pytest.raises(SaasClientError):
            await client._get("/api/test")
        assert mock_httpx.request.await_count == 2

    @pytest.mark.asyncio
    async def test_is_success_false_raises(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(
            json_data={"isSuccess": False, "message": "Org not found"},
        )
        client._client = mock_httpx

        with pytest.raises(SaasClientError, match="Org not found"):
            await client._get("/api/test")

    @pytest.mark.asyncio
    async def test_success_false_raises(self, client: SaasClient):
        """Dashboard-style envelope with success=false."""
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(
            json_data={"success": False, "errorMessage": "Invalid org"},
        )
        client._client = mock_httpx

        with pytest.raises(SaasClientError, match="Invalid org"):
            await client._get("/api/test")

    @pytest.mark.asyncio
    async def test_list_response_wrapped(self, client: SaasClient):
        """If API returns a raw list, wrap it in a dict."""
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        resp = _mock_response()
        resp.json.return_value = [{"id": 1}, {"id": 2}]
        mock_httpx.request.return_value = resp
        client._client = mock_httpx

        result = await client._get("/api/test")
        assert result == {"data": [{"id": 1}, {"id": 2}]}

    @pytest.mark.asyncio
    async def test_retry_then_success(self, client: SaasClient):
        """First call 500, second call succeeds."""
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = [
            _mock_response(status_code=500, text="err"),
            _mock_response(json_data={"ok": True}),
        ]
        client._client = mock_httpx

        result = await client._get("/api/test")
        assert result == {"ok": True}
        assert mock_httpx.request.await_count == 2

    @pytest.mark.asyncio
    async def test_auth_header_set(self, client: SaasClient):
        """Verify the lazy client gets correct headers."""
        real_async_client = httpx.AsyncClient
        with patch("app.services.saas_client.httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock(spec=real_async_client)
            MockClient.return_value = mock_instance
            mock_instance.request.return_value = _mock_response(
                json_data={"ok": True},
            )
            client._client = None
            await client._get("/api/test")
            MockClient.assert_called_once()
            call_kwargs = MockClient.call_args[1]
            assert call_kwargs["headers"]["Authorization"] == "Bearer test-token-123"


# ═══════════════════════════════════════════════════════════════════════════
# 3. Extract list helper
# ═══════════════════════════════════════════════════════════════════════════

class TestExtractList:

    def test_direct_key(self):
        data = {"employees": [{"id": 1}]}
        assert SaasClient._extract_list(data, "employees") == [{"id": 1}]

    def test_nested_under_data(self):
        data = {"success": True, "data": {"employees": [{"id": 1}]}}
        assert SaasClient._extract_list(data, "employees") == [{"id": 1}]

    def test_data_is_list(self):
        data = {"success": True, "data": [{"id": 1}]}
        assert SaasClient._extract_list(data, "whatever") == [{"id": 1}]

    def test_no_match_returns_empty(self):
        data = {"success": True, "data": {"other": "value"}}
        assert SaasClient._extract_list(data, "employees") == []

    def test_tries_multiple_keys(self):
        data = {"subscription": [{"id": 1}]}
        result = SaasClient._extract_list(data, "subscriptions", "subscription")
        assert result == [{"id": 1}]


# ═══════════════════════════════════════════════════════════════════════════
# 4. Dashboard methods
# ═══════════════════════════════════════════════════════════════════════════

class TestDashboard:

    @pytest.mark.asyncio
    async def test_get_dashboard_overview_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "success": True,
            "data": {
                "reports": {"today": {"totalRevenue": 500}},
                "topServices": {"today": []},
            },
            "referenceDate": "2026-03-29",
        })
        client._client = mock_httpx

        result = await client.get_dashboard_overview(7)
        assert result is not None
        assert result["reports"]["today"]["totalRevenue"] == 500

    @pytest.mark.asyncio
    async def test_get_dashboard_overview_returns_none_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_dashboard_overview(7)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_dashboard_overview_by_date(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "success": True,
            "data": {"reports": {"today": {"totalRevenue": 100}}},
        })
        client._client = mock_httpx

        result = await client.get_dashboard_overview_by_date(
            7, "2026-03-01T00:00:00",
        )
        assert result is not None
        # Verify the date was in the URL
        call_args = mock_httpx.request.call_args
        assert "2026-03-01T00:00:00" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_get_dashboard_overview_by_date_returns_none_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.ConnectError("refused")
        client._client = mock_httpx

        result = await client.get_dashboard_overview_by_date(
            7, "2026-03-01T00:00:00",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_get_employee_reviews_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "success": True,
            "data": [{"empId": 1, "rating": 4.5}],
        })
        client._client = mock_httpx

        result = await client.get_employee_reviews(7)
        assert len(result) == 1
        assert result[0]["empId"] == 1

    @pytest.mark.asyncio
    async def test_get_employee_reviews_empty(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "success": True, "data": [],
        })
        client._client = mock_httpx

        result = await client.get_employee_reviews(7)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_employee_reviews_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_employee_reviews(7)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 5. Calendar methods
# ═══════════════════════════════════════════════════════════════════════════

class TestCalendar:

    @pytest.mark.asyncio
    async def test_get_calendar_events_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "calendarEvents": [
                {"id": 1, "title": "Haircut", "employeeid": 5},
            ],
        })
        client._client = mock_httpx

        result = await client.get_calendar_events(7)
        assert len(result) == 1
        assert result[0]["title"] == "Haircut"

    @pytest.mark.asyncio
    async def test_get_calendar_events_with_optional_params(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "calendarEvents": [],
        })
        client._client = mock_httpx

        await client.get_calendar_events(
            7, start_date="2026-03-01", end_date="2026-03-31",
            employee_id=5, location_id=2,
        )
        call_kwargs = mock_httpx.request.call_args[1]
        body = call_kwargs["json"]
        assert body["organizationId"] == 7
        assert body["startDate"] == "2026-03-01"
        assert body["endDate"] == "2026-03-31"
        assert body["employeeId"] == 5
        assert body["locationId"] == 2

    @pytest.mark.asyncio
    async def test_get_calendar_events_omits_none_params(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "calendarEvents": [],
        })
        client._client = mock_httpx

        await client.get_calendar_events(7)
        call_kwargs = mock_httpx.request.call_args[1]
        body = call_kwargs["json"]
        assert "startDate" not in body
        assert "endDate" not in body
        assert "employeeId" not in body

    @pytest.mark.asyncio
    async def test_get_calendar_events_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_calendar_events(7)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 6. Employee methods
# ═══════════════════════════════════════════════════════════════════════════

class TestEmployee:

    @pytest.mark.asyncio
    async def test_search_employees_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "employees": [
                {"id": 1, "firstName": "Sarah", "active": True},
                {"id": 2, "firstName": "Mike", "active": True},
            ],
        })
        client._client = mock_httpx

        result = await client.search_employees(7)
        assert len(result) == 2
        assert result[0]["firstName"] == "Sarah"

    @pytest.mark.asyncio
    async def test_search_employees_passes_search(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "employees": [],
        })
        client._client = mock_httpx

        await client.search_employees(7, search="Sarah")
        body = mock_httpx.request.call_args[1]["json"]
        assert body["search"] == "Sarah"
        assert body["organizationId"] == 7

    @pytest.mark.asyncio
    async def test_search_employees_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.search_employees(7)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_employee_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "employee": {"id": 1, "firstName": "Sarah"},
        })
        client._client = mock_httpx

        result = await client.get_employee(1)
        assert result is not None
        assert result["firstName"] == "Sarah"

    @pytest.mark.asyncio
    async def test_get_employee_returns_none_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_employee(1)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# 7. Customer methods
# ═══════════════════════════════════════════════════════════════════════════

class TestCustomer:

    @pytest.mark.asyncio
    async def test_search_customers_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "customers": [{"id": 10, "firstName": "John", "points": 150}],
        })
        client._client = mock_httpx

        result = await client.search_customers(7)
        assert len(result) == 1
        assert result[0]["points"] == 150

    @pytest.mark.asyncio
    async def test_search_customers_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.ConnectError("refused")
        client._client = mock_httpx

        result = await client.search_customers(7)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_customer_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "customer": {"id": 10, "firstName": "John"},
        })
        client._client = mock_httpx

        result = await client.get_customer(10)
        assert result is not None
        assert result["id"] == 10

    @pytest.mark.asyncio
    async def test_get_customer_returns_none_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_customer(10)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# 8. Service methods
# ═══════════════════════════════════════════════════════════════════════════

class TestServices:

    @pytest.mark.asyncio
    async def test_get_services_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "services": [
                {"id": 1, "name": "Haircut", "price": 35.0, "duration": 30},
            ],
        })
        client._client = mock_httpx

        result = await client.get_services()
        assert len(result) == 1
        assert result[0]["name"] == "Haircut"

    @pytest.mark.asyncio
    async def test_get_services_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_services()
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 9. Visit methods
# ═══════════════════════════════════════════════════════════════════════════

class TestVisits:

    @pytest.mark.asyncio
    async def test_search_visits_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "visits": [
                {"id": 1, "payment": 50.0, "tips": 10.0, "totalPay": 60.0},
            ],
        })
        client._client = mock_httpx

        result = await client.search_visits(7)
        assert len(result) == 1
        assert result[0]["totalPay"] == 60.0

    @pytest.mark.asyncio
    async def test_search_visits_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.search_visits(7)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_visits_by_date_range_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "visits": [{"id": 1}, {"id": 2}],
        })
        client._client = mock_httpx

        result = await client.get_visits_by_date_range(
            7, "2026-03-01", "2026-03-31",
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_visits_by_date_range_with_optional_params(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "visits": [],
        })
        client._client = mock_httpx

        await client.get_visits_by_date_range(
            7, "2026-03-01", "2026-03-31",
            location_id=2, employee_id=5,
        )
        call_kwargs = mock_httpx.request.call_args[1]
        params = call_kwargs["params"]
        assert params["locationId"] == 2
        assert params["empId"] == 5

    @pytest.mark.asyncio
    async def test_get_visits_by_date_range_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_visits_by_date_range(
            7, "2026-03-01", "2026-03-31",
        )
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 10. Expense methods
# ═══════════════════════════════════════════════════════════════════════════

class TestExpenses:

    @pytest.mark.asyncio
    async def test_search_expenses_success(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "expenses": [
                {"id": 1, "amount": "250.00", "categoryName": "Supplies"},
            ],
        })
        client._client = mock_httpx

        result = await client.search_expenses(7)
        assert len(result) == 1
        assert result[0]["categoryName"] == "Supplies"

    @pytest.mark.asyncio
    async def test_search_expenses_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.search_expenses(7)
        assert result == []

    @pytest.mark.asyncio
    async def test_filter_expenses_passes_all_params(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "expenses": [],
        })
        client._client = mock_httpx

        await client.filter_expenses(
            7, from_date="2026-03-01", to_date="2026-03-31",
            category_id=5, location_id=2,
        )
        body = mock_httpx.request.call_args[1]["json"]
        assert body["organizationId"] == 7
        assert body["from"] == "2026-03-01"
        assert body["to"] == "2026-03-31"
        assert body["category"] == 5
        assert body["locationId"] == 2

    @pytest.mark.asyncio
    async def test_filter_expenses_optional_dates(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "expenses": [],
        })
        client._client = mock_httpx

        await client.filter_expenses(7)
        body = mock_httpx.request.call_args[1]["json"]
        assert "from" not in body
        assert "to" not in body

    @pytest.mark.asyncio
    async def test_filter_expenses_returns_empty_on_error(
        self, client: SaasClient,
    ):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.filter_expenses(7)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 11. Attendance methods
# ═══════════════════════════════════════════════════════════════════════════

class TestAttendance:

    @pytest.mark.asyncio
    async def test_success_with_attendances_key(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "attendances": [{"empId": 1, "daysWorked": 22}],
        })
        client._client = mock_httpx

        result = await client.get_attendance_by_date_range(
            7, "2026-03-01", "2026-03-31",
        )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_success_with_attendance_key(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "attendance": [{"empId": 1}],
        })
        client._client = mock_httpx

        result = await client.get_attendance_by_date_range(
            7, "2026-03-01", "2026-03-31",
        )
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_passes_date_params(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "attendances": [],
        })
        client._client = mock_httpx

        await client.get_attendance_by_date_range(
            7, "2026-03-01", "2026-03-31",
        )
        body = mock_httpx.request.call_args[1]["json"]
        assert body["startDate"] == "2026-03-01"
        assert body["endDate"] == "2026-03-31"
        assert body["organizationId"] == 7

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_attendance_by_date_range(
            7, "2026-03-01", "2026-03-31",
        )
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 12. Subscription methods
# ═══════════════════════════════════════════════════════════════════════════

class TestSubscriptions:

    @pytest.mark.asyncio
    async def test_success_with_subscription_key(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "isSuccess": True,
            "subscription": [
                {"id": 1, "custId": 10, "amount": 49.99, "active": True},
            ],
        })
        client._client = mock_httpx

        result = await client.get_subscriptions(7)
        assert len(result) == 1
        assert result[0]["amount"] == 49.99

    @pytest.mark.asyncio
    async def test_success_with_subscriptions_key(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "subscriptions": [{"id": 1}],
        })
        client._client = mock_httpx

        result = await client.get_subscriptions(7)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.TimeoutException("timeout")
        client._client = mock_httpx

        result = await client.get_subscriptions(7)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# 13. Campaign methods
# ═══════════════════════════════════════════════════════════════════════════

class TestCampaigns:

    @pytest.mark.asyncio
    async def test_success_with_campaigns_key(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "campaigns": [
                {"id": 1, "name": "Summer Promo", "totalSent": 500},
            ],
        })
        client._client = mock_httpx

        result = await client.get_campaigns(7)
        assert len(result) == 1
        assert result[0]["name"] == "Summer Promo"

    @pytest.mark.asyncio
    async def test_success_with_data_envelope(self, client: SaasClient):
        """Dashboard-style envelope."""
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "success": True,
            "data": [{"id": 1, "name": "Fall Campaign"}],
        })
        client._client = mock_httpx

        result = await client.get_campaigns(7)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_tenant_id_in_url(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.return_value = _mock_response(json_data={
            "campaigns": [],
        })
        client._client = mock_httpx

        await client.get_campaigns(42)
        call_args = mock_httpx.request.call_args[0]
        assert "/42" in call_args[1]

    @pytest.mark.asyncio
    async def test_returns_empty_on_error(self, client: SaasClient):
        mock_httpx = AsyncMock(spec=httpx.AsyncClient)
        mock_httpx.request.side_effect = httpx.ConnectError("refused")
        client._client = mock_httpx

        result = await client.get_campaigns(7)
        assert result == []