import httpx
from datetime import date
from app.core.config import settings


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

    def __init__(self, http: httpx.AsyncClient):
        self.revenue = _RevenueClient(http)

    @classmethod
    def create(cls) -> tuple["AnalyticsClient", httpx.AsyncClient]:
        headers = {"Content-Type": "application/json"}
        if settings.ANALYTICS_BACKEND_API_KEY:
            headers["X-API-Key"] = settings.ANALYTICS_BACKEND_API_KEY
        http = httpx.AsyncClient(
            base_url=settings.ANALYTICS_BACKEND_URL,
            headers=headers,
            timeout=30.0,
        )
        return cls(http), http
