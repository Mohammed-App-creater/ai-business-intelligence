"""
Standalone HTTP client for the analytics backend — used by ETL scripts only.
Does NOT depend on app.core.config or any FastAPI machinery.
"""

import httpx
from datetime import date


class AnalyticsClientError(Exception):
    pass


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
    Lightweight HTTP wrapper used by ETL scripts.
    Constructed with a base_url; owns its own httpx.AsyncClient lifecycle.
    """

    def __init__(self, base_url: str, api_key: str | None = None):
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if api_key:
            headers["X-API-Key"] = api_key
        self._http = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=30.0)

    async def close(self):
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    # ── Revenue endpoints (6 slices) ────────────────────────────────────────

    async def get_revenue_monthly_summary(
        self, business_id: int, start: date, end: date
    ) -> list[dict]:
        return await _get(self._http, "/revenue/monthly", business_id, start, end)

    async def get_revenue_payment_types(
        self, business_id: int, start: date, end: date
    ) -> list[dict]:
        return await _get(self._http, "/revenue/by-payment-type", business_id, start, end)

    async def get_revenue_by_staff(
        self, business_id: int, start: date, end: date
    ) -> list[dict]:
        return await _get(self._http, "/revenue/by-staff", business_id, start, end)

    async def get_revenue_by_location(
        self, business_id: int, start: date, end: date
    ) -> list[dict]:
        return await _get(self._http, "/revenue/by-location", business_id, start, end)

    async def get_revenue_promo_impact(
        self, business_id: int, start: date, end: date
    ) -> list[dict]:
        return await _get(self._http, "/revenue/promo-usage", business_id, start, end)

    async def get_revenue_failed_refunds(
        self, business_id: int, start: date, end: date
    ) -> list[dict]:
        return await _get(self._http, "/revenue/failed-refunds", business_id, start, end)
