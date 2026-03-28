"""Unit tests for WarehouseClient composition and delegation."""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

import pytest

from app.services.db.warehouse_client import (
    WarehouseAppointmentsClient,
    WarehouseAttendanceClient,
    WarehouseCampaignsClient,
    WarehouseClient,
    WarehouseClientsClient,
    WarehouseEtlClient,
    WarehouseExpensesClient,
    WarehousePaymentsClient,
    WarehouseReviewsClient,
    WarehouseRevenueClient,
    WarehouseServicesClient,
    WarehouseStaffClient,
    WarehouseSubscriptionsClient,
)


def _public_async_methods(obj) -> list[str]:
    names: list[str] = []
    for name in dir(obj):
        if name.startswith("_"):
            continue
        attr = getattr(obj, name)
        if callable(attr) and inspect.iscoroutinefunction(attr):
            names.append(name)
    return sorted(names)


def _total_public_async_methods(wh: WarehouseClient) -> int:
    subattrs = (
        wh.revenue,
        wh.staff,
        wh.services,
        wh.clients,
        wh.appointments,
        wh.expenses,
        wh.reviews,
        wh.payments,
        wh.campaigns,
        wh.attendance,
        wh.subscriptions,
        wh.etl,
    )
    return sum(len(_public_async_methods(s)) for s in subattrs)


def test_subclients_exist_and_types(mock_pool):
    wh = WarehouseClient(mock_pool)
    assert isinstance(wh.revenue, WarehouseRevenueClient)
    assert isinstance(wh.staff, WarehouseStaffClient)
    assert isinstance(wh.services, WarehouseServicesClient)
    assert isinstance(wh.clients, WarehouseClientsClient)
    assert isinstance(wh.appointments, WarehouseAppointmentsClient)
    assert isinstance(wh.expenses, WarehouseExpensesClient)
    assert isinstance(wh.reviews, WarehouseReviewsClient)
    assert isinstance(wh.payments, WarehousePaymentsClient)
    assert isinstance(wh.campaigns, WarehouseCampaignsClient)
    assert isinstance(wh.attendance, WarehouseAttendanceClient)
    assert isinstance(wh.subscriptions, WarehouseSubscriptionsClient)
    assert isinstance(wh.etl, WarehouseEtlClient)


def test_pool_shared_across_subclients(mock_pool):
    wh = WarehouseClient(mock_pool)
    assert wh.revenue._pool is mock_pool
    assert wh.staff._pool is mock_pool
    assert wh.etl._pool is mock_pool


def test_from_pool_factory(mock_pool):
    wh = WarehouseClient.from_pool(mock_pool)
    assert isinstance(wh, WarehouseClient)
    assert wh._pool is mock_pool


def test_total_public_method_count(mock_pool):
    wh = WarehouseClient(mock_pool)
    assert _total_public_async_methods(wh) == 58


@pytest.mark.asyncio
async def test_delegates_revenue_get_monthly_trend(mock_pool):
    with patch(
        "app.services.db.warehouse.wh_revenue.get_monthly_trend",
        new_callable=AsyncMock,
    ) as m:
        m.return_value = []
        wh = WarehouseClient(mock_pool)
        await wh.revenue.get_monthly_trend(42, months=3)
        m.assert_awaited_once_with(mock_pool, 42, 3)
