"""Unit tests for scripts.etl_job — mocks only, no real DB."""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scripts.etl_job import (
    TABLE_REGISTRY,
    TableJob,
    compute_periods,
    get_active_org_ids,
    run_all,
    run_org,
    run_table_for_org,
)


@pytest.fixture
def mock_prod_pool() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_wh_pool() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_etl_logger() -> MagicMock:
    logger = MagicMock()
    logger.start = AsyncMock(return_value="run-uuid-123")
    logger.success = AsyncMock()
    logger.fail = AsyncMock()
    logger.cleanup_orphaned_runs = AsyncMock(return_value=0)
    return logger


def make_mock_table_job(
    table_name: str = "wh_monthly_revenue",
    *,
    needs_period: bool = True,
    multi_input: bool = False,
    extract_return: object | None = None,
) -> TableJob:
    extractor_cls = MagicMock()
    ext_inst = MagicMock()
    if extract_return is None:
        extract_return = [{"business_id": 42, "period_start": date(2026, 1, 1)}]
    ext_inst.extract = AsyncMock(return_value=extract_return)
    extractor_cls.return_value = ext_inst

    transform_fn = MagicMock(
        return_value=[{"business_id": 42, "period_start": date(2026, 1, 1)}],
    )

    loader_cls = MagicMock()
    load_inst = MagicMock()
    load_inst.load = AsyncMock(return_value=(1, 0))
    loader_cls.return_value = load_inst

    return TableJob(
        table_name=table_name,
        extractor_cls=extractor_cls,
        transform_fn=transform_fn,
        loader_cls=loader_cls,
        needs_period=needs_period,
        multi_input=multi_input,
    )


# ---------------------------------------------------------------------------
# compute_periods
# ---------------------------------------------------------------------------


def test_compute_periods_incremental_starts_2_months_ago() -> None:
    ps, _ = compute_periods("incremental", today=date(2026, 3, 15))
    assert ps == date(2026, 1, 1)


def test_compute_periods_incremental_ends_last_day_current_month() -> None:
    _, pe = compute_periods("incremental", today=date(2026, 3, 15))
    assert pe == date(2026, 3, 31)


def test_compute_periods_full_starts_2_years_ago() -> None:
    ref = date(2026, 3, 15)
    ps, _ = compute_periods("full", today=ref)
    assert ps.year == ref.year - 2
    assert ps.month == ref.month
    assert ps.day == 1


def test_compute_periods_explicit_args_override_mode() -> None:
    ps, pe = compute_periods(
        "full",
        "2025-06-01",
        "2025-06-30",
        today=date(2026, 3, 15),
    )
    assert ps == date(2025, 6, 1)
    assert pe == date(2025, 6, 30)


def test_compute_periods_returns_date_objects() -> None:
    ps, pe = compute_periods("incremental", today=date(2026, 1, 5))
    assert isinstance(ps, date)
    assert isinstance(pe, date)


def test_compute_periods_full_ends_last_day_current_month() -> None:
    _, pe = compute_periods("full", today=date(2026, 3, 15))
    assert pe == date(2026, 3, 31)


# ---------------------------------------------------------------------------
# run_table_for_org
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_table_calls_extractor_with_org_id(mock_etl_logger: MagicMock) -> None:
    job = make_mock_table_job()
    ext_inst = job.extractor_cls.return_value
    ps, pe = date(2026, 1, 1), date(2026, 1, 31)
    await run_table_for_org(
        job,
        42,
        ps,
        pe,
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    ext_inst.extract.assert_awaited_once_with(42, ps, pe)


@pytest.mark.asyncio
async def test_run_table_calls_transform_with_raw_rows(mock_etl_logger: MagicMock) -> None:
    job = make_mock_table_job()
    raw = [{"x": 1}]
    job.extractor_cls.return_value.extract = AsyncMock(return_value=raw)
    await run_table_for_org(
        job,
        1,
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    job.transform_fn.assert_called_once_with(raw)


@pytest.mark.asyncio
async def test_run_table_calls_loader_with_transformed_rows(mock_etl_logger: MagicMock) -> None:
    job = make_mock_table_job()
    transformed = [{"t": 1}]
    job.transform_fn.return_value = transformed
    await run_table_for_org(
        job,
        1,
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    load_inst = job.loader_cls.return_value
    load_inst.load.assert_awaited_once_with(transformed)


@pytest.mark.asyncio
async def test_run_table_dry_run_skips_loader(mock_etl_logger: MagicMock) -> None:
    job = make_mock_table_job()
    await run_table_for_org(
        job,
        1,
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
        dry_run=True,
    )
    job.loader_cls.return_value.load.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_table_no_period_extractor_called_without_dates(
    mock_etl_logger: MagicMock,
) -> None:
    job = make_mock_table_job(needs_period=False)
    ext_inst = job.extractor_cls.return_value
    await run_table_for_org(
        job,
        99,
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    ext_inst.extract.assert_awaited_once_with(99)


@pytest.mark.asyncio
async def test_run_table_reviews_multi_input_unpacks_tuple(mock_etl_logger: MagicMock) -> None:
    job = make_mock_table_job(
        "wh_review_summary",
        multi_input=True,
        extract_return=([], [], []),
    )
    await run_table_for_org(
        job,
        1,
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    job.transform_fn.assert_called_once_with([], [], [])


@pytest.mark.asyncio
async def test_run_table_subscriptions_multi_input_passes_same_list_twice(
    mock_etl_logger: MagicMock,
) -> None:
    rows = [{"a": 1}]
    job = make_mock_table_job(
        "wh_subscription_revenue",
        multi_input=True,
        extract_return=rows,
    )
    await run_table_for_org(
        job,
        1,
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    job.transform_fn.assert_called_once()
    args, _ = job.transform_fn.call_args
    assert args == (rows, rows)


@pytest.mark.asyncio
async def test_run_table_on_extractor_failure_calls_etl_logger_fail(
    mock_etl_logger: MagicMock,
) -> None:
    job = make_mock_table_job()
    job.extractor_cls.return_value.extract = AsyncMock(side_effect=RuntimeError("DB down"))
    with pytest.raises(RuntimeError, match="DB down"):
        await run_table_for_org(
            job,
            1,
            date(2026, 1, 1),
            date(2026, 1, 31),
            MagicMock(),
            MagicMock(),
            mock_etl_logger,
        )
    mock_etl_logger.fail.assert_awaited()


# ---------------------------------------------------------------------------
# run_org
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_org_returns_summary_dict(mock_etl_logger: MagicMock) -> None:
    job = make_mock_table_job()
    out = await run_org(
        7,
        [job],
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    assert set(out.keys()) == {
        "org_id",
        "tables_ok",
        "tables_failed",
        "total_inserted",
        "total_updated",
        "errors",
    }


@pytest.mark.asyncio
async def test_run_org_counts_successful_tables(mock_etl_logger: MagicMock) -> None:
    j1 = make_mock_table_job("wh_monthly_revenue")
    j2 = make_mock_table_job("wh_daily_revenue")
    out = await run_org(
        1,
        [j1, j2],
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    assert out["tables_ok"] == 2
    assert out["tables_failed"] == 0


@pytest.mark.asyncio
async def test_run_org_counts_failed_tables(mock_etl_logger: MagicMock) -> None:
    ok = make_mock_table_job("wh_monthly_revenue")
    bad = make_mock_table_job("wh_daily_revenue")
    bad.extractor_cls.return_value.extract = AsyncMock(side_effect=ValueError("nope"))
    out = await run_org(
        1,
        [ok, bad],
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    assert out["tables_ok"] == 1
    assert out["tables_failed"] == 1
    assert len(out["errors"]) == 1


@pytest.mark.asyncio
async def test_run_org_continues_after_table_failure(mock_etl_logger: MagicMock) -> None:
    bad = make_mock_table_job("wh_monthly_revenue")
    bad.extractor_cls.return_value.extract = AsyncMock(side_effect=RuntimeError("fail"))
    good = make_mock_table_job("wh_daily_revenue")
    await run_org(
        1,
        [bad, good],
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    assert bad.extractor_cls.return_value.extract.await_count == 1
    assert good.extractor_cls.return_value.extract.await_count == 1


@pytest.mark.asyncio
async def test_run_org_sums_inserted_and_updated(mock_etl_logger: MagicMock) -> None:
    j1 = make_mock_table_job("t1")
    j2 = make_mock_table_job("t2")
    j1.loader_cls.return_value.load = AsyncMock(return_value=(5, 2))
    j2.loader_cls.return_value.load = AsyncMock(return_value=(3, 1))
    out = await run_org(
        1,
        [j1, j2],
        date(2026, 1, 1),
        date(2026, 1, 31),
        MagicMock(),
        MagicMock(),
        mock_etl_logger,
    )
    assert out["total_inserted"] == 8
    assert out["total_updated"] == 3


# ---------------------------------------------------------------------------
# get_active_org_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_active_org_ids_returns_list_of_ints() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[{"Id": 1}, {"Id": 2}])
    ids = await get_active_org_ids(pool)
    assert ids == [1, 2]


@pytest.mark.asyncio
async def test_get_active_org_ids_empty_result() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[])
    assert await get_active_org_ids(pool) == []


@pytest.mark.asyncio
async def test_get_active_org_ids_sql_filters_client_status() -> None:
    pool = MagicMock()
    conn = MagicMock()
    cur = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cur)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=[])
    await get_active_org_ids(pool)
    sql = cur.execute.await_args.args[0]
    assert "ClientStatus" in sql


# ---------------------------------------------------------------------------
# TABLE_REGISTRY
# ---------------------------------------------------------------------------


def test_registry_has_12_entries() -> None:
    assert len(TABLE_REGISTRY) == 12


def test_registry_all_table_names_start_with_wh() -> None:
    assert all(j.table_name.startswith("wh_") for j in TABLE_REGISTRY)


def test_registry_client_metrics_needs_no_period() -> None:
    job = next(j for j in TABLE_REGISTRY if j.table_name == "wh_client_metrics")
    assert job.needs_period is False


def test_registry_reviews_is_multi_input() -> None:
    job = next(j for j in TABLE_REGISTRY if j.table_name == "wh_review_summary")
    assert job.multi_input is True


def test_registry_subscriptions_is_multi_input() -> None:
    job = next(j for j in TABLE_REGISTRY if j.table_name == "wh_subscription_revenue")
    assert job.multi_input is True


def test_registry_all_have_extractor_loader_transform() -> None:
    for job in TABLE_REGISTRY:
        assert job.extractor_cls is not None
        assert job.transform_fn is not None
        assert job.loader_cls is not None


# ---------------------------------------------------------------------------
# run_all (smoke with mocks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_all_closes_pools() -> None:
    prod = MagicMock()
    prod.close = AsyncMock()
    wh = MagicMock()
    wh.close = AsyncMock()
    args = MagicMock()
    args.mode = "incremental"
    args.org_id = 1
    args.table = None
    args.period_start = None
    args.period_end = None
    args.dry_run = True

    el_inst = MagicMock()
    el_inst.cleanup_orphaned_runs = AsyncMock(return_value=0)

    with (
        patch("scripts.etl_job.DBPool.from_env", AsyncMock(return_value=prod)),
        patch("scripts.etl_job.PGPool.from_env", AsyncMock(return_value=wh)),
        patch("scripts.etl_job.ETLLogger", return_value=el_inst),
        patch("scripts.etl_job.get_active_org_ids", AsyncMock(return_value=[1])),
        patch(
            "scripts.etl_job.run_org",
            AsyncMock(
                return_value={
                    "org_id": 1,
                    "tables_ok": 1,
                    "tables_failed": 0,
                    "total_inserted": 0,
                    "total_updated": 0,
                    "errors": [],
                },
            ),
        ),
    ):
        await run_all(args)

    prod.close.assert_awaited_once()
    wh.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_all_unknown_table_skips_run_org() -> None:
    prod = MagicMock()
    prod.close = AsyncMock()
    wh = MagicMock()
    wh.close = AsyncMock()
    args = MagicMock()
    args.mode = "incremental"
    args.org_id = 1
    args.table = "wh_nonexistent"
    args.period_start = None
    args.period_end = None
    args.dry_run = False

    el_inst = MagicMock()
    el_inst.cleanup_orphaned_runs = AsyncMock(return_value=0)

    with patch("scripts.etl_job.run_org", AsyncMock()) as m_run_org:
        with (
            patch("scripts.etl_job.DBPool.from_env", AsyncMock(return_value=prod)),
            patch("scripts.etl_job.PGPool.from_env", AsyncMock(return_value=wh)),
            patch("scripts.etl_job.ETLLogger", return_value=el_inst),
            patch("scripts.etl_job.get_active_org_ids", AsyncMock(return_value=[1])),
        ):
            await run_all(args)

    m_run_org.assert_not_awaited()
