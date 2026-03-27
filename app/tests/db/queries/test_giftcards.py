"""
Tests for queries/giftcards.py
"""
from __future__ import annotations

import pytest
from .helpers import make_mock_pool, ORG_ID, FROM_DATE, TO_DATE

from app.services.db.queries.giftcards import (
    get_giftcard_liability,
    get_giftcards_issued_by_month,
    get_giftcard_redemptions,
    get_low_balance_giftcards,
    get_giftcard_issued_vs_redeemed,
)


# ---------------------------------------------------------------------------
# get_giftcard_liability
# ---------------------------------------------------------------------------

class TestGetGiftcardLiability:

    async def test_returns_single_dict(self):
        rows = [{"active_cards": 45, "inactive_cards": 12,
                 "total_cards": 57, "total_outstanding_balance": 2250.0,
                 "avg_balance": 50.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_giftcard_liability(pool, ORG_ID)
        assert isinstance(result, dict)

    async def test_returns_correct_values(self):
        rows = [{"active_cards": 45, "inactive_cards": 12,
                 "total_cards": 57, "total_outstanding_balance": 2250.0,
                 "avg_balance": 50.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_giftcard_liability(pool, ORG_ID)
        assert result["total_outstanding_balance"] == 2250.0
        assert result["active_cards"] == 45

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[{"active_cards": 0,
                                             "inactive_cards": 0,
                                             "total_cards": 0,
                                             "total_outstanding_balance": 0,
                                             "avg_balance": 0}])
        await get_giftcard_liability(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_giftcard_table(self):
        pool, cursor = make_mock_pool(rows=[{"active_cards": 0,
                                             "inactive_cards": 0,
                                             "total_cards": 0,
                                             "total_outstanding_balance": 0,
                                             "avg_balance": 0}])
        await get_giftcard_liability(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_giftcard" in sql

    async def test_sql_distinguishes_active_inactive(self):
        pool, cursor = make_mock_pool(rows=[{"active_cards": 0,
                                             "inactive_cards": 0,
                                             "total_cards": 0,
                                             "total_outstanding_balance": 0,
                                             "avg_balance": 0}])
        await get_giftcard_liability(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Active = 1" in sql
        assert "Active = 0" in sql

    async def test_empty_fetchall_returns_empty_dict(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_giftcard_liability(pool, ORG_ID)
        assert result == {}


# ---------------------------------------------------------------------------
# get_giftcards_issued_by_month
# ---------------------------------------------------------------------------

class TestGetGiftcardsIssuedByMonth:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "cards_issued": 12, "total_value": 600.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_giftcards_issued_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcards_issued_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_passes_date_range(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcards_issued_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[1] == FROM_DATE
        assert params[2] == TO_DATE

    async def test_sql_filters_by_activation_date(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcards_issued_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "ActivationDate" in sql

    async def test_sql_groups_by_month(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcards_issued_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GROUP BY" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_giftcards_issued_by_month(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_giftcard_redemptions
# ---------------------------------------------------------------------------

class TestGetGiftcardRedemptions:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "redemption_count": 18,
                 "total_redeemed": 720.0, "avg_redemption": 40.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_sql_queries_tbl_visit(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_visit" in sql

    async def test_sql_filters_gcid_greater_than_zero(self):
        """GCID = 0 means no gift card used."""
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GCID" in sql and "> 0" in sql

    async def test_sql_filters_gcamount_greater_than_zero(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "GCAmount" in sql

    async def test_sql_filters_successful_payments(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "PaymentStatus" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_giftcard_redemptions(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []


# ---------------------------------------------------------------------------
# get_low_balance_giftcards
# ---------------------------------------------------------------------------

class TestGetLowBalanceGiftcards:

    async def test_returns_fetchall_result(self):
        rows = [{"id": 22, "gift_card_number": "GC-001234",
                 "balance": 5.0, "activation_date": "2025-12-01"}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_low_balance_giftcards(pool, ORG_ID)
        assert result == rows

    async def test_passes_org_id(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_low_balance_giftcards(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[0] == ORG_ID

    async def test_default_threshold_is_10(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_low_balance_giftcards(pool, ORG_ID)
        params = cursor.execute.call_args[0][1]
        assert params[1] == 10.0

    async def test_custom_threshold(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_low_balance_giftcards(pool, ORG_ID, threshold=25.0)
        params = cursor.execute.call_args[0][1]
        assert params[1] == 25.0

    async def test_sql_filters_active_cards_only(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_low_balance_giftcards(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "Active" in sql and "= 1" in sql

    async def test_sql_uses_balance_threshold(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_low_balance_giftcards(pool, ORG_ID)
        sql = cursor.execute.call_args[0][0]
        assert "GiftCardBalance" in sql and "<=" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_low_balance_giftcards(pool, ORG_ID)
        assert result == []


# ---------------------------------------------------------------------------
# get_giftcard_issued_vs_redeemed
# ---------------------------------------------------------------------------

class TestGetGiftcardIssuedVsRedeemed:

    async def test_returns_fetchall_result(self):
        rows = [{"month": "2026-03", "issued_value": 600.0,
                 "redeemed_value": 300.0, "net_liability_change": 300.0}]
        pool, _ = make_mock_pool(rows=rows)
        result = await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == rows

    async def test_passes_org_id_four_times(self):
        """Query uses org_id in 4 subqueries."""
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        org_id_count = sum(1 for p in params if p == ORG_ID)
        assert org_id_count == 4

    async def test_total_params_count(self):
        """Expect 12 params: 4 × (org_id, from_date, to_date)."""
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        params = cursor.execute.call_args[0][1]
        assert len(params) == 12

    async def test_sql_contains_union(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "UNION" in sql

    async def test_sql_references_both_tables(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "tbl_giftcard" in sql
        assert "tbl_visit" in sql

    async def test_sql_computes_net_liability(self):
        pool, cursor = make_mock_pool(rows=[])
        await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        sql = cursor.execute.call_args[0][0]
        assert "net_liability" in sql.lower() or "IFNULL" in sql

    async def test_empty_result(self):
        pool, _ = make_mock_pool(rows=[])
        result = await get_giftcard_issued_vs_redeemed(pool, ORG_ID, FROM_DATE, TO_DATE)
        assert result == []
