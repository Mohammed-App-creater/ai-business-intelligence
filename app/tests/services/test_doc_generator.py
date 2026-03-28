"""
Unit tests for DocGenerator — mocked warehouse, LLM, embeddings, vector store.
"""
from __future__ import annotations

from contextlib import ExitStack
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.prompts.types import DocGenData
from app.services.doc_generator import DocGenerator, _DOMAIN_HANDLERS
from app.services.llm.types import UseCase
from app.prompts.doc_generation import anthropic as dg_anthropic
from app.prompts.doc_generation import openai as dg_openai


@pytest.fixture
def mock_gateway():
    gw = AsyncMock()
    gw.call_with_data = AsyncMock(return_value=MagicMock(
        content="The business recorded strong performance this period."
    ))
    return gw


@pytest.fixture
def mock_emb():
    emb = AsyncMock()
    emb.embed = AsyncMock(return_value=[0.1] * 1536)
    emb.embed_batch = AsyncMock(return_value=[[0.1] * 1536])
    return emb


@pytest.fixture
def mock_vs():
    vs = AsyncMock()
    vs.upsert = AsyncMock(return_value="some-uuid")
    vs.get_doc_ids = AsyncMock(return_value=[])
    vs.get_doc_metadata = AsyncMock(return_value=None)
    vs.exists = AsyncMock(return_value=False)
    return vs


@pytest.fixture
def mock_wh():
    wh = MagicMock()
    wh.revenue = MagicMock()
    wh.revenue.get_monthly_trend = AsyncMock(return_value=[
        {"business_id": 42, "period_start": date(2026, 1, 1), "period_end": date(2026, 1, 31),
         "gross_revenue": 9200.0, "total_tips": 820.0, "total_discounts": 340.0,
         "visit_count": 150, "avg_visit_value": 61.33,
         "cash_revenue": 4100.0, "card_revenue": 4600.0,
         "other_revenue": 500.0, "total_gc_amount": 500.0,
         "cancelled_visit_count": 0}
    ])
    wh.staff = MagicMock()
    wh.staff.get_staff_monthly_performance = AsyncMock(return_value=[
        {"employee_id": 7, "employee_name": "Sarah", "period_start": date(2026, 1, 1),
         "total_revenue": 3100.0, "avg_rating": 4.8, "total_visits": 45,
         "total_tips": 280.0, "total_commission": 620.0,
         "utilisation_rate": 87.0, "appointments_booked": 52,
         "appointments_completed": 45, "appointments_cancelled": 7,
         "review_count": 12}
    ])
    wh.staff.get_staff_trend = AsyncMock(return_value=[
        {"period_start": date(2026, 1, 1), "total_revenue": 3100.0, "total_visits": 45,
         "total_tips": 280.0, "total_commission": 620.0, "avg_rating": 4.8,
         "review_count": 12, "utilisation_rate": 87.0,
         "appointments_booked": 52, "appointments_completed": 45, "appointments_cancelled": 7},
        {"period_start": date(2025, 12, 1), "total_revenue": 2768.0, "total_visits": 40,
         "total_tips": 200.0, "total_commission": 500.0, "avg_rating": 4.5,
         "review_count": 8, "utilisation_rate": 80.0,
         "appointments_booked": 48, "appointments_completed": 40, "appointments_cancelled": 8},
    ])
    wh.services = MagicMock()
    wh.services.get_service_monthly_performance = AsyncMock(return_value=[
        {"service_name": "Balayage", "booking_count": 42, "revenue": 3360.0, "avg_price": 80.0},
    ])
    wh.clients = MagicMock()
    wh.clients.get_retention_summary = AsyncMock(return_value={
        "total_clients": 284, "active_count": 241, "churned_count": 43,
        "avg_visit_frequency_days": 22.0, "avg_spend_per_visit": 61.33,
    })
    wh.clients.get_top_clients_by_spend = AsyncMock(return_value=[
        {"customer_id": 99, "total_visits": 48, "total_spend": 4200.0, "days_since_last_visit": 3},
    ])
    wh.appointments = MagicMock()
    wh.appointments.get_appointment_monthly_summary = AsyncMock(return_value={
        "total_booked": 180, "confirmed_count": 165, "completed_count": 150,
        "cancelled_count": 30, "no_show_count": 8, "walkin_count": 45,
        "app_booking_count": 135, "cancellation_rate": 17.0, "completion_rate": 83.0,
    })
    wh.expenses = MagicMock()
    wh.expenses.get_expense_monthly_summary = AsyncMock(return_value=[
        {"location_id": 0, "category_name": "Supplies", "total_amount": 800.0, "expense_count": 10},
    ])
    wh.expenses.get_expense_total = AsyncMock(return_value={"total": 2100.0, "count": 24})
    wh.reviews = MagicMock()
    wh.reviews.get_review_monthly_summary = AsyncMock(return_value={
        "overall_avg_rating": 4.6, "total_review_count": 64,
        "emp_review_count": 32, "emp_avg_rating": 4.7,
        "visit_review_count": 18, "visit_avg_rating": 4.5,
        "google_review_count": 14, "google_avg_rating": 4.6, "google_bad_review_count": 2,
    })
    wh.payments = MagicMock()
    wh.payments.get_payment_monthly_breakdown = AsyncMock(return_value={
        "total_amount": 9200.0, "total_count": 150,
        "cash_amount": 4100.0, "cash_count": 68,
        "card_amount": 4600.0, "card_count": 72,
        "gift_card_amount": 500.0, "gift_card_count": 10,
        "other_amount": 0.0, "other_count": 0,
    })
    wh.campaigns = MagicMock()
    wh.campaigns.get_campaign_monthly_summary = AsyncMock(return_value=[
        {"campaign_name": "Spring Promo", "total_sent": 400, "open_rate": 42.0,
         "click_rate": 18.0, "fail_rate": 2.0},
    ])
    wh.attendance = MagicMock()
    wh.attendance.get_staff_attendance_monthly = AsyncMock(return_value=[
        {"employee_name": "Sarah", "total_hours_worked": 94.0, "days_worked": 22, "location_id": 0},
        {"employee_name": "Mia", "total_hours_worked": 52.0, "days_worked": 16, "location_id": 0},
    ])
    wh.subscriptions = MagicMock()
    wh.subscriptions.get_subscription_monthly_summary = AsyncMock(return_value={
        "active_subscriptions": 38, "new_subscriptions": 5, "cancelled_subscriptions": 2,
        "gross_subscription_revenue": 1900.0, "net_subscription_revenue": 1710.0,
        "avg_subscription_value": 50.0,
    })
    return wh


@pytest.fixture
def gen(mock_wh, mock_gateway, mock_emb, mock_vs):
    return DocGenerator(mock_wh, mock_gateway, mock_emb, mock_vs)


ORG_ID = 42
PERIOD = date(2026, 1, 1)


# --- DocGenData ----------------------------------------------------------------


def test_doc_gen_data_has_doc_domain_field():
    d = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="x",
    )
    assert d.doc_domain == "revenue"


def test_doc_gen_data_has_doc_type_field():
    d = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="x",
    )
    assert d.doc_type == "monthly_summary"


def test_doc_gen_data_has_kpi_block_field():
    d = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="KPI text",
    )
    assert d.kpi_block == "KPI text"


def test_doc_gen_data_entity_name_defaults_to_empty_string():
    d = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="x",
    )
    assert d.entity_name == ""


def test_doc_gen_data_required_fields_only():
    DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="...",
    )


# --- Prompt builders -----------------------------------------------------------


def _sample_docgen():
    return DocGenData(
        business_id="42",
        business_type="Hair Salon",
        period="March 2026",
        doc_domain="revenue",
        doc_type="monthly_summary",
        kpi_block="Revenue : $100",
        entity_name="",
    )


def test_anthropic_build_returns_tuple_of_two_strings():
    sys_p, user_p = dg_anthropic.build(_sample_docgen())
    assert isinstance(sys_p, str) and isinstance(user_p, str)


def test_anthropic_system_prompt_has_rules():
    sys_p, _ = dg_anthropic.build(_sample_docgen())
    assert "rules" in sys_p.lower() or "<rules>" in sys_p


def test_anthropic_user_has_kpi_data_tags():
    _, user_p = dg_anthropic.build(_sample_docgen())
    assert "<kpi_data>" in user_p


def test_anthropic_user_contains_kpi_block():
    _, user_p = dg_anthropic.build(_sample_docgen())
    assert "Revenue : $100" in user_p


def test_anthropic_user_contains_business_id():
    _, user_p = dg_anthropic.build(_sample_docgen())
    assert "42" in user_p


def test_anthropic_entity_name_included_when_set():
    d = DocGenData(
        business_id="42", business_type="Salon", period="P",
        doc_domain="staff", doc_type="individual", kpi_block="k",
        entity_name="Sarah",
    )
    _, user_p = dg_anthropic.build(d)
    assert "Sarah" in user_p
    assert "Entity" in user_p


def test_anthropic_entity_name_omitted_when_empty():
    _, user_p = dg_anthropic.build(_sample_docgen())
    assert "Entity" not in user_p


def test_openai_build_returns_tuple_of_two_strings():
    sys_p, user_p = dg_openai.build(_sample_docgen())
    assert isinstance(sys_p, str) and isinstance(user_p, str)


def test_openai_user_contains_kpi_block():
    _, user_p = dg_openai.build(_sample_docgen())
    assert "Revenue : $100" in user_p


def test_openai_user_contains_business_id():
    _, user_p = dg_openai.build(_sample_docgen())
    assert "42" in user_p


def test_openai_entity_name_included_when_set():
    d = DocGenData(
        business_id="42", business_type="Salon", period="P",
        doc_domain="staff", doc_type="individual", kpi_block="k",
        entity_name="Sarah",
    )
    _, user_p = dg_openai.build(d)
    assert "Sarah" in user_p


# --- Helpers -------------------------------------------------------------------


def test_pct_returns_correct_percentage():
    assert DocGenerator._pct(45, 100) == "45%"


def test_pct_returns_na_when_total_zero():
    assert DocGenerator._pct(10, 0) == "N/A"


def test_mom_returns_arrow_string():
    s = DocGenerator._mom(9200, 13100)
    assert "▼" in s


def test_mom_returns_empty_when_no_previous():
    assert DocGenerator._mom(9200, None) == ""


def test_mom_positive_change_uses_up_arrow():
    s = DocGenerator._mom(13100, 9200)
    assert "▲" in s


# --- KPI builders --------------------------------------------------------------


def test_kpi_revenue_contains_gross_revenue(gen):
    cur = {"gross_revenue": 9200.0, "visit_count": 150, "total_tips": 1, "total_discounts": 2,
           "avg_visit_value": 61.33, "cash_revenue": 1, "card_revenue": 1, "total_gc_amount": 1,
           "other_revenue": 1, "cancelled_visit_count": 0}
    text = gen._kpi_revenue(cur, None)
    assert "9,200" in text or "9200" in text.replace(",", "")


def test_kpi_revenue_includes_mom_when_prev_given(gen):
    cur = {"gross_revenue": 9200.0, "visit_count": 150, "total_tips": 0, "total_discounts": 0,
           "avg_visit_value": 60.0, "cash_revenue": 4600, "card_revenue": 4600,
           "total_gc_amount": 0, "other_revenue": 0, "cancelled_visit_count": 0}
    prev = {"gross_revenue": 10000.0, "visit_count": 200}
    text = gen._kpi_revenue(cur, prev)
    assert "▼" in text or "▲" in text


def test_kpi_revenue_skips_mom_when_no_prev(gen):
    cur = {"gross_revenue": 9200.0, "visit_count": 150, "total_tips": 0, "total_discounts": 0,
           "avg_visit_value": 60.0, "cash_revenue": 4600, "card_revenue": 4600,
           "total_gc_amount": 0, "other_revenue": 0, "cancelled_visit_count": 0}
    text = gen._kpi_revenue(cur, None)
    rev_line = next(ln for ln in text.split("\n") if ln.strip().startswith("Revenue"))
    assert "vs $" not in rev_line


def test_kpi_staff_monthly_lists_all_staff(gen):
    rows = [
        {"employee_name": "Sarah", "total_revenue": 3100, "avg_rating": 4.8, "total_visits": 45},
        {"employee_name": "Maria", "total_revenue": 2800, "avg_rating": 4.5, "total_visits": 40},
    ]
    text = gen._kpi_staff_monthly(rows, "March 2026")
    assert "Sarah" in text and "Maria" in text


def test_kpi_staff_individual_contains_staff_name(gen):
    rows = [
        {"period_start": date(2026, 1, 1), "total_revenue": 3100.0, "total_visits": 45,
         "total_tips": 280.0, "total_commission": 620.0, "avg_rating": 4.8, "review_count": 12,
         "utilisation_rate": 87.0, "appointments_booked": 52,
         "appointments_completed": 45, "appointments_cancelled": 7},
    ]
    text = gen._kpi_staff_individual(rows, "Sarah")
    assert "Sarah" in text


def test_kpi_appointments_shows_cancel_rate(gen):
    row = {"total_booked": 100, "confirmed_count": 90, "completed_count": 80, "cancelled_count": 10,
           "no_show_count": 2, "walkin_count": 5, "app_booking_count": 95,
           "cancellation_rate": 17.0, "completion_rate": 83.0}
    text = gen._kpi_appointments(row, "March 2026")
    assert "17%" in text


def test_kpi_expenses_lists_categories(gen):
    rows = [{"category_name": "Supplies", "total_amount": 800.0, "expense_count": 5,
             "location_id": 0}]
    total = {"total": 800.0, "count": 5}
    text = gen._kpi_expenses(rows, total, "March 2026")
    assert "Supplies" in text


def test_kpi_reviews_shows_overall_rating(gen):
    row = {"overall_avg_rating": 4.6, "total_review_count": 64,
           "emp_review_count": 32, "emp_avg_rating": 4.7,
           "visit_review_count": 18, "visit_avg_rating": 4.5,
           "google_review_count": 14, "google_avg_rating": 4.6, "google_bad_review_count": 0}
    text = gen._kpi_reviews(row, "March 2026")
    assert "4.6" in text


# --- _make_chunk_text ----------------------------------------------------------


@pytest.mark.asyncio
async def test_make_chunk_text_calls_gateway(gen):
    data = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="KPI",
    )
    await gen._make_chunk_text(42, data)
    gen._gateway.call_with_data.assert_awaited()
    call_kw = gen._gateway.call_with_data.call_args
    assert call_kw[0][0] == UseCase.DOC_GENERATION


@pytest.mark.asyncio
async def test_make_chunk_text_combines_kpi_and_observation(gen):
    data = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="LINE_A",
    )
    out = await gen._make_chunk_text(42, data)
    assert "LINE_A" in out
    assert "strong performance" in out


@pytest.mark.asyncio
async def test_make_chunk_text_returns_kpi_only_on_llm_failure(gen):
    gen._gateway.call_with_data = AsyncMock(side_effect=RuntimeError("fail"))
    data = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="ONLY_KPI",
    )
    out = await gen._make_chunk_text(42, data)
    assert out == "ONLY_KPI"


@pytest.mark.asyncio
async def test_make_chunk_text_observation_label_present(gen):
    data = DocGenData(
        business_id="42", business_type="Salon", period="Jan 2026",
        doc_domain="revenue", doc_type="monthly_summary", kpi_block="KPI",
    )
    out = await gen._make_chunk_text(42, data)
    assert "Observation:" in out


# --- _store_doc ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_doc_calls_embed(gen, mock_vs, mock_emb):
    mock_vs.get_doc_ids = AsyncMock(return_value=[])
    status = await gen._store_doc(
        "42", "42_x", "revenue", "monthly_summary", "chunk text", PERIOD, {},
    )
    assert status == "created"
    mock_emb.embed.assert_awaited_once_with("chunk text")


@pytest.mark.asyncio
async def test_store_doc_calls_vector_store_upsert(gen, mock_vs):
    mock_vs.get_doc_ids = AsyncMock(return_value=[])
    await gen._store_doc(
        "42", "doc1", "revenue", "monthly_summary", "hello", PERIOD, {"a": 1},
    )
    mock_vs.upsert.assert_awaited()
    ca = mock_vs.upsert.call_args[1]
    assert ca["tenant_id"] == "42"
    assert ca["doc_id"] == "doc1"
    assert ca["doc_domain"] == "revenue"
    assert ca["doc_type"] == "monthly_summary"


@pytest.mark.asyncio
async def test_store_doc_returns_created_on_new_doc(gen, mock_vs):
    mock_vs.get_doc_ids = AsyncMock(return_value=[])
    st = await gen._store_doc("42", "d1", "revenue", "monthly_summary", "t", PERIOD, {})
    assert st == "created"


@pytest.mark.asyncio
async def test_store_doc_returns_skipped_on_unchanged_hash(gen, mock_vs):
    import hashlib
    text = "same"
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    mock_vs.get_doc_ids = AsyncMock(return_value=["d1"])
    mock_vs.get_doc_metadata = AsyncMock(return_value={"content_hash": h})
    st = await gen._store_doc("42", "d1", "revenue", "monthly_summary", text, PERIOD, {})
    assert st == "skipped"
    mock_vs.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_store_doc_returns_created_when_hash_changed(gen, mock_vs):
    mock_vs.get_doc_ids = AsyncMock(return_value=["d1"])
    mock_vs.get_doc_metadata = AsyncMock(return_value={"content_hash": "old"})
    st = await gen._store_doc("42", "d1", "revenue", "monthly_summary", "new text", PERIOD, {})
    assert st == "created"
    mock_vs.upsert.assert_awaited()


@pytest.mark.asyncio
async def test_store_doc_force_skips_hash_check(gen, mock_vs):
    import hashlib
    text = "x"
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    mock_vs.get_doc_ids = AsyncMock(return_value=["d1"])
    mock_vs.get_doc_metadata = AsyncMock(return_value={"content_hash": h})
    await gen._store_doc("42", "d1", "revenue", "monthly_summary", text, PERIOD, {}, force=True)
    mock_vs.upsert.assert_awaited()


# --- generate_all --------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_all_returns_summary_dict(mock_wh, mock_gateway, mock_emb, mock_vs):
    with ExitStack() as stack:
        for _dom, meth in _DOMAIN_HANDLERS.items():
            stack.enter_context(
                patch.object(DocGenerator, meth, AsyncMock(return_value=(0, 0, 0)))
            )
        g = DocGenerator(mock_wh, mock_gateway, mock_emb, mock_vs)
        result = await g.generate_all(ORG_ID, period_start=PERIOD, months=1)
    assert result["org_id"] == ORG_ID
    assert "docs_created" in result
    assert "docs_skipped" in result
    assert "docs_failed" in result
    assert "errors" in result


@pytest.mark.asyncio
async def test_generate_all_calls_all_domain_handlers(mock_wh, mock_gateway, mock_emb, mock_vs):
    mocks = {}
    with ExitStack() as stack:
        for _dom, meth in _DOMAIN_HANDLERS.items():
            m = AsyncMock(return_value=(0, 0, 0))
            mocks[meth] = m
            stack.enter_context(patch.object(DocGenerator, meth, m))
        g = DocGenerator(mock_wh, mock_gateway, mock_emb, mock_vs)
        await g.generate_all(ORG_ID, period_start=PERIOD, months=1)
    for m in mocks.values():
        assert m.await_count >= 1


@pytest.mark.asyncio
async def test_generate_all_calls_single_domain_when_specified(
    mock_wh, mock_gateway, mock_emb, mock_vs
):
    with patch.object(DocGenerator, "_gen_revenue", AsyncMock(return_value=(0, 0, 0))) as m_rev:
        with patch.object(DocGenerator, "_gen_staff", AsyncMock(return_value=(0, 0, 0))) as m_st:
            g = DocGenerator(mock_wh, mock_gateway, mock_emb, mock_vs)
            await g.generate_all(ORG_ID, period_start=PERIOD, months=1, domain="revenue")
    m_rev.assert_awaited()
    m_st.assert_not_awaited()


@pytest.mark.asyncio
async def test_generate_all_counts_created_docs(mock_wh, mock_gateway, mock_emb, mock_vs):
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(DocGenerator, "_gen_revenue", AsyncMock(return_value=(2, 0, 0)))
        )
        for _dom, meth in _DOMAIN_HANDLERS.items():
            if meth == "_gen_revenue":
                continue
            stack.enter_context(
                patch.object(DocGenerator, meth, AsyncMock(return_value=(0, 0, 0)))
            )
        g = DocGenerator(mock_wh, mock_gateway, mock_emb, mock_vs)
        result = await g.generate_all(ORG_ID, period_start=PERIOD, months=1)
    assert result["docs_created"] == 2


@pytest.mark.asyncio
async def test_generate_all_continues_after_domain_failure(
    mock_wh, mock_gateway, mock_emb, mock_vs
):
    async def boom(*_a, **_k):
        raise RuntimeError("fail")

    with ExitStack() as stack:
        stack.enter_context(patch.object(DocGenerator, "_gen_revenue", side_effect=boom))
        for _dom, meth in _DOMAIN_HANDLERS.items():
            if meth == "_gen_revenue":
                continue
            stack.enter_context(
                patch.object(DocGenerator, meth, AsyncMock(return_value=(1, 0, 0)))
            )
        g = DocGenerator(mock_wh, mock_gateway, mock_emb, mock_vs)
        result = await g.generate_all(ORG_ID, period_start=PERIOD, months=1)
    assert len(result["errors"]) >= 1
    assert result["docs_created"] == 10
