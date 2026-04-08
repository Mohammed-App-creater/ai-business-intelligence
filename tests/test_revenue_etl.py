"""
tests/test_revenue_etl.py

Full ETL test suite for the Revenue domain.
Spins up the mock analytics server, runs the extractor,
and validates every document type, field, and computed value.

Run:
    pytest tests/test_revenue_etl.py -v
"""

import asyncio
import pytest
from datetime import date

from analytics_client import AnalyticsClient
from revenue_etl import RevenueExtractor
from mock_analytics_server import start_mock_server
from revenue_fixtures import (
    MONTHLY_SUMMARY,
    PAYMENT_TYPES,
    STAFF_REVENUE,
    LOCATION_REVENUE,
    PROMO_IMPACT,
    FAILED_REFUNDS,
)

BUSINESS_ID = 42
START_DATE  = date(2025, 1, 1)
END_DATE    = date(2025, 6, 30)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def mock_server():
    """Start the mock analytics server once for the whole session."""
    server = start_mock_server()
    yield server
    server.stop()


@pytest.fixture(scope="session")
def client(mock_server):
    return AnalyticsClient(base_url=mock_server.base_url)


@pytest.fixture(scope="session")
def docs(client):
    """Run the extractor once and reuse the output across all tests."""
    extractor = RevenueExtractor(client=client)
    return asyncio.get_event_loop().run_until_complete(
        extractor.run(BUSINESS_ID, START_DATE, END_DATE)
    )


def docs_of_type(docs, doc_type):
    return [d for d in docs if d["doc_type"] == doc_type]


# ── Basic shape tests ─────────────────────────────────────────────────────────

class TestDocShape:
    def test_produces_documents(self, docs):
        assert len(docs) > 0, "ETL produced no documents"

    def test_all_six_doc_types_present(self, docs):
        types = {d["doc_type"] for d in docs}
        expected = {
            "monthly_summary",
            "payment_type_breakdown",
            "staff_revenue",
            "location_revenue",
            "promo_impact",
            "failed_refunds",
        }
        assert expected == types

    def test_correct_number_of_monthly_docs(self, docs):
        # 6 periods in the fixture
        assert len(docs_of_type(docs, "monthly_summary")) == 6

    def test_correct_number_of_staff_docs(self, docs):
        # 4 staff members in the fixture
        assert len(docs_of_type(docs, "staff_revenue")) == 4

    def test_correct_number_of_location_docs(self, docs):
        # 2 locations × 6 periods = 12
        assert len(docs_of_type(docs, "location_revenue")) == 12


# ── Tenant isolation ──────────────────────────────────────────────────────────

class TestTenantIsolation:
    def test_all_docs_have_correct_tenant_id(self, docs):
        wrong = [d for d in docs if d.get("tenant_id") != BUSINESS_ID]
        assert wrong == [], f"{len(wrong)} docs with wrong tenant_id: {wrong}"

    def test_domain_field_is_revenue(self, docs):
        wrong = [d for d in docs if d.get("domain") != "revenue"]
        assert wrong == [], "Found docs with wrong domain"

    def test_rejected_unknown_business_id(self, client):
        """Mock server returns 403 for unknown business_id."""
        import httpx
        extractor = RevenueExtractor(client=AnalyticsClient(base_url=client.base_url))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            asyncio.get_event_loop().run_until_complete(
                extractor.run(business_id=9999, start_date=START_DATE, end_date=END_DATE)
            )
        assert exc_info.value.response.status_code == 403


# ── Monthly summary correctness ───────────────────────────────────────────────

class TestMonthlySummary:
    def test_service_revenue_matches_fixture(self, docs):
        monthly = {d["period"]: d for d in docs_of_type(docs, "monthly_summary")}
        fixture_periods = {r["period"]: r for r in MONTHLY_SUMMARY["data"]}
        for period, fixture_row in fixture_periods.items():
            assert period in monthly, f"Missing period {period}"
            assert monthly[period]["service_revenue"] == fixture_row["service_revenue"]

    def test_first_period_mom_growth_is_none(self, docs):
        monthly = sorted(docs_of_type(docs, "monthly_summary"), key=lambda d: d["period"])
        assert monthly[0]["mom_growth_pct"] is None, \
            "First period should have null mom_growth_pct"

    def test_subsequent_periods_have_mom_growth(self, docs):
        monthly = sorted(docs_of_type(docs, "monthly_summary"), key=lambda d: d["period"])
        for doc in monthly[1:]:
            assert doc["mom_growth_pct"] is not None, \
                f"Period {doc['period']} missing mom_growth_pct"

    def test_total_collected_gte_service_revenue(self, docs):
        for doc in docs_of_type(docs, "monthly_summary"):
            assert doc["total_collected"] >= doc["service_revenue"], \
                f"Period {doc['period']}: total_collected < service_revenue"

    def test_avg_ticket_positive(self, docs):
        for doc in docs_of_type(docs, "monthly_summary"):
            assert doc["avg_ticket"] > 0, f"Period {doc['period']}: avg_ticket is 0 or negative"

    def test_trend_slope_is_positive(self, docs):
        # Fixture data is growing — slope should be positive
        monthly = docs_of_type(docs, "monthly_summary")
        slopes = {d["trend_slope"] for d in monthly}
        assert len(slopes) == 1, "trend_slope should be same across all monthly docs"
        slope = list(slopes)[0]
        assert slope > 0, f"Expected positive trend slope, got {slope}"

    def test_trend_direction_matches_slope(self, docs):
        for doc in docs_of_type(docs, "monthly_summary"):
            slope = doc["trend_slope"]
            direction = doc["trend_direction"]
            if slope > 0:
                assert direction == "up"
            elif slope < 0:
                assert direction == "down"
            else:
                assert direction == "flat"

    def test_text_field_contains_period(self, docs):
        for doc in docs_of_type(docs, "monthly_summary"):
            assert doc["period"] in doc["text"], \
                f"Text for {doc['period']} doesn't mention the period"

    def test_text_field_contains_revenue_amount(self, docs):
        for doc in docs_of_type(docs, "monthly_summary"):
            # Revenue should appear formatted in the text
            assert "$" in doc["text"], f"Text for {doc['period']} missing revenue figure"


# ── Payment types ─────────────────────────────────────────────────────────────

class TestPaymentTypes:
    def test_one_payment_type_doc(self, docs):
        assert len(docs_of_type(docs, "payment_type_breakdown")) == 1

    def test_breakdown_has_correct_types(self, docs):
        doc = docs_of_type(docs, "payment_type_breakdown")[0]
        types = {r["payment_type"] for r in doc["breakdown"]}
        assert "Card" in types
        assert "Cash" in types

    def test_pct_sums_to_100(self, docs):
        doc = docs_of_type(docs, "payment_type_breakdown")[0]
        total = sum(r["pct_of_total"] for r in doc["breakdown"])
        assert abs(total - 100.0) < 1.0, f"pct_of_total sums to {total}, expected ~100"


# ── Staff revenue ─────────────────────────────────────────────────────────────

class TestStaffRevenue:
    def test_rank_1_has_highest_revenue(self, docs):
        staff = docs_of_type(docs, "staff_revenue")
        rank1 = next(d for d in staff if d["revenue_rank"] == 1)
        for other in staff:
            if other["revenue_rank"] != 1:
                assert rank1["service_revenue"] >= other["service_revenue"], \
                    "Rank 1 staff does not have highest revenue"

    def test_inactive_staff_included(self, docs):
        # Tom Rivera is inactive but should appear in historical revenue
        staff_names = [d["staff_name"] for d in docs_of_type(docs, "staff_revenue")]
        assert "Tom Rivera" in staff_names, \
            "Inactive staff 'Tom Rivera' missing from revenue history"

    def test_text_contains_staff_name(self, docs):
        for doc in docs_of_type(docs, "staff_revenue"):
            assert doc["staff_name"] in doc["text"]


# ── Location revenue ──────────────────────────────────────────────────────────

class TestLocationRevenue:
    def test_both_locations_present(self, docs):
        location_names = {d["location_name"] for d in docs_of_type(docs, "location_revenue")}
        assert "Main St" in location_names
        assert "Westside" in location_names

    def test_first_period_mom_null_per_location(self, docs):
        for loc_name in ("Main St", "Westside"):
            loc_docs = [
                d for d in docs_of_type(docs, "location_revenue")
                if d["location_name"] == loc_name
            ]
            first = min(loc_docs, key=lambda d: d["period"])
            assert first["mom_growth_pct"] is None, \
                f"First period for {loc_name} should have null mom_growth_pct"

    def test_pct_of_total_sums_near_100_per_period(self, docs):
        from collections import defaultdict
        period_totals = defaultdict(float)
        for doc in docs_of_type(docs, "location_revenue"):
            period_totals[doc["period"]] += doc.get("pct_of_total_revenue", 0)
        for period, total in period_totals.items():
            assert abs(total - 100.0) < 2.0, \
                f"Period {period}: location revenue pct sums to {total}"


# ── Promo impact ──────────────────────────────────────────────────────────────

class TestPromoImpact:
    def test_one_promo_doc(self, docs):
        assert len(docs_of_type(docs, "promo_impact")) == 1

    def test_total_discount_matches_sum(self, docs):
        doc = docs_of_type(docs, "promo_impact")[0]
        calculated = sum(r["total_discount_given"] for r in doc["breakdown"])
        assert abs(doc["total_discount_given"] - calculated) < 0.01, \
            "total_discount_given doesn't match sum of breakdown"

    def test_text_mentions_discount_amount(self, docs):
        doc = docs_of_type(docs, "promo_impact")[0]
        assert "$" in doc["text"]


# ── Failed / refunded visits ──────────────────────────────────────────────────

class TestFailedRefunds:
    def test_one_failed_refunds_doc(self, docs):
        assert len(docs_of_type(docs, "failed_refunds")) == 1

    def test_total_lost_matches_sum(self, docs):
        doc = docs_of_type(docs, "failed_refunds")[0]
        calculated = sum(r["lost_revenue"] for r in doc["breakdown"])
        assert abs(doc["total_lost_revenue"] - calculated) < 0.01

    def test_all_three_statuses_present(self, docs):
        doc = docs_of_type(docs, "failed_refunds")[0]
        labels = {r["status_label"] for r in doc["breakdown"]}
        assert "Failed" in labels
        assert "Refunded" in labels
        assert "Canceled" in labels

    def test_text_mentions_no_show_gap(self, docs):
        doc = docs_of_type(docs, "failed_refunds")[0]
        assert "no-show" in doc["text"].lower(), \
            "failed_refunds text should mention the no-show data gap"


# ── Embedding text quality ────────────────────────────────────────────────────

class TestEmbeddingText:
    def test_all_docs_have_non_empty_text(self, docs):
        empty = [d for d in docs if not d.get("text", "").strip()]
        assert empty == [], f"{len(empty)} docs have empty text fields"

    def test_no_text_is_just_whitespace(self, docs):
        for doc in docs:
            assert len(doc["text"].strip()) > 20, \
                f"doc_type={doc['doc_type']} text is too short: '{doc['text']}'"
