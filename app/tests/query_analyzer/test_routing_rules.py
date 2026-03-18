"""
test_routing_rules.py
=====================
Tests for Step 1: rule-based routing.

Covers:
  - RAG routing via possessive/first-person patterns
  - RAG routing via domain keyword matching (single and multi-keyword)
  - DIRECT routing via general-advice patterns
  - Possessive override of DIRECT patterns ("how can MY salon...")
  - Edge cases: empty input, whitespace-only, punctuation, casing
"""
import pytest
from app.services.query_analyzer import QueryAnalyzer, Route, AnalysisResult

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def route(question: str, **kwargs) -> AnalysisResult:
    """Shorthand: analyze with a rules-only analyzer."""
    return await QueryAnalyzer(**kwargs).analyze(question, business_id="test_tenant")


# ---------------------------------------------------------------------------
# RAG — possessive / first-person signals
# ---------------------------------------------------------------------------

class TestPossessiveSignals:

    async def test_my_revenue(self):
        r = await route("Why did my revenue decrease this month?")
        assert r.route == Route.RAG
        assert r.confidence >= 0.90
        assert r.method == "rules"

    async def test_my_clients(self):
        # "my top clients by spend" — possessive matched but "clients" is a single
        # domain keyword; without a classifier the fallback confidence is 0.60.
        r = await route("Who are my top clients by spend?")
        assert r.route == Route.RAG
        assert r.confidence >= 0.50

    async def test_my_staff(self):
        r = await route("Show me my staff performance for last quarter.")
        assert r.route == Route.RAG

    async def test_my_cancellations(self):
        r = await route("How does my cancellation rate compare to last month?")
        assert r.route == Route.RAG

    async def test_my_bookings(self):
        r = await route("What do my bookings look like this week?")
        assert r.route == Route.RAG

    async def test_my_salon(self):
        r = await route("How is my salon performing overall?")
        assert r.route == Route.RAG

    async def test_our_team(self):
        r = await route("How is our team utilisation this month?")
        assert r.route == Route.RAG

    async def test_we_had(self):
        r = await route("We had a lot of cancellations last week — what happened?")
        assert r.route == Route.RAG

    async def test_my_kpi(self):
        r = await route("Give me my KPI breakdown for March.")
        assert r.route == Route.RAG

    async def test_my_report(self):
        r = await route("Pull up my report for last quarter.")
        assert r.route == Route.RAG


# ---------------------------------------------------------------------------
# RAG — multi-keyword domain matching
# ---------------------------------------------------------------------------

class TestDomainKeywords:

    async def test_revenue_and_appointments(self):
        r = await route("Revenue dropped and appointments were down this month.")
        assert r.route == Route.RAG
        assert r.confidence >= 0.75

    async def test_staff_and_performance(self):
        r = await route("Which staff member has the best performance and rating?")
        assert r.route == Route.RAG

    async def test_cancellation_trend(self):
        r = await route("Is there a trend in the cancellation rate?")
        assert r.route == Route.RAG

    async def test_client_retention_and_churn(self):
        r = await route("What is the client retention and churn situation?")
        assert r.route == Route.RAG

    async def test_marketing_roi(self):
        r = await route("What is the ROI on the last marketing campaign?")
        assert r.route == Route.RAG

    async def test_services_popularity(self):
        r = await route("Which services have the most popularity and bookings?")
        assert r.route == Route.RAG

    async def test_forecast_revenue(self):
        r = await route("Give me a forecast for revenue next quarter.")
        assert r.route == Route.RAG

    async def test_keyword_confidence_scales_with_count(self):
        # More keywords → higher confidence
        few = await route("revenue trend")
        many = await route("revenue profit cancellations appointments trend decline")
        assert many.confidence >= few.confidence

    async def test_keyword_case_insensitive(self):
        r = await route("REVENUE DROPPED AND APPOINTMENTS WERE DOWN")
        assert r.route == Route.RAG

    async def test_keyword_with_punctuation(self):
        r = await route("Revenue? Appointments? What's going on?")
        assert r.route == Route.RAG


# ---------------------------------------------------------------------------
# DIRECT — general-advice patterns
# ---------------------------------------------------------------------------

class TestDirectPatterns:

    async def test_how_can_salons(self):
        r = await route("How can salons improve customer retention?")
        assert r.route == Route.DIRECT
        assert r.confidence >= 0.85

    async def test_how_do_businesses(self):
        r = await route("How do businesses reduce no-shows?")
        assert r.route == Route.DIRECT

    async def test_what_are_best_practices(self):
        r = await route("What are the best practices for upselling services?")
        assert r.route == Route.DIRECT

    async def test_tips_for(self):
        r = await route("Give me tips for improving staff scheduling.")
        assert r.route == Route.DIRECT

    async def test_advice_on(self):
        r = await route("Any advice on managing client cancellations?")
        assert r.route == Route.DIRECT

    async def test_industry_average(self):
        r = await route("What is the industry average cancellation rate for salons?")
        assert r.route == Route.DIRECT

    async def test_industry_benchmark(self):
        r = await route("What is the industry benchmark for staff utilisation?")
        assert r.route == Route.DIRECT

    async def test_in_general(self):
        r = await route("In general, how should salons handle slow months?")
        assert r.route == Route.DIRECT

    async def test_explain_what(self):
        r = await route("Explain what a cancellation rate means.")
        assert r.route == Route.DIRECT

    async def test_what_does_mean(self):
        r = await route("What does churn rate mean?")
        assert r.route == Route.DIRECT

    async def test_define(self):
        r = await route("Define customer lifetime value.")
        assert r.route == Route.DIRECT


# ---------------------------------------------------------------------------
# Possessive override of DIRECT patterns
# ---------------------------------------------------------------------------

class TestPossessiveOverride:
    """
    When a question matches a DIRECT pattern but also contains a
    possessive signal ("my", "our"), it must be routed to RAG.
    """

    async def test_how_can_my_salon(self):
        r = await route("How can my salon improve customer retention?")
        assert r.route == Route.RAG

    async def test_what_are_my_best_services(self):
        r = await route("What are my best performing services this month?")
        assert r.route == Route.RAG

    async def test_our_cancellation_rate(self):
        r = await route("How should we handle our cancellation rate spike?")
        assert r.route == Route.RAG


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    async def test_empty_string(self):
        r = await route("")
        assert r.route == Route.DIRECT
        assert r.confidence == 1.0
        assert r.method == "rules"

    async def test_whitespace_only(self):
        r = await route("   \n\t  ")
        assert r.route == Route.DIRECT
        assert r.confidence == 1.0

    async def test_single_word_no_keyword(self):
        r = await route("Hello")
        assert r.route in (Route.DIRECT, Route.RAG)   # any valid route — must not crash

    async def test_very_long_question(self):
        q = "What is the revenue trend " + "for my salon " * 100 + "?"
        r = await route(q)
        assert r.route == Route.RAG
        assert r.latency_ms < 500   # rule engine must stay fast even on long strings

    async def test_result_has_latency(self):
        r = await route("Why did revenue drop last month?")
        assert r.latency_ms >= 0

    async def test_result_fields_populated(self):
        r = await route("Why did my revenue drop?")
        assert isinstance(r.route, Route)
        assert 0.0 <= r.confidence <= 1.0
        assert r.method in ("rules", "classifier", "rules_fallback", "classifier_error")

    async def test_business_id_does_not_affect_route(self):
        """business_id is metadata only — must not change the routing decision."""
        r1 = await QueryAnalyzer().analyze("Why did my revenue drop?", business_id="salon_a")
        r2 = await QueryAnalyzer().analyze("Why did my revenue drop?", business_id="salon_b")
        assert r1.route == r2.route
        assert r1.confidence == r2.confidence
