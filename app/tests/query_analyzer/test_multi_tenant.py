"""
test_multi_tenant.py
====================
Tests for multi-tenant isolation behaviour.

The Query Analyzer itself is stateless between tenants — it holds no
per-tenant state. These tests confirm that:
  - business_id has no effect on routing decisions
  - Concurrent analyses for different tenants do not interfere
  - business_id is correctly threaded through to the result (for audit logging)
"""
import asyncio
import pytest
from app.services.query_analyzer import QueryAnalyzer, Route

pytestmark = pytest.mark.asyncio


class TestTenantIsolation:

    async def test_same_question_same_route_different_tenants(self):
        analyzer = QueryAnalyzer()
        q = "Why did my revenue decrease this month?"
        r1 = await analyzer.analyze(q, business_id="salon_001")
        r2 = await analyzer.analyze(q, business_id="salon_002")
        assert r1.route == r2.route
        assert r1.confidence == r2.confidence

    async def test_business_id_does_not_bleed_between_calls(self):
        """Back-to-back calls for different tenants must produce independent results."""
        analyzer = QueryAnalyzer()
        results = []
        for tenant in ["salon_a", "spa_b", "barbershop_c"]:
            r = await analyzer.analyze("Show me my revenue report.", business_id=tenant)
            results.append(r)
        routes = [r.route for r in results]
        assert all(route == Route.RAG for route in routes)

    async def test_concurrent_tenant_requests(self):
        """Concurrent analyses for many tenants must all return valid results."""
        analyzer = QueryAnalyzer()
        questions = [
            ("Why did my revenue drop?",           Route.RAG),
            ("How can salons reduce no-shows?",    Route.DIRECT),
            ("Show me my staff performance.",      Route.RAG),
            ("Explain what churn rate means.",     Route.DIRECT),   # "explain what" → DIRECT
            ("My cancellations spiked last month.", Route.RAG),
        ]
        tasks = [
            analyzer.analyze(q, business_id=f"tenant_{i}")
            for i, (q, _) in enumerate(questions)
        ]
        results = await asyncio.gather(*tasks)
        for (question, expected_route), result in zip(questions, results):
            assert result.route == expected_route, (
                f"Tenant isolation failure: '{question}' → {result.route}, expected {expected_route}"
            )

    async def test_empty_business_id_still_routes(self):
        """business_id is optional metadata — empty string must not crash."""
        analyzer = QueryAnalyzer()
        result = await analyzer.analyze("Why did my revenue drop?", business_id="")
        assert result.route == Route.RAG

    async def test_special_characters_in_business_id(self):
        """business_id with special chars must not affect routing."""
        analyzer = QueryAnalyzer()
        result = await analyzer.analyze(
            "Why did my revenue drop?",
            business_id="salon/123&name=test"
        )
        assert result.route == Route.RAG
