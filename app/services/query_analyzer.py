"""
Query Analyzer / Router
=======================
Determines the correct processing path for each incoming question.

Routes:
  - DIRECT  → General knowledge / advice, no business data needed
  - RAG     → Requires analysis of the tenant's own business data
  - AGENT   → (V2, post-MVP) Multi-step reasoning with tool use

Architecture (per spec):
  Step 1 — Rule-based check      (~1 ms,   no I/O)
  Step 2 — Classifier fallback   (~50–100 ms, LLM call)

The classifier is only invoked when rules are inconclusive, keeping
the happy-path latency as low as possible.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route enum
# ---------------------------------------------------------------------------

class Route(str, Enum):
    DIRECT = "DIRECT"
    RAG    = "RAG"
    AGENT  = "AGENT"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AnalysisResult:
    route:            Route
    confidence:       float
    method:           str
    matched_keywords: list[str]      = field(default_factory=list)
    latency_ms:       float          = 0.0
    reasoning:        Optional[str]  = None


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------

REVENUE_KEYWORD_GROUP: dict[str, list[str]] = {
    "financial": [
        # Existing terms — kept as-is
        "revenue", "profit", "income", "earnings", "sales", "turnover",
        "margin", "cost", "expense", "invoice", "payment", "refund",
        "price", "pricing", "discount", "cashflow", "cash flow",

        # Revenue-specific — covers all 20 Step 1 questions
        "total revenue", "service revenue", "gross revenue",
        "average ticket", "avg ticket", "ticket value", "ticket size",
        "per visit", "per appointment",

        # Trends & growth (Q5, Q16, Q17)
        "trending", "trend", "growing", "growth", "shrinking",
        "going up", "going down", "revenue trend",
        "business growing", "business shrinking",

        # Period comparisons (Q4, Q7)
        "month over month", "mom", "year over year", "yoy",
        "change in revenue", "compare", "comparison",

        # Payment types (Q10)
        "cash", "card", "credit card", "payment type", "payment method",
        "payment breakdown",

        # Gift cards (Q11, LQ10)
        "gift card", "gift cards", "gift card redemption", "redeemed",

        # Promos & discounts (Q12, LQ9)
        "promo", "promo code", "promotion", "coupon", "promo cost",
        "discount cost",

        # Tips (Q18)
        "tips", "tip", "gratuity", "staff tips",

        # Tax (Q19)
        "tax", "taxes", "tax collected",

        # Failed / refunded (Q20, Q15)
        "refunded", "failed payment", "canceled", "cancelled",
        "no-show", "no show", "missed appointment", "lost revenue",

        # Root cause (Q13, Q14)
        "revenue dropped", "revenue fell", "revenue decrease",
        "revenue increase", "less busy", "fewer visits",
        "why did revenue", "why was revenue",

        # Staff revenue (Q8)
        "who made the most", "top staff", "staff revenue",
        "which employee", "which staff", "best performer",

        # Location revenue (Q9, LQ1–LQ10)
        "which location", "by location", "location revenue",
        "top location", "best location", "worst location",
        "location breakdown", "location comparison",

        # Advice (Q16, Q17)
        "increase revenue", "improve revenue", "boost revenue",
        "should i be worried", "is my business",
    ]
}

RAG_KEYWORD_GROUPS: dict[str, list[str]] = {
    "financial": REVENUE_KEYWORD_GROUP["financial"],
    "appointments": [
        "appointment", "appointments", "booking", "bookings", "schedule",
        "cancellation", "cancellations", "cancellation rate", "no-show",
        "no show", "rescheduled", "rebook",
    ],
    "clients": [
        "client", "clients", "customer", "customers", "retention",
        "repeat client", "new client", "churn", "loyalty", "returning",
        "acquisition", "lost client", "inactive",
    ],
    "staff": [
        "staff", "employee", "employees", "team", "stylist", "therapist",
        "technician", "performance", "utilisation", "utilization",
        "rating", "reviews", "rating score",
    ],
    "services": [
        "service", "services", "treatment", "treatments", "menu",
        "popularity", "upsell", "add-on", "add on", "package",
    ],
    "time_comparisons": [
        "this month", "last month", "this week", "last week",
        "this quarter", "last quarter", "year to date", "ytd",
        "compared to", "vs last", "versus", "trend", "trends",
        "month on month", "week on week", "quarter on quarter",
        "year over year", "yoy", "period",
    ],
    "marketing": [
        "campaign", "campaigns", "marketing", "promotion", "promotions",
        "conversion", "impressions", "ad spend", "roi",
    ],
    "analytics": [
        "report", "reports", "analytics", "analysis", "breakdown",
        "summary", "overview", "dashboard", "metric", "metrics", "kpi",
        "forecast", "projection", "predict", "prediction",
        "decrease", "increase", "decline", "growth", "drop", "spike",
        "why did", "what caused", "what happened",
    ],
}

_RAG_KEYWORDS: frozenset[str] = frozenset(
    kw for group in RAG_KEYWORD_GROUPS.values() for kw in group
)

DIRECT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bhow (can|do|should|could) (salons?|spas?|barbershops?|businesses?)\b", re.I),
    re.compile(r"\bwhat (are|is) (the )?(best|good|common|typical|average)\b", re.I),
    re.compile(r"\btips? (for|on|to)\b", re.I),
    re.compile(r"\badvice (for|on|about)\b", re.I),
    re.compile(r"\bindustry (standard|average|benchmark|best practice)\b", re.I),
    re.compile(r"\bin general\b", re.I),
    re.compile(r"\bexplain (what|how|why)\b", re.I),
    re.compile(r"\bwhat does .+ mean\b", re.I),
    re.compile(r"\bdefine\b", re.I),
]

MY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bmy (revenue|profit|clients?|staff|appointments?|bookings?|services?|business|salon|spa|shop|team|cancellations?|performance|data|report|metrics?|kpi)\b", re.I),
    re.compile(r"\b(our|we|i have|i've|i'm)\b", re.I),
]

# NOTE: CLASSIFIER_SYSTEM_PROMPT removed — the prompt now lives in
# app/prompts/classifier/anthropic.py and app/prompts/classifier/openai.py.
# The gateway selects the correct one automatically via call_with_data().


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class QueryAnalyzer:
    """
    Classifies an incoming question and returns the correct Route.

    Usage
    -----
        analyzer = QueryAnalyzer()                        # rules only
        analyzer = QueryAnalyzer(gateway=my_gateway)      # + classifier fallback

    Parameters
    ----------
    gateway:
        Optional LLMGateway instance. When provided, ambiguous questions are
        sent to the classifier via gateway.call_with_data(UseCase.CLASSIFIER).
        If None, ambiguous questions fall back to RAG (safe default).

    confidence_threshold:
        Rule confidence must exceed this to skip the classifier. Default 0.75.
    """

    def __init__(
        self,
        gateway=None,
        confidence_threshold: float = 0.75,
    ) -> None:
        self._gateway = gateway
        self._confidence_threshold = confidence_threshold

    @property
    def confidence_threshold(self) -> float:
        """Minimum rule confidence before skipping the LLM classifier."""
        return self._confidence_threshold

    def preview_rule_routing(self, question: str) -> AnalysisResult:
        """
        Run only Step 1 (rule-based routing). Does not call the LLM classifier.

        Use this to debug why ``analyze()`` might choose RAG vs DIRECT before
        any gateway fallback or classifier call.
        """
        question = question.strip()
        if not question:
            return AnalysisResult(
                route=Route.DIRECT,
                confidence=1.0,
                method="rules",
                reasoning="Empty question.",
                latency_ms=0.0,
            )
        return self._rule_based_check(question)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def analyze(self, question: str, business_id: str = "") -> AnalysisResult:
        """
        Analyze a question and return the routing decision.

        Parameters
        ----------
        question:    The raw user question string.
        business_id: Tenant identifier — passed to the gateway for quota/logging.
        """
        t0 = time.perf_counter()
        question = question.strip()

        if not question:
            return AnalysisResult(
                route=Route.DIRECT,
                confidence=1.0,
                method="rules",
                reasoning="Empty question.",
                latency_ms=0.0,
            )

        # Step 1 — Rule-based check
        rule_result = self._rule_based_check(question)
        rule_result.latency_ms = (time.perf_counter() - t0) * 1000

        logger.debug(
            "query_analyzer.rules business_id=%s route=%s confidence=%.2f "
            "keywords=%s latency_ms=%.1f",
            business_id,
            rule_result.route,
            rule_result.confidence,
            rule_result.matched_keywords,
            rule_result.latency_ms,
        )

        if rule_result.confidence >= self._confidence_threshold:
            return rule_result

        # Step 2 — Classifier fallback
        if self._gateway is None:
            rule_result.route = Route.RAG
            rule_result.method = "rules_fallback"
            rule_result.reasoning = (
                "Rules inconclusive; no classifier configured — defaulting to RAG."
            )
            return rule_result

        classifier_result = await self._classifier_check(question, business_id)
        classifier_result.latency_ms = (time.perf_counter() - t0) * 1000

        logger.info(
            "query_analyzer.classifier business_id=%s route=%s confidence=%.2f "
            "latency_ms=%.1f reasoning=%r",
            business_id,
            classifier_result.route,
            classifier_result.confidence,
            classifier_result.latency_ms,
            classifier_result.reasoning,
        )

        return classifier_result

    # ------------------------------------------------------------------
    # Step 1 — Rule-based check
    # ------------------------------------------------------------------

    def _rule_based_check(self, question: str) -> AnalysisResult:
        q_lower = question.lower()

        # 1a. Strong DIRECT signals
        for pattern in DIRECT_PATTERNS:
            if pattern.search(question):
                if not any(p.search(question) for p in MY_PATTERNS):
                    return AnalysisResult(
                        route=Route.DIRECT,
                        confidence=0.90,
                        method="rules",
                        reasoning=f"Matched general-advice pattern: {pattern.pattern}",
                    )

        # 1b. Possessive / first-person → almost certainly RAG
        my_matches = [p.pattern for p in MY_PATTERNS if p.search(question)]
        if my_matches:
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.95,
                method="rules",
                matched_keywords=my_matches,
                reasoning="Possessive/first-person pronoun detected.",
            )

        # 1c. Domain keyword matching
        matched = [
            kw for kw in _RAG_KEYWORDS
            if re.search(r"\b" + re.escape(kw) + r"\b", q_lower)
        ]

        if len(matched) >= 2:
            confidence = min(0.95, 0.75 + len(matched) * 0.04)
            return AnalysisResult(
                route=Route.RAG,
                confidence=confidence,
                method="rules",
                matched_keywords=matched,
            )

        if len(matched) == 1:
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.60,
                method="rules",
                matched_keywords=matched,
                reasoning="Single domain keyword matched — low confidence.",
            )

        # 1d. No signals
        return AnalysisResult(
            route=Route.DIRECT,
            confidence=0.55,
            method="rules",
            reasoning="No domain keywords or patterns matched",
        )

    # ------------------------------------------------------------------
    # Step 2 — LLM classifier fallback
    # ------------------------------------------------------------------

    async def _classifier_check(
        self, question: str, business_id: str
    ) -> AnalysisResult:
        """
        Calls the classifier via the gateway's prompt layer.

        The gateway resolves the correct provider-specific prompt
        (XML tags for Anthropic, plain text for OpenAI) automatically.
        """
        from app.prompts.types import ClassifierData
        from app.services.llm.types import UseCase

        try:
            data = ClassifierData(question=question)
            response = await self._gateway.call_with_data(
                UseCase.CLASSIFIER,
                data,
                business_id or "unknown",
            )

            payload = json.loads(response.content)
            route_str = str(payload.get("route", "RAG")).upper()
            route = Route.RAG if route_str == "RAG" else Route.DIRECT

            return AnalysisResult(
                route=route,
                confidence=float(payload.get("confidence", 0.80)),
                method="classifier",
                reasoning=payload.get("reasoning", ""),
            )

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "query_analyzer.classifier_error error=%r — defaulting to RAG", exc
            )
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.70,
                method="classifier_error",
                reasoning=f"Classifier failed ({exc}); defaulting to RAG.",
            )


# ---------------------------------------------------------------------------
# Convenience sync wrapper (for scripts / tests)
# ---------------------------------------------------------------------------

def analyze_sync(question: str, **kwargs) -> AnalysisResult:
    """Blocking wrapper around QueryAnalyzer.analyze() for non-async contexts."""
    analyzer = QueryAnalyzer(**kwargs)
    return asyncio.run(analyzer.analyze(question))


# ---------------------------------------------------------------------------
# FastAPI dependency factory
# ---------------------------------------------------------------------------

def get_query_analyzer(gateway=None) -> QueryAnalyzer:
    """
    FastAPI dependency. Wire into app startup or use Depends().

    Example
    -------
        from app.services.query_analyzer import get_query_analyzer, QueryAnalyzer
        from app.services.llm.llm_gateway import LLMGateway
        from fastapi import Depends

        gateway = LLMGateway.from_env()

        async def chat(
            payload: ChatRequest,
            analyzer: QueryAnalyzer = Depends(lambda: get_query_analyzer(gateway)),
        ):
            result = await analyzer.analyze(payload.question, payload.business_id)
            if result.route == Route.RAG:
                ...
    """
    return QueryAnalyzer(gateway=gateway)


# ---------------------------------------------------------------------------
# Quick smoke-test  (python query_analyzer.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    TEST_CASES: list[tuple[str, Route]] = [
        ("Why did my revenue decrease this month?",               Route.RAG),
        ("Which staff member generates the most repeat clients?",  Route.RAG),
        ("What services should I add to increase profitability?",  Route.RAG),
        ("How does my cancellation rate compare to last quarter?", Route.RAG),
        ("Show me a summary of last month's appointments.",        Route.RAG),
        ("Who are my top 5 clients by spend?",                    Route.RAG),
        ("How can salons improve customer retention?",             Route.DIRECT),
        ("What are the best practices for upselling?",             Route.DIRECT),
        ("Explain what a cancellation rate means.",                Route.DIRECT),
        ("What is the industry average for no-shows?",             Route.DIRECT),
        ("Give me tips on staff scheduling.",                      Route.DIRECT),
    ]

    analyzer = QueryAnalyzer()  # rules only for smoke test

    async def run():
        passed = failed = 0
        for question, expected in TEST_CASES:
            result = await analyzer.analyze(question, business_id="test")
            status = "✓" if result.route == expected else "✗"
            if result.route != expected:
                failed += 1
            else:
                passed += 1
            print(
                f"{status} [{result.route.value:<6}] conf={result.confidence:.2f} "
                f"method={result.method:<18} | {question}"
            )
            if result.reasoning:
                print(f"    → {result.reasoning}")
        print(f"\n{passed}/{passed+failed} passed")

    asyncio.run(run())