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
 
import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import asyncio
 
logger = logging.getLogger(__name__)
 


# ---------------------------------------------------------------------------
# Route enum
# ---------------------------------------------------------------------------

class Route(str, Enum):
    DIRECT = "DIRECT"   # General knowledge / advice — no retrieval
    RAG    = "RAG"      # Business-data question — full RAG pipeline
    AGENT  = "AGENT"    # V2 post-MVP: multi-step tool use
    


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AnalysisResult:
    route:       Route
    confidence:  float                    # 0.0 – 1.0
    method:      str                      # "rules" | "classifier"
    matched_keywords: list[str] = field(default_factory=list)
    latency_ms:  float = 0.0
    reasoning:   Optional[str] = None     # classifier explanation (debug)
    

# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------
 
# Keywords that strongly indicate the question requires business data.
# Organized by semantic category for easy extension.

RAG_KEYWORD_GROUPS: dict[str, list[str]] = {
  "financial": [
        "revenue", "profit", "income", "earnings", "sales", "turnover",
        "margin", "cost", "expense", "invoice", "payment", "refund",
        "price", "pricing", "discount", "cashflow", "cash flow",
    ],
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


# Flatten to a single set for O(1) lookup
_RAG_KEYWORDS: frozenset[str] = frozenset(
    kw for group in RAG_KEYWORD_GROUPS.values() for kw in group
)

# Patterns that strongly indicate DIRECT (general advice, not tenant data)
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


# Possessive / first-person signals → almost certainly a tenant-data question
MY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bmy (revenue|profit|clients?|staff|appointments?|bookings?|services?|business|salon|spa|shop|team|cancellations?|performance|data|report|metrics?|kpi)\b", re.I),
    re.compile(r"\b(our|we|i have|i've|i'm)\b", re.I),
]


 # ---------------------------------------------------------------------------
# Classifier prompt (used only when rules are inconclusive)
# ---------------------------------------------------------------------------
 
CLASSIFIER_SYSTEM_PROMPT = """\
You are a routing classifier for an AI Business Intelligence assistant \
serving beauty and wellness businesses (salons, spas, barbershops).
 
Your ONLY job is to classify the user question into one of two routes:
  RAG    — the question requires analysis of the business's own data \
(revenue, appointments, staff, clients, services, trends, etc.)
  DIRECT — the question is general knowledge or advice that does NOT \
require any specific business data.
 
Respond ONLY with valid JSON — no preamble, no markdown:
{
  "route": "RAG" | "DIRECT",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one sentence>"
}
"""

# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------
 
class QueryAnalyzer:
    """
    Classifies an incoming question and returns the correct Route.
 
    Usage
    -----
        analyzer = QueryAnalyzer()                         # rules only
        analyzer = QueryAnalyzer(llm_client=my_client)     # + classifier fallback
 
    Parameters
    ----------
    llm_client:
        Optional async callable with signature:
            async (system: str, user: str) -> str
        Should return the raw text response from the LLM.
        If None, ambiguous questions fall back to RAG (safe default).
 
    confidence_threshold:
        Rule-based confidence must exceed this value to skip the classifier.
        Default 0.75.
    """
 
    def __init__(
        self,
        llm_client=None,
        confidence_threshold: float = 0.75,
    ) -> None:
        self._llm_client = llm_client
        self._confidence_threshold = confidence_threshold

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
 
    async def analyze(self, question: str, business_id: str = "") -> AnalysisResult:
        """
        Analyze a question and return the routing decision.
 
        Parameters
        ----------
        question:    The raw user question string.
        business_id: Tenant identifier — used for logging/audit only.
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
 
        elapsed_ms = (time.perf_counter() - t0) * 1000
        rule_result.latency_ms = elapsed_ms
 
        logger.debug(
            "query_analyzer.rules business_id=%s route=%s confidence=%.2f "
            "keywords=%s latency_ms=%.1f",
            business_id,
            rule_result.route,
            rule_result.confidence,
            rule_result.matched_keywords,
            elapsed_ms,
        )
        
        if rule_result.confidence >= self._confidence_threshold:
            return rule_result
          
        # Step 2 — Classifier fallback
        if self._llm_client is None:
            # No classifier configured → default to RAG (safe, never loses data)
            rule_result.route = Route.RAG
            rule_result.method = "rules_fallback"
            rule_result.reasoning = (
                "Rules inconclusive; no classifier configured — defaulting to RAG."
            )
            return rule_result
 
        classifier_result = await self._classifier_check(question)
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
 
        # 1a. Strong DIRECT signals — general advice patterns
        for pattern in DIRECT_PATTERNS:
            if pattern.search(question):
                # Still check for possessive override ("how can MY salon…")
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
        matched = [kw for kw in _RAG_KEYWORDS if re.search(r"\b" + re.escape(kw) + r"\b", q_lower)]
 
        if len(matched) >= 2:
            confidence = min(0.95, 0.75 + len(matched) * 0.04)
            return AnalysisResult(
                route=Route.RAG,
                confidence=confidence,
                method="rules",
                matched_keywords=matched,
            )
 
        if len(matched) == 1:
            # Single keyword — moderate confidence, send to classifier if available
            return AnalysisResult(
                route=Route.RAG,
                confidence=0.60,
                method="rules",
                matched_keywords=matched,
                reasoning="Single domain keyword matched — low confidence.",
            )
 
        # 1d. No signals → assume DIRECT but low confidence
        return AnalysisResult(
            route=Route.DIRECT,
            confidence=0.55,
            method="rules",
            reasoning="No domain keywords or patterns matched",
        )
        
    # ------------------------------------------------------------------
    # Step 2 — LLM classifier fallback
    # ------------------------------------------------------------------
 
    async def _classifier_check(self, question: str) -> AnalysisResult:
        import json
 
        try:
            raw = await self._llm_client(
                system=CLASSIFIER_SYSTEM_PROMPT,
                user=question,
            )
            payload = json.loads(raw)
            route_str = str(payload.get("route", "RAG")).upper()
            route = Route.RAG if route_str == "RAG" else Route.DIRECT
            confidence = float(payload.get("confidence", 0.80))
            reasoning = payload.get("reasoning", "")
 
            return AnalysisResult(
                route=route,
                confidence=confidence,
                method="classifier",
                reasoning=reasoning,
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
 
def get_query_analyzer(llm_client=None) -> QueryAnalyzer:
    """
    FastAPI dependency.  Wire into app startup or use Depends().
 
    Example
    -------
        from app.services.query_analyzer import get_query_analyzer, QueryAnalyzer
        from fastapi import Depends
 
        async def chat(
            payload: ChatRequest,
            analyzer: QueryAnalyzer = Depends(lambda: get_query_analyzer(llm_client)),
        ):
            result = await analyzer.analyze(payload.question, payload.business_id)
            if result.route == Route.RAG:
                ...
    """
    return QueryAnalyzer(llm_client=llm_client)
 
 # ---------------------------------------------------------------------------
# Quick smoke-test  (python query_analyzer.py)
# ---------------------------------------------------------------------------
 
if __name__ == "__main__":
    import asyncio
 
    TEST_CASES: list[tuple[str, Route]] = [
        # Expected RAG
        ("Why did my revenue decrease this month?",          Route.RAG),
        ("Which staff member generates the most repeat clients?", Route.RAG),
        ("What services should I add to increase profitability?", Route.RAG),
        ("How does my cancellation rate compare to last quarter?", Route.RAG),
        ("Show me a summary of last month's appointments.",   Route.RAG),
        ("Who are my top 5 clients by spend?",               Route.RAG),
        # Expected DIRECT
        ("How can salons improve customer retention?",        Route.DIRECT),
        ("What are the best practices for upselling?",        Route.DIRECT),
        ("Explain what a cancellation rate means.",           Route.DIRECT),
        ("What is the industry average for no-shows?",        Route.DIRECT),
        ("Give me tips on staff scheduling.",                 Route.DIRECT),
    ]
 
    analyzer = QueryAnalyzer()   # rules only for smoke test
 
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