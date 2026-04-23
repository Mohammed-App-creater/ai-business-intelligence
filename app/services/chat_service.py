"""
Chat Service
============
Core business logic for POST /api/v1/chat.

Orchestrates the full request lifecycle:
  1. Analyse the question (QueryAnalyzer)
  2. Route to DIRECT or RAG
  3. For RAG: retrieve context, build prompt, call LLM
  4. For DIRECT: simple prompt, call LLM
  5. Detect live-data intent → graceful redirect
  6. Return ChatResponse

This is a pure service class — no FastAPI dependencies.
Tested by passing mock dependencies to the constructor.

Usage::

    service = ChatService(analyzer, retriever, gateway)
    response = await service.handle(chat_request)
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from app.api.v1.schemas import ChatResponse
from app.prompts.types import RagChatData
from app.services.llm.types import UseCase
from app.services.query_analyzer import Route
from app.services.retriever import RetrievalContext
from app.services.time_parser import parse_since_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Live-data keyword detection (stopgap — moves to QueryAnalyzer later)
# ---------------------------------------------------------------------------

_LIVE_DATA_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\btoday\b", re.I),
    re.compile(r"\bcurrently\b", re.I),
    re.compile(r"\bthis morning\b", re.I),
    re.compile(r"\btonight\b", re.I),
    re.compile(r"\bupcoming\b", re.I),
    re.compile(r"\bat the moment\b", re.I),
    re.compile(r"\bjust now\b", re.I),
    re.compile(r"\bright now\b", re.I),
    re.compile(r"\bas of now\b", re.I),
    re.compile(r"\bthis month\b", re.I),   # NEW — in-progress month is live data
    re.compile(r"\bthis week\b", re.I),
]

LIVE_DATA_REDIRECT = (
    "Live data isn't available yet — I can only analyse historical trends "
    "from your business data. You can check today's information in your "
    "dashboard.\n\n"
    "Would you like me to analyse your recent trends instead?"
)


# ---------------------------------------------------------------------------
# PII name-lookup detection — refuse before RAG retrieval
# ---------------------------------------------------------------------------

_PII_NAME_LOOKUP_PATTERNS: list[re.Pattern[str]] = [
    # "Tell me about Jane Smith" / "Show me Jane Smith" / "Look up Jane Smith"
    re.compile(
        r"\b(?i:tell me about|show me|look up|look at|find|pull up|get me|give me)\s+"
        r"[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}"
        r"(?:[.?!]|\s*$)"
    ),

    # "Jane Smith's visits/spend/history/profile/info/balance/points/submissions"
    re.compile(
        r"\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}'s\s+"
        r"(?i:spend|spending|visits?|history|profile|contact|info|details|"
        r"balance|points|total|revenue|appointments?|bookings?|"
        r"submissions?|entries|entry|records?|logs?|expenses?|transactions?|"
        r"activity|habits|patterns?|behaviou?r|performance)\b"
    ),

    # "What did Jane Smith spend/buy/visit/book"
    re.compile(
        r"\b(?i:what)\s+(?i:did|does|has)\s+[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\s+"
        r"(?i:spend|spent|buy|book|visit|do|like|prefer)\b"
    ),

    # "How much/often has Jane Smith ..." / "How many times did Jane Smith ..."
    re.compile(
        r"\b(?i:how)\s+(?i:much|often|many\s+times)\s+(?i:has|did|does)\s+"
        r"[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\b"
    ),

    # "The client Jane Smith" / "Customer Jane Smith"
    re.compile(
        r"\b(?i:the\s+)?(?i:client|customer|patron|guest)\s+"
        r"[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\b"
    ),

    # "Jane Smith as a client" / "Jane Smith as a customer"
    re.compile(
        r"\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\s+(?i:as\s+a)\s+"
        r"(?i:client|customer|patron|guest)\b"
    ),
]

# Staff-context keywords rescue a name-lookup from PII refusal — let RAG route
# "Tell me about Maria Lopez my stylist" normally instead of blocking it.
_STAFF_CONTEXT_KEYWORDS: set[str] = {
    "staff", "employee", "employees", "stylist", "stylists",
    "therapist", "therapists", "technician", "technicians",
    "worker", "workers", "performer", "performers",
    "team member", "team members", "the team",
    "my stylist", "my therapist", "my staff",
    "emp ", "emp_", "empid",
}

PII_REFUSAL_REDIRECT = (
    "I don't look up individual people by name — whether a customer or "
    "a staff member — to protect privacy.\n\n"
    "I can help you with aggregate questions instead, like:\n"
    "• Top 10 clients by lifetime spend\n"
    "• Most frequent customers this month\n"
    "• At-risk clients we can still reach\n"
    "• Best-performing staff by revenue\n\n"
    "What would you like to know?"
)


# ---------------------------------------------------------------------------
# DIRECT route prompt
# ---------------------------------------------------------------------------

DIRECT_SYSTEM_PROMPT = """\
You are an expert business analytics assistant for beauty and wellness \
businesses (salons, spas, barbershops, nail studios).

You provide clear, practical advice based on industry knowledge and \
best practices. Be concise, specific, and actionable.

If the question is outside your area of expertise, say so honestly."""


# ---------------------------------------------------------------------------
# Chat Service
# ---------------------------------------------------------------------------

class ChatService:
    """
    Orchestrates the chat request lifecycle.

    Parameters
    ----------
    analyzer:
        ``QueryAnalyzer`` instance — classifies the question route.
    retriever:
        ``Retriever`` instance — fetches RAG context from vector store.
    gateway:
        ``LLMGateway`` instance — calls the LLM.
    business_type:
        Default business type label for prompts.
    """

    def __init__(
        self,
        analyzer: Any,
        retriever: Any,
        gateway: Any,
        business_type: str = "Beauty & Wellness Business",
    ) -> None:
        self._analyzer = analyzer
        self._retriever = retriever
        self._gateway = gateway
        self._business_type = business_type

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def handle(self, request: Any) -> ChatResponse:
        """
        Process a chat request end-to-end.

        Parameters
        ----------
        request:
            ``ChatRequest`` with question, business_id, org_id,
            and optional conversation_id.

        Returns
        -------
        ``ChatResponse`` — always returns, never raises.
        """
        t0 = time.perf_counter()

        try:
            # 1. Analyse the question
            analysis = await self._analyzer.analyze(
                request.question, request.business_id,
            )

            # 2. Check for live-data intent
            if self._has_live_intent(request.question):
                return self._live_data_response(analysis, t0, request)

            # 2b. Check for personal-name lookup (PII refusal)
            if self._has_pii_name_lookup(request.question):
                logger.info(
                    "chat_service.pii_refusal business_id=%s question=%r",
                    request.business_id, request.question,
                )
                return self._pii_refusal_response(analysis, t0, request)

            # 3. Route
            if analysis.route == Route.DIRECT:
                return await self._handle_direct(
                    request, analysis, t0,
                )

            # Default: RAG
            return await self._handle_rag(
                request, analysis, t0,
            )

        except Exception as exc:
            latency = (time.perf_counter() - t0) * 1000
            logger.error(
                "chat_service.handle failed business_id=%s error=%r "
                "latency_ms=%.1f",
                request.business_id, exc, latency,
            )
            return ChatResponse(
                answer="I'm sorry, something went wrong while processing "
                       "your question. Please try again.",
                route="ERROR",
                confidence=0.0,
                sources=[],
                conversation_id=request.conversation_id,
                latency_ms=latency,
            )

    # ------------------------------------------------------------------
    # DIRECT route
    # ------------------------------------------------------------------

    async def _handle_direct(
        self, request: Any, analysis: Any, t0: float,
    ) -> ChatResponse:
        """Handle a DIRECT-routed question (general advice, no data)."""
        response = await self._gateway.call(
            UseCase.RAG_CHAT,
            DIRECT_SYSTEM_PROMPT,
            request.question,
            request.business_id,
        )

        latency = (time.perf_counter() - t0) * 1000

        logger.info(
            "chat_service.direct business_id=%s confidence=%.2f "
            "latency_ms=%.1f",
            request.business_id, analysis.confidence, latency,
        )

        return ChatResponse(
            answer=response.content,
            route="DIRECT",
            confidence=analysis.confidence,
            sources=[],
            conversation_id=request.conversation_id,
            latency_ms=latency,
        )

    # ------------------------------------------------------------------
    # RAG route
    # ------------------------------------------------------------------

    async def _handle_rag(
        self, request: Any, analysis: Any, t0: float,
    ) -> ChatResponse:
        """Handle a RAG-routed question (needs business data)."""

        # 1. Parse time reference from question for pre-filter
        since_date = parse_since_date(request.question)

        # 2. Retrieve context from vector store
        ctx: RetrievalContext = await self._retriever.retrieve(
            question=request.question,
            tenant_id=request.business_id,
            analysis=analysis,
            since_date=since_date,
        )

        # 3. Build RagChatData with retrieved documents.
        #    analysis_period gives the LLM concrete period anchoring —
        #    without it, "last month" / "this quarter" / "this year"
        #    lose their time reference and the LLM picks wrong chunks.
        analysis_period = self._format_analysis_period(
            since_date, request.question,
        )
        rag_data = RagChatData(
            business_id=request.business_id,
            business_type=self._business_type,
            analysis_period=analysis_period,
            question=request.question,
            documents=ctx.documents,
        )

        # 4. Call LLM with structured data
        response = await self._gateway.call_with_data(
            UseCase.RAG_CHAT,
            rag_data,
            request.business_id,
        )

        latency = (time.perf_counter() - t0) * 1000

        # 5. Extract answer from LLM response
        answer = self._extract_answer(response)

        logger.info(
            "chat_service.rag business_id=%s confidence=%.2f "
            "since_date=%s docs=%d sources=%s latency_ms=%.1f",
            request.business_id, analysis.confidence, since_date,
            ctx.total_results, ctx.doc_ids, latency,
        )

        return ChatResponse(
            answer=answer,
            route="RAG",
            confidence=analysis.confidence,
            sources=ctx.doc_ids,
            conversation_id=request.conversation_id,
            latency_ms=latency,
        )

    # ------------------------------------------------------------------
    # Analysis period formatting — anchors the LLM in real time
    # ------------------------------------------------------------------

    @staticmethod
    def _format_analysis_period(since_date: Any, question: str) -> str:
        """
        Build the `analysis_period` string passed into the RAG prompt.

        This gives the LLM explicit awareness of:
          • Today's date (so "last month", "this quarter" mean something)
          • The parsed `since_date` window (what data was retrieved)
          • Key derived periods ("last month", "this month", etc.)

        Without this, the LLM receives ``analysis_period="Recent"`` and
        has no idea whether "last month" means Feb or March or July.
        """
        from datetime import date, timedelta

        today = date.today()

        # Compute "last month" — first day of prior month to last day of prior month
        first_of_this_month = today.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        first_of_prev_month = last_day_prev_month.replace(day=1)

        # Compute "this quarter" / "last quarter"
        q = (today.month - 1) // 3 + 1
        q_start_month = (q - 1) * 3 + 1
        this_quarter_start = date(today.year, q_start_month, 1)
        last_quarter_start = (
            date(today.year - 1, 10, 1) if q == 1
            else date(today.year, q_start_month - 3, 1)
        )
        last_quarter_end = this_quarter_start - timedelta(days=1)

        # "This year" window
        this_year_start = date(today.year, 1, 1)

        parts = [
            f"Today is {today.isoformat()} ({today.strftime('%A, %B %d, %Y')}).",
            f"\"This month\" = {today.strftime('%B %Y')} (partial, in progress).",
            f"\"Last month\" = {first_of_prev_month.strftime('%B %Y')} "
            f"({first_of_prev_month.isoformat()} to {last_day_prev_month.isoformat()}).",
            f"\"This quarter\" = Q{q} {today.year} "
            f"(started {this_quarter_start.isoformat()}).",
            f"\"Last quarter\" = Q{((q - 2) % 4) + 1} "
            f"{this_quarter_start.year - (1 if q == 1 else 0)} "
            f"({last_quarter_start.isoformat()} to {last_quarter_end.isoformat()}).",
            f"\"This year\" / YTD = {this_year_start.isoformat()} to {today.isoformat()}.",
        ]

        if since_date is not None:
            parts.append(
                f"Retrieved documents cover: {since_date.isoformat()} onward."
            )

        return " ".join(parts)

    # ------------------------------------------------------------------
    # Live-data detection (stopgap — moves to QueryAnalyzer later)
    # ------------------------------------------------------------------

    @staticmethod
    def _has_live_intent(question: str) -> bool:
        """
        Check if the question asks for live/real-time data.

        This is a stopgap — will move to QueryAnalyzer.AnalysisResult
        as ``has_live_intent`` in a future update.
        """
        return any(p.search(question) for p in _LIVE_DATA_PATTERNS)

    @staticmethod
    def _live_data_response(
        analysis: Any, t0: float, request: Any,
    ) -> ChatResponse:
        """Return a graceful redirect for live-data questions."""
        latency = (time.perf_counter() - t0) * 1000
        return ChatResponse(
            answer=LIVE_DATA_REDIRECT,
            route=analysis.route.value if hasattr(analysis.route, "value") else str(analysis.route),
            confidence=analysis.confidence,
            sources=[],
            conversation_id=request.conversation_id,
            latency_ms=latency,
        )

    # ------------------------------------------------------------------
    # PII name-lookup detection — refuse before RAG retrieval
    # ------------------------------------------------------------------

    @staticmethod
    def _has_pii_name_lookup(question: str) -> bool:
        """
        True if the question looks like a personal-name lookup
        (e.g. "Tell me about Jane Smith") that should be refused
        BEFORE reaching the RAG layer.

        Staff-context keywords rescue the query (so "Tell me about
        Maria Lopez my stylist" is NOT treated as PII).
        """
        if not any(p.search(question) for p in _PII_NAME_LOOKUP_PATTERNS):
            return False

        q_lower = question.lower()
        if any(kw in q_lower for kw in _STAFF_CONTEXT_KEYWORDS):
            return False

        return True

    @staticmethod
    def _pii_refusal_response(
        analysis: Any, t0: float, request: Any,
    ) -> ChatResponse:
        """Return a graceful refusal for personal-name lookups."""
        latency = (time.perf_counter() - t0) * 1000
        return ChatResponse(
            answer=PII_REFUSAL_REDIRECT,
            route="BLOCKED_PII",
            confidence=1.0,
            sources=[],
            conversation_id=request.conversation_id,
            latency_ms=latency,
        )

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_answer(response: Any) -> str:
        """
        Extract the answer string from an LLM response.

        The RAG prompt asks for structured JSON with a ``summary`` field.
        If parsing succeeds, use ``summary``. Otherwise, fall back to
        the raw content.
        """
        import json

        content = response.content if hasattr(response, "content") else str(response)

        # Try to parse structured JSON response
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                # Build a rich answer from the structured response
                parts = []

                summary = parsed.get("summary")
                if summary:
                    parts.append(summary)

                supporting = parsed.get("supporting_data")
                if supporting:
                    parts.append(f"\n\n{supporting}")

                root_causes = parsed.get("root_causes")
                if root_causes and isinstance(root_causes, list):
                    causes = ", ".join(str(c) for c in root_causes if c)
                    if causes:
                        parts.append(f"\n\nKey factors: {causes}")

                recommendations = parsed.get("recommendations")
                if recommendations and isinstance(recommendations, list):
                    recs = ", ".join(str(r) for r in recommendations if r)
                    if recs:
                        parts.append(f"\n\nRecommendations: {recs}")

                if parts:
                    return "".join(parts)

        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

        # Fallback: return raw content
        return content