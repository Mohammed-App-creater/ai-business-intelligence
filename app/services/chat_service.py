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
    re.compile(r"\bright now\b", re.I),
    re.compile(r"\bcurrently\b", re.I),
    re.compile(r"\bthis morning\b", re.I),
    re.compile(r"\btonight\b", re.I),
    re.compile(r"\bupcoming\b", re.I),
    re.compile(r"\bright now\b", re.I),
    re.compile(r"\bat the moment\b", re.I),
    re.compile(r"\bjust now\b", re.I),
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

        # 3. Build RagChatData with retrieved documents
        rag_data = RagChatData(
            business_id=request.business_id,
            business_type=self._business_type,
            analysis_period="Recent",  # TODO: infer from question
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