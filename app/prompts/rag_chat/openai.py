"""
prompts/rag_chat/openai.py
===========================
RAG chat prompt optimised for OpenAI GPT.

OpenAI-specific choices:
  - Plain markdown-style sections instead of XML tags
  - No JSON schema instruction — response_format=json_object handles it
  - Concise system prompt — GPT performs well with direct instructions
  - Headers (##) used instead of XML tags for data sections
  - Same output schema as Anthropic for consistency downstream

Context sources (rendered in priority order):
  1. Retrieved documents (MVP) — chunk_texts from vector store
  2. Typed entries (V2)       — structured data from warehouse
"""
from __future__ import annotations

from ..types import RagChatData

SYSTEM_PROMPT = """\
You are an expert business analytics assistant for beauty and wellness businesses \
(salons, spas, barbershops, nail studios).

Instructions:
- Analyse ONLY the data provided in the prompt. Do not invent figures or trends.
- If the data is insufficient, say so — do not guess.
- Respond with a JSON object using this schema:
  {
    "summary": "One-sentence direct answer",
    "root_causes": ["cause 1", "cause 2"],
    "supporting_data": "Key figures supporting the analysis",
    "recommendations": ["action 1", "action 2", "action 3"],
    "confidence": "high | medium | low",
    "data_gaps": "Missing data that would help, or null"
  }"""


def build(data: RagChatData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call(UseCase.RAG_CHAT, ...).
    response_format=json_object applied by provider automatically.
    """
    sections = []

    sections.append(
        f"## Business Context\n"
        f"- Business ID: {data.business_id}\n"
        f"- Type: {data.business_type}\n"
        f"- Period: {data.analysis_period}"
    )

    # MVP path — retrieved documents from vector store
    if data.documents:
        sections.append(_render_documents(data.documents))

    # V2 path — structured entries from warehouse
    if data.revenue:
        sections.append(_render_revenue(data.revenue))

    if data.appointments:
        sections.append(_render_appointments(data.appointments))

    if data.staff:
        sections.append(_render_staff(data.staff))

    if data.services:
        sections.append(_render_services(data.services))

    if data.campaigns:
        sections.append(_render_campaigns(data.campaigns))

    sections.append(f"## Question\n{data.question.strip()}")

    user = "\n\n".join(sections)
    return SYSTEM_PROMPT, user


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def _render_documents(docs: list[str]) -> str:
    """Render retrieved vector store documents with markdown headers."""
    numbered = "\n\n".join(
        f"### Document {i + 1}\n{doc.strip()}"
        for i, doc in enumerate(docs)
    )
    return f"## Retrieved Context\n{numbered}"


def _render_revenue(entries: list) -> str:
    lines = ["## Revenue"]
    for e in entries:
        sym = "$" if e.currency == "USD" else f"{e.currency} "
        if e.change_pct is not None:
            arrow = "▲" if e.change_pct >= 0 else "▼"
            lines.append(
                f"- {e.period}: {sym}{e.amount:,.0f} "
                f"({arrow} {abs(e.change_pct):.0f}% vs previous period)"
            )
        else:
            lines.append(f"- {e.period}: {sym}{e.amount:,.0f}")
    return "\n".join(lines)


def _render_appointments(entries: list) -> str:
    lines = ["## Appointments"]
    for e in entries:
        lines.append(
            f"- {e.period}: {e.total} total, "
            f"{e.cancellation_rate_pct:.0f}% cancellation rate"
        )
    return "\n".join(lines)


def _render_staff(entries: list) -> str:
    lines = ["## Staff Performance"]
    for m in entries:
        note = f" {m.note}" if m.note else ""
        lines.append(
            f"- {m.name}{note}: {m.appointments} appts, "
            f"${m.revenue:,.0f} revenue, {m.rating:.1f} rating"
        )
    return "\n".join(lines)


def _render_services(entries: list) -> str:
    lines = ["## Service Popularity"]
    for s in entries:
        lines.append(f"- {s.name}: {s.bookings} bookings, ${s.revenue:,.0f} revenue")
    return "\n".join(lines)


def _render_campaigns(entries: list) -> str:
    lines = ["## Marketing Campaigns"]
    for c in entries:
        roi = ((c.revenue_attributed - c.spend) / c.spend * 100) if c.spend else 0
        lines.append(
            f"- {c.name}: spend ${c.spend:,.0f}, "
            f"{c.conversions} conversions, "
            f"${c.revenue_attributed:,.0f} revenue, ROI {roi:.0f}%"
        )
    return "\n".join(lines)