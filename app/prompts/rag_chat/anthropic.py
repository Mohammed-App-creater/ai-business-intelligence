"""
prompts/rag_chat/anthropic.py
==============================
RAG chat prompt optimised for Anthropic Claude.

Anthropic-specific choices:
  - Full XML tag structure — clear boundaries reduce hallucinations
  - Explicit chain-of-thought instruction before final answer
  - Explicit JSON schema in <output_format> tag (no native response_format)
  - Guardrails block — tell the model exactly what NOT to do
  - Only non-empty data sections are rendered

Context sources (rendered in priority order):
  1. Retrieved documents (MVP) — chunk_texts from vector store
  2. Typed entries (V2)       — structured data from warehouse
"""
from __future__ import annotations

from ..types import RagChatData, RevenueEntry

def _build_system_prompt() -> str:
    """
    Build the system prompt with today's date injected.

    Today's date is computed at request time so the LLM always has correct
    temporal context. The <temporal_context> block lets the LLM map relative
    phrases ("last month", "this quarter") to absolute periods.
    """
    from datetime import date, timedelta

    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_day_prev_month.replace(day=1)
    q = (today.month - 1) // 3 + 1
    last_q = q - 1 if q > 1 else 4
    last_q_year = today.year if q > 1 else today.year - 1

    return f"""\
<role>
You are an expert business analytics assistant for beauty and wellness businesses
(salons, spas, barbershops, nail studios). Your role is to analyse business data
and provide clear, accurate, and actionable insights to business owners.
</role>

<temporal_context>
Today's date is {today.strftime('%A, %B %d, %Y')} ({today.isoformat()}).
- "This month"   = {today.strftime('%B %Y')} (in progress, partial data only)
- "Last month"   = {first_of_prev_month.strftime('%B %Y')}
- "This quarter" = Q{q} {today.year}
- "Last quarter" = Q{last_q} {last_q_year}
- "This year" / "YTD" = January 1, {today.year} through today
The "Analysis Period" field in <business_context> tells you which specific
period the user is asking about. Focus on that period when answering.
</temporal_context>

<rules>
- Base ALL analysis strictly on data inside <business_data> tags.
- Do NOT invent figures, trends, or comparisons not present in the data.
- The retrieved documents have been pre-filtered to match the Analysis Period
  in <business_context>. Trust them — if a document covers the period being
  asked about, use it. Do not refuse just because no document mentions today's
  date or another period.
- If, after consulting the retrieved documents, the data genuinely does not
  contain what was asked (e.g., the user asks about a metric that the
  documents simply do not report), say so explicitly — do not guess.
- Reason step-by-step before writing your final answer.
- Respond ONLY with the JSON object defined in <output_format>.
- Do not include any text outside the JSON object.
- Set any field to null if it cannot be determined from the data.
</rules>"""


# Backward-compatible alias — kept so any downstream code that imports
# SYSTEM_PROMPT as a constant doesn't break. Computed once at import time
# (today's date for module-import call); build() always calls _build_system_prompt()
# fresh so live requests always get the current date.
SYSTEM_PROMPT = _build_system_prompt()


def build(data: RagChatData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call(UseCase.RAG_CHAT, ...).

    System prompt is rebuilt per-request so today's-date context in
    <temporal_context> reflects the actual request time.
    """
    system_prompt = _build_system_prompt()
    sections = []

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

    data_block = (
        "<business_data>\n" + "\n".join(sections) + "\n</business_data>"
        if sections else ""
    )

    user = "\n\n".join(filter(None, [
        _business_context(data),
        data_block,
        f"<question>\n{data.question.strip()}\n</question>",
        _output_format(),
    ]))

    return system_prompt, user


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def _business_context(data: RagChatData) -> str:
    return (
        f"<business_context>\n"
        f"Business ID    : {data.business_id}\n"
        f"Business Type  : {data.business_type}\n"
        f"Analysis Period: {data.analysis_period}\n"
        f"</business_context>"
    )


def _render_documents(docs: list[str]) -> str:
    """Render retrieved vector store documents inside XML tags."""
    numbered = "\n\n".join(
        f"<document index=\"{i + 1}\">\n{doc.strip()}\n</document>"
        for i, doc in enumerate(docs)
    )
    return f"<retrieved_context>\n{numbered}\n</retrieved_context>"


def _render_revenue(entries: list) -> str:
    lines = []
    for i, e in enumerate(entries):
        sym = "$" if e.currency == "USD" else f"{e.currency} "
        if e.change_pct is not None:
            arrow = "▲" if e.change_pct >= 0 else "▼"
            lines.append(
                f"  {e.period:<20}: {sym}{e.amount:,.0f}  "
                f"({arrow} {abs(e.change_pct):.0f}% vs previous period)"
            )
        else:
            lines.append(f"  {e.period:<20}: {sym}{e.amount:,.0f}")
    return "<revenue>\n" + "\n".join(lines) + "\n</revenue>"


def _render_appointments(entries: list) -> str:
    lines = [
        f"  {e.period:<20}: {e.total} total | {e.cancellation_rate_pct:.0f}% cancellation rate"
        for e in entries
    ]
    return "<appointments>\n" + "\n".join(lines) + "\n</appointments>"


def _render_staff(entries: list) -> str:
    lines = []
    for m in entries:
        note = f" {m.note}" if m.note else ""
        lines.append(
            f"  {m.name:<15}: {m.appointments} appts | "
            f"${m.revenue:,.0f} revenue | {m.rating:.1f} rating{note}"
        )
    return "<staff_performance>\n" + "\n".join(lines) + "\n</staff_performance>"


def _render_services(entries: list) -> str:
    lines = [
        f"  {s.name:<25}: {s.bookings} bookings | ${s.revenue:,.0f} revenue"
        for s in entries
    ]
    return "<service_popularity>\n" + "\n".join(lines) + "\n</service_popularity>"


def _render_campaigns(entries: list) -> str:
    lines = []
    for c in entries:
        roi = ((c.revenue_attributed - c.spend) / c.spend * 100) if c.spend else 0
        lines.append(
            f"  {c.name:<25}: spend ${c.spend:,.0f} | "
            f"{c.conversions} conversions | "
            f"${c.revenue_attributed:,.0f} revenue | ROI {roi:.0f}%"
        )
    return "<marketing_campaigns>\n" + "\n".join(lines) + "\n</marketing_campaigns>"


def _output_format() -> str:
    return """\
<output_format>
Respond with ONLY a valid JSON object using this exact schema:
{
  "summary": "One-sentence direct answer",
  "root_causes": ["cause 1", "cause 2"],
  "supporting_data": "Key figures from the data that support the analysis",
  "recommendations": ["action 1", "action 2", "action 3"],
  "confidence": "high | medium | low",
  "data_gaps": "Missing data that would improve this analysis, or null"
}
</output_format>"""