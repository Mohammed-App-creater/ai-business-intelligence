"""
prompts/doc_generation/openai.py
==================================
Document generation prompt optimised for OpenAI GPT.

OpenAI-specific choices:
  - Plain key-value data block, no XML
  - Direct concise system instruction — GPT needs less scaffolding
  - Same output expectation: 2-3 sentence prose summary
"""
from __future__ import annotations

from ..types import DocGenData

SYSTEM_PROMPT = """\
You write concise monthly performance summaries for beauty and wellness \
business owners. Summaries are stored and used to answer future questions.

Rules:
- 2-3 sentences maximum.
- Plain language — owner is not a data analyst.
- Only use the data provided — no advice or recommendations.
- Lead with the most notable insight.
- Third person only ("The business..." not "You...").
- Flowing prose — no bullet points or headers."""


def build(data: DocGenData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call(UseCase.DOC_GENERATION, ...).
    """
    lines = [
        f"Business ID   : {data.business_id}",
        f"Business Type : {data.business_type}",
        f"Period        : {data.period}",
    ]

    if data.revenue is not None:
        lines.append(f"Revenue       : ${data.revenue:,.0f}")
    if data.prev_revenue is not None and data.revenue is not None:
        change_pct = ((data.revenue - data.prev_revenue) / data.prev_revenue * 100)
        arrow = "▲" if change_pct >= 0 else "▼"
        lines.append(
            f"vs Prev Period: {arrow} {abs(change_pct):.0f}% "
            f"(${data.prev_revenue:,.0f})"
        )
    if data.appointments is not None:
        lines.append(f"Appointments  : {data.appointments}")
    if data.cancellation_rate_pct is not None:
        lines.append(f"Cancel Rate   : {data.cancellation_rate_pct:.0f}%")
    if data.top_service:
        lines.append(f"Top Service   : {data.top_service}")
    if data.top_staff:
        lines.append(f"Top Staff     : {data.top_staff}")
    if data.extra_notes:
        lines.append(f"Notes         : {data.extra_notes}")

    lines.append("")
    lines.append(
        "Write a 2-3 sentence summary of this business period. "
        "Lead with the most notable insight."
    )

    user = "\n".join(lines)
    return SYSTEM_PROMPT, user