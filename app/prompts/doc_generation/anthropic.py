"""
prompts/doc_generation/anthropic.py
=====================================
Document generation prompt optimised for Anthropic Claude.

Used by the ETL pipeline to convert warehouse rows into human-readable
narrative summaries before embedding into the vector store.

Anthropic-specific choices:
  - XML tags wrap the raw data for clear boundary
  - Explicit instruction to write a flowing paragraph (not bullet points)
  - Chain-of-thought nudge — identify the most notable insight first
  - Tone instruction: factual, concise, written for a non-technical owner
"""
from __future__ import annotations

from ..types import DocGenData

SYSTEM_PROMPT = """\
<role>
You are a business analyst writing concise monthly performance summaries
for beauty and wellness business owners. Your summaries will be stored
and used to answer future questions about business performance.
</role>

<rules>
- Write 2-3 sentences maximum.
- Use plain language — the reader is a business owner, not a data analyst.
- Stick strictly to the data provided — do not add advice or recommendations.
- Lead with the most notable insight from the data.
- Write in third person (e.g. "The business recorded..." not "You recorded...").
- Do not use bullet points or headers — write flowing prose only.
</rules>"""


def build(data: DocGenData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call(UseCase.DOC_GENERATION, ...).
    """
    lines = [
        f"<business_data>",
        f"  Business ID   : {data.business_id}",
        f"  Business Type : {data.business_type}",
        f"  Period        : {data.period}",
    ]

    if data.revenue is not None:
        lines.append(f"  Revenue       : ${data.revenue:,.0f}")
    if data.prev_revenue is not None and data.revenue is not None:
        change_pct = ((data.revenue - data.prev_revenue) / data.prev_revenue * 100)
        arrow = "▲" if change_pct >= 0 else "▼"
        lines.append(
            f"  vs Prev Period: {arrow} {abs(change_pct):.0f}% "
            f"(${data.prev_revenue:,.0f})"
        )
    if data.appointments is not None:
        lines.append(f"  Appointments  : {data.appointments}")
    if data.cancellation_rate_pct is not None:
        lines.append(f"  Cancel Rate   : {data.cancellation_rate_pct:.0f}%")
    if data.top_service:
        lines.append(f"  Top Service   : {data.top_service}")
    if data.top_staff:
        lines.append(f"  Top Staff     : {data.top_staff}")
    if data.extra_notes:
        lines.append(f"  Notes         : {data.extra_notes}")

    lines.append("</business_data>")
    lines.append("")
    lines.append(
        "Write a 2-3 sentence summary of this business period. "
        "Lead with the most notable insight."
    )

    user = "\n".join(lines)
    return SYSTEM_PROMPT, user