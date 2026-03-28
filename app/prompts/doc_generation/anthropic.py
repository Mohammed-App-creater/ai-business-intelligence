"""
prompts/doc_generation/anthropic.py
=====================================
Document generation prompt optimised for Anthropic Claude.

Receives a pre-formatted KPI block from the Python template layer.
The LLM's only job is to write a 2-3 sentence observation paragraph.
Prompt evaluation will refine this prompt later — for now it's
intentionally simple and correct.
"""
from __future__ import annotations
from ..types import DocGenData

SYSTEM_PROMPT = """\
<role>
You are a business analyst writing concise performance summaries for
beauty and wellness business owners (salons, spas, barbershops).
Your summaries are stored and later retrieved to answer owner questions.
</role>
<rules>
- Write exactly 2-3 sentences. No more, no less.
- Use plain language — the reader is a business owner, not a data analyst.
- Base the observation strictly on the data in <kpi_data> — do not invent figures.
- Lead with the single most notable insight.
- Third person only: "The business recorded..." not "You recorded..."
- Flowing prose only — no bullet points, headers, or lists.
- Do not add advice or recommendations — observation only.
</rules>"""


def build(data: DocGenData) -> tuple[str, str]:
    """
    Returns (system, user) ready for gateway.call_with_data(UseCase.DOC_GENERATION, ...).
    """
    domain_label = data.doc_domain.replace("_", " ").title()
    type_label   = data.doc_type.replace("_", " ").title()

    entity_line = (
        f"\n  Entity        : {data.entity_name}"
        if data.entity_name else ""
    )

    user = (
        f"<context>\n"
        f"  Business ID   : {data.business_id}\n"
        f"  Business Type : {data.business_type}\n"
        f"  Period        : {data.period}\n"
        f"  Domain        : {domain_label}\n"
        f"  Summary Type  : {type_label}"
        f"{entity_line}\n"
        f"</context>\n\n"
        f"<kpi_data>\n"
        f"{data.kpi_block.strip()}\n"
        f"</kpi_data>\n\n"
        f"Write a 2-3 sentence observation about this {domain_label} period. "
        f"Lead with the most notable insight from the data above."
    )
    return SYSTEM_PROMPT, user
