"""
prompts/types.py
================
Input dataclasses for all prompt modules.

Every prompt function receives one of these — never raw dicts or loose args.
This is the contract between services and the prompts layer.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

@dataclass
class ClassifierData:
    """
    Input for classifier prompts.
    Used by: query_analyzer when rules are inconclusive.
    """
    question: str


# ---------------------------------------------------------------------------
# RAG Chat
# ---------------------------------------------------------------------------

@dataclass
class RevenueEntry:
    period: str
    amount: float
    currency: str = "USD"
    change_pct: Optional[float] = None   # vs previous period, None for first entry


@dataclass
class AppointmentEntry:
    period: str
    total: int
    cancellation_rate_pct: float


@dataclass
class StaffEntry:
    name: str
    appointments: int
    revenue: float
    rating: float
    note: str = ""


@dataclass
class ServiceEntry:
    name: str
    bookings: int
    revenue: float


@dataclass
class CampaignEntry:
    name: str
    spend: float
    conversions: int
    revenue_attributed: float


@dataclass
class RagChatData:
    """
    Input for rag_chat prompts.
    Used by: RAG pipeline before calling LLM for a business question.

    Two context paths (can be used together):

    1. **documents** (MVP) — pre-formatted text summaries retrieved from
       the vector store. Each string is a complete chunk_text produced by
       the doc generator (KPI block + observation). The prompt template
       renders these as-is inside context tags.

    2. **Typed entry lists** (V2 / warehouse path) — structured data
       populated directly from the warehouse client. Only populate the
       sections relevant to the question — empty lists are omitted from
       the prompt automatically.
    """
    business_id:     str
    business_type:   str                          # e.g. "Hair Salon"
    analysis_period: str                          # e.g. "March 2026"
    question:        str

    # MVP path — RAG documents from vector store
    documents:    list[str]              = field(default_factory=list)

    # V2 path — structured entries from warehouse
    revenue:      list[RevenueEntry]      = field(default_factory=list)
    appointments: list[AppointmentEntry]  = field(default_factory=list)
    staff:        list[StaffEntry]        = field(default_factory=list)
    services:     list[ServiceEntry]      = field(default_factory=list)
    campaigns:    list[CampaignEntry]     = field(default_factory=list)


# ---------------------------------------------------------------------------
# Document generation
# ---------------------------------------------------------------------------

@dataclass
class DocGenData:
    """
    Input for doc_generation prompts.

    The Python template in doc_generator.py builds the full KPI block
    as a pre-formatted string (kpi_block). The LLM only writes the
    observation paragraph — it never sees raw numbers directly.

    Fields
    ------
    business_id   : Tenant identifier (e.g. "42")
    business_type : Human label (e.g. "Hair Salon")
    period        : Human-readable period (e.g. "March 2026")
    doc_domain    : Domain being summarised — 'revenue' | 'staff' |
                    'services' | 'clients' | 'appointments' |
                    'expenses' | 'reviews' | 'payments' |
                    'campaigns' | 'attendance' | 'subscriptions'
    doc_type      : Document shape — 'monthly_summary' | 'individual' |
                    'ranking' | 'retention_summary' | 'top_spenders' |
                    'location_breakdown' | 'daily_trend'
    kpi_block     : Pre-formatted KPI text built by the Python template.
                    The LLM writes an observation based on this text.
    entity_name   : Optional — used when doc_type='individual' to name
                    the staff member or service being summarised.
    """
    business_id:   str
    business_type: str
    period:        str
    doc_domain:    str
    doc_type:      str
    kpi_block:     str
    entity_name:   str = ""