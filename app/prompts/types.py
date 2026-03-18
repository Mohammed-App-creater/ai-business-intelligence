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

    Only populate the sections relevant to the question —
    empty lists are omitted from the prompt automatically.
    """
    business_id:     str
    business_type:   str                          # e.g. "Hair Salon"
    analysis_period: str                          # e.g. "March 2026"
    question:        str

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
    Used by: ETL pipeline to generate human-readable summaries
    from warehouse rows before embedding into the vector store.
    """
    business_id:   str
    business_type: str
    period:        str

    # At least one of the following should be populated
    revenue:       Optional[float]  = None
    prev_revenue:  Optional[float]  = None       # for MoM comparison
    appointments:  Optional[int]    = None
    cancellation_rate_pct: Optional[float] = None
    top_service:   Optional[str]    = None
    top_staff:     Optional[str]    = None
    extra_notes:   Optional[str]    = None       # any free-form context