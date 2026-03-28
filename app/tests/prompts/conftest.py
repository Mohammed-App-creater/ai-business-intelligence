"""
conftest.py
Fixtures for app/tests/prompts/

Project root (ai-business-intelligence/) is added to sys.path
so that `from app.prompts import ...` resolves correctly.
"""
import sys
import os

# Navigate up from app/tests/prompts/ → app/tests/ → app/ → project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

import pytest
from app.prompts.types import (
    ClassifierData, RagChatData, DocGenData,
    RevenueEntry, AppointmentEntry, StaffEntry, ServiceEntry, CampaignEntry,
)


@pytest.fixture
def classifier_data():
    return ClassifierData(question="Why did my revenue decrease this month?")


@pytest.fixture
def full_rag_data():
    return RagChatData(
        business_id="salon_123",
        business_type="Hair Salon",
        analysis_period="March 2026",
        question="Why did my revenue decrease this month?",
        revenue=[
            RevenueEntry("February 2026", 13100.0),
            RevenueEntry("March 2026",     9200.0, change_pct=-29.8),
        ],
        appointments=[
            AppointmentEntry("February 2026", 230, 6.0),
            AppointmentEntry("March 2026",    150, 18.0),
        ],
        staff=[
            StaffEntry("Sarah", 45, 3100.0, 4.8),
            StaffEntry("James", 30, 1800.0, 4.2),
            StaffEntry("Mia",    8,  600.0, 3.1, "(new)"),
        ],
        services=[
            ServiceEntry("Facial Treatment", 42, 2100.0),
        ],
        campaigns=[
            CampaignEntry("Spring Promo", 500.0, 45, 3000.0),
        ],
    )


@pytest.fixture
def minimal_rag_data():
    return RagChatData(
        business_id="spa_001",
        business_type="Spa",
        analysis_period="March 2026",
        question="What is my top service?",
    )


@pytest.fixture
def full_doc_data():
    kpi = (
        "Revenue       : $9,200 (▼ 30% vs $13,100)\n"
        "Visits        : 150 (▼ 35% vs 230)\n"
        "Cancel Rate   : 18%\n"
        "Top staff     : Sarah\n"
        "Top service   : Facial Treatment\n"
        "Notes         : Staff member Mia joined this month."
    )
    return DocGenData(
        business_id="salon_123",
        business_type="Hair Salon",
        period="March 2026",
        doc_domain="revenue",
        doc_type="monthly_summary",
        kpi_block=kpi,
    )


@pytest.fixture
def minimal_doc_data():
    return DocGenData(
        business_id="salon_123",
        business_type="Hair Salon",
        period="March 2026",
        doc_domain="revenue",
        doc_type="monthly_summary",
        kpi_block="(minimal context)",
    )