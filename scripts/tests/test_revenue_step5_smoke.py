from __future__ import annotations

import asyncio
import math
import sys
import traceback
import types
import importlib.util
from datetime import date, datetime
from pathlib import Path

# Allow direct script execution: `python scripts/tests/test_revenue_step5_smoke.py`
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep this script independent from full env/settings validation.
_fake_config = types.ModuleType("app.core.config")
_fake_config.settings = type("_Settings", (), {"ANALYTICS_BACKEND_URL": "http://127.0.0.1:0"})()
sys.modules["app.core.config"] = _fake_config

# Compatibility shim for broken singular/plural package path in doc generator.
sys.modules["app.services.doc_generator"] = types.ModuleType("app.services.doc_generator")
sys.modules["app.services.doc_generator.domains"] = types.ModuleType("app.services.doc_generator.domains")
_rev_path = ROOT / "app" / "services" / "doc_generators" / "domains" / "revenue.py"
_spec = importlib.util.spec_from_file_location("app.services.doc_generator.domains.revenue", _rev_path)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load revenue module from {_rev_path}")
_revenue_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_revenue_mod)
sys.modules["app.services.doc_generator.domains.revenue"] = _revenue_mod

from app.services.analytics_client import AnalyticsClient
from app.services.doc_generators.domains import DocGenerator
from app.services.query_analyzer import QueryAnalyzer, Route
from etl.transforms.revenue_etl import RevenueExtractor
from tests.mocks.mock_analytics_server import REVENUE_PATHS, start_mock_server
from tests.mocks.revenue_fixtures import (
    FAILED_REFUNDS,
    FIXTURES,
    LOCATION_REVENUE,
    MONTHLY_SUMMARY,
    PAYMENT_TYPES,
    PROMO_IMPACT,
    STAFF_REVENUE,
)


ORG_ID = 42
START_DATE = date(2025, 1, 1)
END_DATE = date(2025, 6, 30)

REVENUE_QUESTIONS = [
    "What was my total revenue last month?",
    "How much revenue did I make this year so far?",
    "What is my average ticket value per visit?",
    "How does my revenue this month compare to last month?",
    "Is my revenue trending up or down over the last 6 months?",
    "Which was my best revenue month this year?",
    "How does my revenue this quarter compare to the same quarter last year?",
    "Which staff member generated the most revenue last month?",
    "Which location brought in the most revenue this year?",
    "What percentage of my revenue came from cash vs card vs other payment types?",
    "How much of my revenue came from gift cards being redeemed?",
    "How much revenue did promo codes cost me last month?",
    "Why did my revenue drop last month?",
    "My revenue went up this month but I feel like I was less busy - why?",
    "I had a lot of no-shows last week - how much revenue did that cost me?",
    "What can I do to increase my revenue next month?",
    "Should I be worried about my revenue trend - is my business growing or shrinking?",
    "How much in tips did my staff collect last month?",
    "How much tax did I collect this month?",
    "How many visits ended with a refund or failed payment?",
]


class _FakeGatewayResponse:
    def __init__(self, content: str) -> None:
        self.content = content


class _FakeGateway:
    async def call_with_data(self, *args, **kwargs):
        return _FakeGatewayResponse("Smoke observation.")


class _FakeEmbeddingClient:
    async def embed(self, text: str) -> list[float]:
        seed = sum(ord(c) for c in text)
        vec = [math.sin(seed + i) for i in range(16)]
        norm = math.sqrt(sum(v * v for v in vec))
        return [v / norm for v in vec] if norm else [0.0] * 16


class _FakeVectorStore:
    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    async def get_doc_ids(self, tenant_id: str, doc_domain: str, doc_type: str) -> list[str]:
        return [
            d["doc_id"]
            for d in self.docs.values()
            if d["tenant_id"] == tenant_id
            and d["doc_domain"] == doc_domain
            and d["doc_type"] == doc_type
        ]

    async def get_doc_metadata(self, tenant_id: str, doc_id: str) -> dict | None:
        rec = self.docs.get(doc_id)
        if rec and rec["tenant_id"] == tenant_id:
            return rec["metadata"]
        return None

    async def upsert(
        self,
        tenant_id: str,
        doc_id: str,
        doc_domain: str,
        doc_type: str,
        chunk_text: str,
        embedding: list[float],
        period_start: date | None,
        metadata: dict,
    ) -> None:
        self.docs[doc_id] = {
            "tenant_id": tenant_id,
            "doc_id": doc_id,
            "doc_domain": doc_domain,
            "doc_type": doc_type,
            "chunk_text": chunk_text,
            "embedding": embedding,
            "period_start": period_start,
            "metadata": metadata,
        }


class _FakeRevenueWarehouse:
    async def get_monthly_trend(self, org_id: int, months: int = 6) -> list[dict]:
        rows = []
        for r in MONTHLY_SUMMARY["data"]:
            year, month = r["period"].split("-")
            rows.append(
                {
                    "period_start": date(int(year), int(month), 1),
                    "gross_revenue": r["service_revenue"],
                    "total_tips": r["total_tips"],
                    "total_discounts": r["total_discounts"],
                    "visit_count": r["visit_count"],
                    "avg_visit_value": r["avg_ticket"],
                    "cancelled_visit_count": r["cancel_count"],
                    "cash_revenue": 0.0,
                    "card_revenue": 0.0,
                    "total_gc_amount": r["gc_redemptions"],
                    "other_revenue": 0.0,
                }
            )
        rows.sort(key=lambda x: x["period_start"], reverse=True)
        return rows[:months]

    async def get_payment_type_breakdown(self, org_id: int, months: int = 3) -> list[dict]:
        return PAYMENT_TYPES["data"]

    async def get_staff_revenue(self, org_id: int, months: int = 3) -> list[dict]:
        return STAFF_REVENUE["data"]

    async def get_location_revenue(self, org_id: int, months: int = 3) -> list[dict]:
        return LOCATION_REVENUE["data"]

    async def get_promo_impact(self, org_id: int, months: int = 3) -> list[dict]:
        return PROMO_IMPACT["data"]

    async def get_failed_refunds(self, org_id: int, months: int = 3) -> list[dict]:
        return FAILED_REFUNDS["data"]


class _FakeWarehouse:
    def __init__(self) -> None:
        self.revenue = _FakeRevenueWarehouse()


def _print_check(name: str, ok: bool, details: str = "") -> None:
    status = "PASS" if ok else "FAIL"
    suffix = f" - {details}" if details else ""
    print(f"[{status}] {name}{suffix}")


async def check_1_mock_server_endpoints(client: AnalyticsClient) -> tuple[bool, str]:
    calls = [
        client.get_revenue_monthly_summary(ORG_ID, START_DATE, END_DATE),
        client.get_revenue_payment_types(ORG_ID, START_DATE, END_DATE),
        client.get_revenue_by_staff(ORG_ID, START_DATE, END_DATE),
        client.get_revenue_by_location(ORG_ID, START_DATE, END_DATE),
        client.get_revenue_promo_impact(ORG_ID, START_DATE, END_DATE),
        client.get_revenue_failed_refunds(ORG_ID, START_DATE, END_DATE),
    ]
    results = await asyncio.gather(*calls)
    expected = [FIXTURES[p]["data"] for p in REVENUE_PATHS]
    if results != expected:
        return False, "one or more endpoint payloads differ from fixtures"
    return True, f"{len(results)} endpoints returned fixture data"


async def check_2_revenue_extractor(client: AnalyticsClient) -> tuple[bool, str]:
    docs = await RevenueExtractor(client=client).run(
        business_id=ORG_ID, start_date=START_DATE, end_date=END_DATE
    )
    if not docs:
        return False, "extractor returned no docs"

    required_keys = {"tenant_id", "domain", "doc_type", "period", "text"}
    bad = [d for d in docs if not required_keys.issubset(d.keys())]
    if bad:
        return False, "some docs missing required shape keys"

    doc_types = {d.get("doc_type") for d in docs}
    expected_types = {
        "monthly_summary",
        "payment_type_breakdown",
        "staff_revenue",
        "location_revenue",
        "promo_impact",
        "failed_refunds",
    }
    if not expected_types.issubset(doc_types):
        return False, "not all revenue doc types were produced"

    return True, f"{len(docs)} docs produced with required shape"


async def check_3_query_analyzer_routing() -> tuple[bool, str]:
    analyzer = QueryAnalyzer()
    results = await asyncio.gather(*(analyzer.analyze(q, business_id=str(ORG_ID)) for q in REVENUE_QUESTIONS))
    non_rag = [q for q, res in zip(REVENUE_QUESTIONS, results) if res.route != Route.RAG]
    if non_rag:
        return False, f"{len(non_rag)} questions not routed to RAG"
    return True, "all 20/20 revenue questions routed to RAG"


async def check_4_doc_generator_revenue() -> tuple[bool, str]:
    generator = DocGenerator(
        warehouse=_FakeWarehouse(),
        gateway=_FakeGateway(),
        embedding_client=_FakeEmbeddingClient(),
        vector_store=_FakeVectorStore(),
    )
    created, skipped, failed = await generator._gen_revenue(
        org_id=ORG_ID,
        period_start=START_DATE,
        months=1,
        force=True,
    )
    if failed > 0:
        return False, f"_gen_revenue reported failures={failed}"
    if created == 0 and skipped == 0:
        return False, "_gen_revenue produced no documents"
    return True, f"_gen_revenue ran: created={created}, skipped={skipped}, failed={failed}"


async def _main() -> int:
    server = start_mock_server()
    client = AnalyticsClient(base_url=server.base_url)

    checks = [
        ("1) Mock server + 6 endpoints", check_1_mock_server_endpoints(client)),
        ("2) RevenueExtractor end-to-end", check_2_revenue_extractor(client)),
        ("3) Query analyzer routes 20 revenue questions", check_3_query_analyzer_routing()),
        ("4) doc_generator._gen_revenue with mock warehouse", check_4_doc_generator_revenue()),
    ]

    all_ok = True
    try:
        for name, coro in checks:
            try:
                ok, details = await coro
            except Exception as exc:
                ok = False
                details = f"{exc.__class__.__name__}: {exc}"
                traceback.print_exc()
            _print_check(name, ok, details)
            all_ok = all_ok and ok
    finally:
        server.stop()

    print("\nOverall:", "PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
