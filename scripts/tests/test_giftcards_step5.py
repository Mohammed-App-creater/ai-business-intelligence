"""
test_giftcards_step5.py
========================

Step 5 verification — Connect to Chat (Domain 9, Sprint 9).

Asserts:
  1. Chunk generators return non-empty text on minimal inputs
  2. Vocab audit (L5/L6) — synonym header on every chunk + per-location
     chunk has both 'branch' AND 'location'
  3. PII guardrail — no email/phone/GC-XXX/cust_id leaks in normal chunks;
     anomalies chunk allows internal integer card IDs only
  4. Three-hop routing simulation — all 30 acceptance questions match the
     "giftcards" keyword group
  5. End-to-end: GiftcardsExtractor.run() → returns rows + writes warehouse;
     generate_giftcards_docs(warehouse_rows, ...) → embeds + upserts chunks;
     anchor numbers persist through the full chain
  6. Idempotency: re-running the ETL produces the same warehouse row counts
"""

import asyncio
import re
import sys
from datetime import date

import asyncpg

sys.path.insert(0, "/home/claude")
sys.path.insert(0, "/home/claude/smoketest")

from giftcards import (
    CHUNK_GENERATORS,
    generate_giftcards_docs,
    gen_monthly_summary, gen_liability_snapshot, gen_by_staff,
    gen_by_location, gen_aging_bucket, gen_dormancy_summary,
    gen_anomalies_snapshot, gen_denomination_snapshot, gen_health_snapshot,
)
from giftcards_etl import GiftcardsExtractor
from query_analyzer_giftcards_keywords import GIFTCARDS_KEYWORDS
from tests.mocks.mock_analytics_server import start_mock_server


# =============================================================================
# Minimal AnalyticsClient (mirrors the real one — _post + 8 giftcard methods)
# =============================================================================

import httpx


class AnalyticsClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def _post(self, path, payload):
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(f"{self.base_url}{path}", json=payload)
            r.raise_for_status()
            body = r.json()
            return body.get("data", [])

    async def get_giftcard_monthly(self, business_id, start_date, end_date):
        return await self._post("/api/v1/leo/giftcards/monthly", {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        })

    async def get_giftcard_liability_snapshot(self, business_id, snapshot_date):
        return await self._post("/api/v1/leo/giftcards/liability-snapshot", {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        })

    async def get_giftcard_by_staff(self, business_id, start_date, end_date):
        return await self._post("/api/v1/leo/giftcards/by-staff", {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        })

    async def get_giftcard_by_location(self, business_id, start_date, end_date):
        return await self._post("/api/v1/leo/giftcards/by-location", {
            "business_id": business_id,
            "start_date":  start_date.isoformat(),
            "end_date":    end_date.isoformat(),
        })

    async def get_giftcard_aging_snapshot(self, business_id, snapshot_date):
        return await self._post("/api/v1/leo/giftcards/aging-snapshot", {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        })

    async def get_giftcard_anomalies_snapshot(self, business_id, snapshot_date,
                                                start_date, end_date):
        return await self._post("/api/v1/leo/giftcards/anomalies-snapshot", {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
            "start_date":    start_date.isoformat(),
            "end_date":      end_date.isoformat(),
        })

    async def get_giftcard_denomination_snapshot(self, business_id, snapshot_date):
        return await self._post("/api/v1/leo/giftcards/denomination-snapshot", {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        })

    async def get_giftcard_health_snapshot(self, business_id, snapshot_date):
        return await self._post("/api/v1/leo/giftcards/health-snapshot", {
            "business_id":   business_id,
            "snapshot_date": snapshot_date.isoformat(),
        })


# =============================================================================
# Fake EmbeddingClient + VectorStore (matches clients.py upsert/exists API)
# =============================================================================

class FakeEmbeddingClient:
    async def embed(self, text: str) -> list[float]:
        return [0.0] * 1536

class FakeVectorStore:
    def __init__(self):
        self.docs = {}

    async def exists(self, doc_id: str) -> bool:
        return doc_id in self.docs

    async def upsert(self, *, doc_id, tenant_id, doc_domain, doc_type,
                       chunk_text, embedding, metadata):
        self.docs[doc_id] = {
            "tenant_id":  tenant_id,
            "doc_domain": doc_domain,
            "doc_type":   doc_type,
            "chunk_text": chunk_text,
            "metadata":   metadata,
        }


# =============================================================================
# 30 acceptance questions
# =============================================================================

ACCEPTANCE_QUESTIONS = {
    "Q1":  "How many gift cards did I sell last month?",
    "Q2":  "What's my outstanding gift card liability?",
    "Q3":  "How many active gift cards do I have?",
    "Q4":  "What's the gift card redemption trend over the last 6 months?",
    "Q5":  "How many gift cards have I sold this year so far?",
    "Q6":  "Has my gift card liability gone up or down over the last 6 months?",
    "Q7":  "How does this March compare to last March for gift card redemption?",
    "Q8":  "Which staff redeems the most gift cards?",
    "Q9":  "Which branch has the most gift card redemptions?",
    "Q10": "What percentage of gift card redemption happened at Westside?",
    "Q12": "What's the most common gift card denomination?",
    "Q13": "Why is my gift card revenue up so much this month?",
    "Q14": "How many gift cards are sitting unused?",
    "Q15": "On average, how long does a gift card sit before it gets redeemed?",
    "Q16": "Should I be promoting gift cards more?",
    "Q17": "What should I do about dormant gift cards?",
    "Q18": "How many prepaid cards do I have outstanding?",
    "Q19": "What's the total value on my gift vouchers?",
    "Q20": "How much stored value do customers still have?",
    "Q21": "How many GCs got redeemed last month?",
    "Q22": "What's the average remaining balance on active gift cards?",
    "Q23": "What percentage of gift cards I issued have been redeemed?",
    "Q24": "Are there any gift cards that show drained but still active?",
    "Q25": "How many gift cards have been deactivated?",
    "Q26": "Which gift card has been dormant the longest?",
    "Q27": "How much extra do customers spend on top of their gift cards?",
    "Q28": "What's the aging breakdown of my outstanding gift card liability?",
    "Q29": "Are gift cards more often redeemed on weekends?",
    "Q30": "How many of my redeemed gift cards needed multiple visits to drain?",
    "Q31": "Were there any refunded gift card redemptions this quarter?",
}


def matches_giftcards_group(question: str) -> tuple[bool, list[str]]:
    q_lower = question.lower()
    matched = [kw for kw in GIFTCARDS_KEYWORDS if kw in q_lower]
    return (len(matched) > 0, matched)


# =============================================================================
# Tests
# =============================================================================

async def test_chunk_generators_minimal_inputs():
    print("\n=== TEST 1 — chunk generators don't crash on minimal inputs ===")
    minimal = {
        "monthly_summary":      {"period_start": date(2026, 3, 1)},
        "liability_snapshot":   {"snapshot_date": date(2026, 3, 31)},
        "by_staff":             {"period_start": date(2026, 3, 1), "staff_name": "Test"},
        "by_location":          {"period_start": date(2026, 3, 1), "location_name": "TestLoc",
                                  "location_id": 1},
        "aging_bucket":         {"snapshot_date": date(2026, 3, 31), "age_bucket": "0-30"},
        "dormancy_summary":     {"snapshot_date": date(2026, 3, 31)},
        "anomalies_snapshot":   {"snapshot_date": date(2026, 3, 31)},
        "denomination_snapshot": [{"snapshot_date": date(2026, 3, 31),
                                    "denomination_bucket": "$51-$100",
                                    "card_count": 4, "total_value_issued": 375,
                                    "avg_face_value": 93.75, "pct_of_cards": 40}],
        "health_snapshot":      {"snapshot_date": date(2026, 3, 31)},
    }
    for doc_type, fn in CHUNK_GENERATORS.items():
        text = fn(minimal[doc_type])
        assert isinstance(text, str) and len(text) > 50, f"{doc_type}: too short"
        print(f"  ✓ {doc_type:<24s} → {len(text)} chars")
    print(f"All {len(CHUNK_GENERATORS)} chunk generators produce non-empty text ✓")


async def test_vocab_audit():
    print("\n=== TEST 2 — Lesson 5/6 vocab audit ===")
    monthly_text = gen_monthly_summary({
        "period_start": date(2026, 3, 1),
        "redemption_count": 6, "redemption_amount_total": 235.50,
        "distinct_cards_redeemed": 4, "activation_count": 1,
        "weekend_redemption_count": 2, "weekday_redemption_count": 4,
        "avg_uplift_per_visit": 35.83, "uplift_total": 215.00,
        "mom_redemption_pct": 2255.00, "mom_activation_pct": 0.00,
        "yoy_redemption_pct": 1077.50,
    })
    location_text = gen_by_location({
        "period_start": date(2026, 3, 1), "location_id": 1,
        "location_name": "Main St", "redemption_count": 4,
        "redemption_amount_total": 180.00, "distinct_cards_redeemed": 2,
        "pct_of_org_redemption": 76.43, "mom_redemption_pct": 1700.00,
    }, org_total=235.50)
    staff_text = gen_by_staff({
        "staff_id": 12, "staff_name": "Maria Lopez", "is_active": 1,
        "period_start": date(2026, 3, 1), "redemption_count": 3,
        "redemption_amount_total": 135.00, "distinct_cards_redeemed": 2,
        "rank_in_period": 1,
    })
    liability_text = gen_liability_snapshot({
        "snapshot_date": date(2026, 3, 31), "active_card_count": 9,
        "outstanding_liability_total": 1125.50,
        "avg_remaining_balance_excl_drained": 187.58,
        "avg_remaining_balance_incl_drained": 125.06,
        "drained_active_count": 3, "median_remaining_balance": 160.00,
    })
    anomalies_text = gen_anomalies_snapshot({
        "snapshot_date": date(2026, 3, 31), "drained_active_count": 3,
        "drained_active_card_ids": [1, 2, 8], "deactivated_count": 1,
        "deactivated_value_total_derived": 300.00,
        "refunded_redemption_count": 0, "refunded_redemption_amount": 0.00,
        "period_start": date(2026, 1, 1), "period_end": date(2026, 3, 31),
    })

    synonyms = ["gift card", "giftcard", "prepaid card", "gift voucher",
                "stored value", "gc"]
    for name, text in [("monthly", monthly_text), ("location", location_text),
                        ("staff", staff_text), ("liability", liability_text),
                        ("anomalies", anomalies_text)]:
        for syn in synonyms:
            assert syn.lower() in text.lower(), \
                f"L6 violation in {name}: missing '{syn}'"
        print(f"  ✓ L6 — {name} chunk carries all 6 synonym variants")

    assert "branch" in location_text.lower()
    assert "location" in location_text.lower()
    print("  ✓ L5 — per-location chunk has both 'branch' and 'location'")

    assert "$235.50" in location_text or "235.50" in location_text
    print("  ✓ P5 — location chunk includes org-wide total for disambiguation")

    assert "zero refunded" in anomalies_text.lower()
    print("  ✓ Q31 — anomalies chunk explicitly emits zero-refund language")


async def test_pii_guardrail():
    print("\n=== TEST 3 — PII guardrail (P7) ===")
    rep_chunks = [
        gen_monthly_summary({
            "period_start": date(2026, 3, 1), "redemption_count": 6,
            "redemption_amount_total": 235.50, "distinct_cards_redeemed": 4,
            "activation_count": 1, "weekend_redemption_count": 2,
            "weekday_redemption_count": 4, "avg_uplift_per_visit": 35.83,
            "uplift_total": 215.00,
        }),
        gen_health_snapshot({
            "snapshot_date": date(2026, 3, 31), "total_cards_issued": 10,
            "cards_with_redemption": 7, "redemption_rate_pct": 70.00,
            "single_visit_drained_count": 3, "multi_visit_redeemed_count": 4,
            "single_visit_drained_pct_of_redeemed": 42.86,
            "multi_visit_redeemed_pct_of_redeemed": 57.14,
            "distinct_customer_redeemers": 7,
        }),
    ]
    pii_patterns = {
        "email":   r"[\w\.\-]+@[\w\.\-]+",
        "phone":   r"\b\d{3}[\-\.\s]?\d{3}[\-\.\s]?\d{4}\b",
        "card_no": r"\bGC-\d{3,}\b",
        "cust_id": r"\bcust_id[\s:=]+\d+\b",
    }
    for i, text in enumerate(rep_chunks):
        for label, pat in pii_patterns.items():
            assert re.search(pat, text, flags=re.IGNORECASE) is None, \
                f"PII leak: {label} in chunk {i}"
    print(f"  ✓ {len(rep_chunks)} chunks free of email/phone/card-no/cust_id leaks")

    anomalies_text = gen_anomalies_snapshot({
        "snapshot_date": date(2026, 3, 31), "drained_active_count": 3,
        "drained_active_card_ids": [1, 2, 8], "deactivated_count": 1,
        "deactivated_value_total_derived": 300.00,
        "refunded_redemption_count": 0, "refunded_redemption_amount": 0.00,
        "period_start": date(2026, 1, 1), "period_end": date(2026, 3, 31),
    })
    assert not re.search(r"\bGC-\d+\b", anomalies_text)
    print("  ✓ anomalies chunk uses internal IDs only, no GC-XXX strings")


async def test_routing_against_acceptance_questions():
    print("\n=== TEST 4 — three-hop routing against 30 acceptance questions ===")
    matched_count = 0
    unmatched = []
    for qid, q in ACCEPTANCE_QUESTIONS.items():
        ok, kws = matches_giftcards_group(q)
        if ok:
            matched_count += 1
        else:
            unmatched.append((qid, q))
    if unmatched:
        for qid, q in unmatched:
            print(f"  ✗ {qid}: {q}")
    assert len(unmatched) == 0, f"{len(unmatched)} unmatched"
    print(f"  ✓ {matched_count}/{len(ACCEPTANCE_QUESTIONS)} acceptance questions route correctly")


async def test_etl_returns_rows_and_writes_warehouse():
    """End-to-end — extractor returns rows AND writes warehouse simultaneously."""
    print("\n=== TEST 5 — extractor returns rows + writes warehouse ===")

    server = start_mock_server()
    print(f"  Mock server up at {server.base_url}")

    pool = await asyncpg.create_pool(
        host="localhost", port=5432,
        user="leo_test", password="leo_test",
        database="leo_warehouse_test",
        min_size=1, max_size=4,
    )

    # Clean state
    async with pool.acquire() as conn:
        for tbl in ["wh_giftcard_monthly", "wh_giftcard_liability_snapshot",
                    "wh_giftcard_by_staff", "wh_giftcard_by_location",
                    "wh_giftcard_aging_snapshot", "wh_giftcard_anomalies_snapshot",
                    "wh_giftcard_denomination_snapshot", "wh_giftcard_health_snapshot"]:
            await conn.execute(f"DELETE FROM {tbl} WHERE business_id = 42")

    # Run the extractor — gets rows back AND writes warehouse
    client = AnalyticsClient(server.base_url)
    extractor = GiftcardsExtractor(client=client, wh_pool=pool)
    rows = await extractor.run(
        business_id=42,
        start_date=date(2025, 1, 1),
        end_date=date(2026, 3, 31),
    )

    # Assert: extractor returned populated dict
    assert isinstance(rows, dict)
    assert len(rows["monthly"]) == 13
    assert rows["liability"] is not None
    assert float(rows["liability"]["outstanding_liability_total"]) == 1125.50
    assert len(rows["by_staff"]) == 12
    assert len(rows["by_location"]) == 10
    assert len(rows["aging"]) == 5
    assert rows["anomalies"]["refunded_redemption_count"] == 0
    assert len(rows["denomination"]) == 6
    assert rows["health"]["total_cards_issued"] == 10
    print(f"  ✓ extractor returned rows: monthly={len(rows['monthly'])}, "
          f"by_staff={len(rows['by_staff'])}, aging={len(rows['aging'])}, "
          f"liability=${float(rows['liability']['outstanding_liability_total']):.2f}")

    # Assert: warehouse populated as side effect
    async with pool.acquire() as conn:
        n_monthly = await conn.fetchval(
            "SELECT COUNT(*) FROM wh_giftcard_monthly WHERE business_id = 42")
        n_liab = await conn.fetchval(
            "SELECT outstanding_liability_total FROM wh_giftcard_liability_snapshot "
            "WHERE business_id = 42 AND snapshot_date = '2026-03-31'")
        n_anom = await conn.fetchval(
            "SELECT refunded_redemption_count FROM wh_giftcard_anomalies_snapshot "
            "WHERE business_id = 42")
    assert n_monthly == 13
    assert float(n_liab) == 1125.50
    assert n_anom == 0
    print(f"  ✓ warehouse populated as side effect: monthly={n_monthly} rows, "
          f"liability=${float(n_liab):.2f}, anomalies refunded_count={n_anom}")

    # Doc generator — feeds rows directly (NO wh reader needed)
    emb = FakeEmbeddingClient()
    vs = FakeVectorStore()
    result = await generate_giftcards_docs(
        org_id=42, warehouse_rows=rows, embedding_client=emb,
        vector_store=vs, force=True,
    )
    assert result["docs_failed"] == 0
    print(f"  ✓ generate_giftcards_docs(warehouse_rows=...) — "
          f"created={result['docs_created']} skipped={result['docs_skipped']} failed={result['docs_failed']}")

    # Verify chunks landed correctly in vector store
    print(f"  ✓ {len(vs.docs)} chunks stored in (fake) vector store")

    # Tally by doc_type
    from collections import Counter
    by_type = Counter(d["doc_type"] for d in vs.docs.values())
    print(f"  Doc types:")
    for dt, n in sorted(by_type.items()):
        print(f"    {dt:<24s}: {n}")

    # Verify is_rollup metadata flags set correctly
    rollup_types = [d for d in vs.docs.values() if d["metadata"].get("is_rollup") is True]
    per_loc_or_staff = [d for d in vs.docs.values() if d["metadata"].get("is_rollup") is False]
    print(f"  ✓ is_rollup=True: {len(rollup_types)} chunks (org-wide)")
    print(f"  ✓ is_rollup=False: {len(per_loc_or_staff)} chunks (per-staff/per-location)")
    assert all(d["doc_type"] in ("by_staff", "by_location") for d in per_loc_or_staff)

    # Verify all chunks have correct tenant_id and doc_domain
    assert all(d["tenant_id"] == "42" for d in vs.docs.values())
    assert all(d["doc_domain"] == "giftcards" for d in vs.docs.values())
    print(f"  ✓ all chunks tagged tenant_id=42 doc_domain=giftcards")

    # Sample chunk with anchor numbers
    liab_chunks = [d for d in vs.docs.values() if d["doc_type"] == "liability_snapshot"]
    assert len(liab_chunks) == 1
    assert "$1,125.50" in liab_chunks[0]["chunk_text"]
    assert "9 active" in liab_chunks[0]["chunk_text"]
    print(f"  ✓ liability chunk contains anchor: $1,125.50, 9 active cards")

    anom_chunks = [d for d in vs.docs.values() if d["doc_type"] == "anomalies_snapshot"]
    assert len(anom_chunks) == 1
    assert "zero refunded" in anom_chunks[0]["chunk_text"].lower()
    print(f"  ✓ anomalies chunk contains 'zero refunded' (Q31 always-emit)")

    # Idempotency: re-run extractor → same warehouse counts
    print("\n  --- idempotency check ---")
    rows2 = await extractor.run(
        business_id=42,
        start_date=date(2025, 1, 1),
        end_date=date(2026, 3, 31),
    )
    async with pool.acquire() as conn:
        n_monthly2 = await conn.fetchval(
            "SELECT COUNT(*) FROM wh_giftcard_monthly WHERE business_id = 42")
    assert n_monthly2 == n_monthly
    print(f"  ✓ idempotency: re-run produces same {n_monthly2} rows in wh_giftcard_monthly")

    await pool.close()
    server.stop()


async def main():
    await test_chunk_generators_minimal_inputs()
    await test_vocab_audit()
    await test_pii_guardrail()
    await test_routing_against_acceptance_questions()
    await test_etl_returns_rows_and_writes_warehouse()
    print("\n========================================")
    print("✓ STEP 5 VERIFICATION: ALL TESTS PASSED")
    print("========================================")


asyncio.run(main())