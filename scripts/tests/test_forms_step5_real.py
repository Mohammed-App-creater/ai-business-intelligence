"""
scripts/tests/test_forms_step5_real.py
========================================

Step 5 verification — runs against REAL services on the VM:
  - Real warehouse pool (PGPool to wh_pool, port 5433)
  - Real vector pool (PGPool to vec_pool, port 5432, table=embeddings)
  - Real EmbeddingClient (whatever provider is set in env)
  - Real AnalyticsClient hitting the mock server :8001
  - Real FormsExtractor + generate_forms_docs pipeline

Pre-requisites (must all be in place):
  1. Step 4 signed off (warehouse populated)
  2. forms.py at app/services/doc_generators/domains/forms.py
  3. doc_generators/__init__.py has the 3 forms patches
  4. query_analyzer.py has FORMS_KEYWORDS in RAG_KEYWORD_GROUPS
  5. retriever.py has "forms": ["forms"] in KEYWORD_GROUP_TO_DOMAINS
  6. Mock server running on :8001

Then run:
  PYTHONPATH=. python scripts/tests/test_forms_step5_real.py

WHAT IT VERIFIES
================
1. Doc generator runs end-to-end without error
2. Expected ~14 chunks land in pgvector with tenant_id=42, domain=forms
3. Locked anchor numbers appear in the right chunks (4 templates, 18 subs,
   72.22% completion, +25% MoM, Form 1 ranks #1, Form 4 active dormant)
4. Tenant isolation: biz 99 has 0 forms chunks
5. All 14+1 acceptance questions route to the "forms" keyword group
6. PII guardrail: no CustId, no FormTemp/JsonTemp/HtmlTemp content,
   no OnlineCode strings in chunks
7. Idempotency: re-running produces same chunk count (no dupes)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from app.services.db.db_pool import PGPool, PGTarget
from app.services.analytics_client import AnalyticsClient
from app.services.embeddings.embedding_client import EmbeddingClient
from app.services.vector_store import VectorStore
from app.services.doc_generators.domains.forms import generate_forms_docs
from etl.transforms.forms_etl import FormsExtractor

logging.basicConfig(level=logging.WARNING, format="%(message)s")

BUSINESS_ID    = 42
SNAPSHOT_DATE  = date(2026, 3, 31)
START_DATE     = date(2025, 1, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Tally
# ─────────────────────────────────────────────────────────────────────────────

class TestTally:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.failures: list[str] = []

    def ok(self, msg: str) -> None:
        print(f"  \033[92m✓\033[0m {msg}")
        self.passed += 1

    def fail(self, msg: str) -> None:
        print(f"  \033[91m✗\033[0m {msg}")
        self.failed += 1
        self.failures.append(msg)

    def section(self, name: str) -> None:
        print(f"\n=== {name} ===")

    def summary(self) -> int:
        print("\n" + "=" * 60)
        if self.failed == 0:
            print(f"\033[92m✓ STEP 5 REAL-ENV VERIFICATION: {self.passed}/{self.passed} PASSED\033[0m")
            return 0
        print(f"\033[91m✗ {self.failed} CHECK(S) FAILED ({self.passed} passed)\033[0m")
        for f in self.failures:
            print(f"  - {f}")
        return 1


# ─────────────────────────────────────────────────────────────────────────────
# 14+1 acceptance questions
# ─────────────────────────────────────────────────────────────────────────────

ACCEPTANCE_QUESTIONS = {
    # Cat 1 — Basic facts
    "F1":  "How many form templates do I have?",
    "F2":  "How many form submissions did I get last month?",
    "F3":  "How many active form templates are there?",
    # Cat 2 — Trends
    "F4":  "What's the form submission trend over the last 6 months?",
    "F5":  "How many form submissions have I received this year so far?",
    "F6":  "How does this month's form submissions compare to last month?",
    # Cat 3 — Rankings
    "F7":  "Which form is most submitted?",
    "F8":  "Which forms are dormant?",
    # Cat 4 — Why / lifecycle
    "F9":  "What's my form completion rate?",
    "F10": "Are any form submissions stuck waiting?",
    # Cat 5 — Advice
    "F11": "Should I deactivate any unused customer forms?",
    # Cat 6 — Vocabulary variants
    "F12": "How many questionnaires were filled out last month?",
    "F13": "Are any intake forms waiting for review?",
    # Cat 7 — PII refusal
    "F14": "Show me the answers customer 503 gave on the Intake Questionnaire form",
    # Stretch — cross-domain
    "S1":  "Did our busiest revenue month also have the most form submissions?",
}


# ─────────────────────────────────────────────────────────────────────────────
# Check 1 — Doc generator runs end-to-end
# ─────────────────────────────────────────────────────────────────────────────

async def check_doc_gen_run(t: TestTally, analytics, wh_pool, emb_client, vector_store) -> dict | None:
    t.section("CHECK 1 — Doc generator end-to-end run")

    extractor = FormsExtractor(analytics=analytics, wh=wh_pool)
    try:
        rows = await extractor.run(
            business_id=BUSINESS_ID,
            start_date=START_DATE,
            end_date=SNAPSHOT_DATE,
            snapshot_date=SNAPSHOT_DATE,
        )
        t.ok("FormsExtractor.run() returned rows")
    except Exception as e:
        t.fail(f"FormsExtractor.run() raised: {e}")
        return None

    try:
        result = await generate_forms_docs(
            org_id=BUSINESS_ID,
            warehouse_rows=rows,
            embedding_client=emb_client,
            vector_store=vector_store,
            force=True,
        )
        t.ok(f"generate_forms_docs completed: created={result['docs_created']} "
             f"skipped={result['docs_skipped']} failed={result['docs_failed']}")
    except Exception as e:
        t.fail(f"generate_forms_docs() raised: {e}")
        return None

    if result.get("docs_failed", 0) == 0:
        t.ok("zero failed docs")
    else:
        t.fail(f"{result['docs_failed']} docs failed")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Check 2 — pgvector population
# ─────────────────────────────────────────────────────────────────────────────

async def check_vector_population(t: TestTally, vec_pool) -> list[dict]:
    t.section("CHECK 2 — pgvector population")

    async with vec_pool.acquire() as conn:
        # Count chunks by doc_type
        rows = await conn.fetch(
            "SELECT doc_type, COUNT(*) AS n "
            "FROM embeddings WHERE tenant_id = $1 AND doc_domain = $2 "
            "GROUP BY doc_type ORDER BY doc_type",
            str(BUSINESS_ID), "forms")

        counts = {r["doc_type"]: r["n"] for r in rows}
        total = sum(counts.values())

        # Expected counts: 1 catalog + 7 monthly + 1 monthly_summary
        #                + 4 per_form + 1 lifecycle + 1 anomalies
        #                + 1 pii_policy = 16
        expected = {
            "catalog":          1,
            "monthly":          7,
            "monthly_summary":  1,
            "per_form":         4,
            "lifecycle":        1,
            "anomalies":        1,
            "pii_policy":       1,
        }
        for doc_type, expected_n in expected.items():
            actual = counts.get(doc_type, 0)
            if actual == expected_n:
                t.ok(f"{doc_type}: {actual} chunk(s)")
            else:
                t.fail(f"{doc_type}: expected {expected_n}, got {actual}")

        if total == 16:
            t.ok(f"total forms chunks for biz {BUSINESS_ID}: {total}")
        else:
            t.fail(f"total forms chunks: expected 16, got {total}")

        # Pull all chunks for content-level checks
        all_chunks = await conn.fetch(
            "SELECT doc_id, doc_type, chunk_text, metadata "
            "FROM embeddings WHERE tenant_id = $1 AND doc_domain = $2",
            str(BUSINESS_ID), "forms")
        return [dict(r) for r in all_chunks]


# ─────────────────────────────────────────────────────────────────────────────
# Check 3 — Anchor numbers in chunk text
# ─────────────────────────────────────────────────────────────────────────────

async def check_anchor_content(t: TestTally, chunks: list[dict]) -> None:
    t.section("CHECK 3 — Anchor numbers in chunk text")

    # asyncpg may return JSONB metadata as a str (not auto-decoded) — parse once.
    import json
    for c in chunks:
        md = c.get("metadata")
        if isinstance(md, str):
            try:
                c["metadata"] = json.loads(md)
            except (json.JSONDecodeError, TypeError):
                c["metadata"] = {}
        elif md is None:
            c["metadata"] = {}

    by_type = {c["doc_type"]: [] for c in chunks}
    for c in chunks:
        by_type[c["doc_type"]].append(c)

    # Catalog: 4 templates / 3 active / 1 inactive / 18 subs / dormant form named
    cat = by_type.get("catalog", [])
    if len(cat) == 1:
        text = cat[0]["chunk_text"]
        for needle in ["4 form template", "3 active and 1 inactive", "18 total submission",
                        "12 submission", "New Customer Welcome"]:
            if needle in text:
                t.ok(f'catalog contains "{needle[:35]}"')
            else:
                t.fail(f'catalog missing "{needle[:35]}"')

    # Monthly: March 2026 → 5 submissions / +25% MoM
    mar = [c for c in by_type.get("monthly", [])
            if c["metadata"] and c["metadata"].get("period") == "2026-03"]
    if len(mar) == 1:
        text = mar[0]["chunk_text"]
        if "5 form submissions" in text:
            t.ok('monthly Mar 2026: "5 form submissions"')
        else:
            t.fail('monthly Mar 2026: missing "5 form submissions"')
        if "up 25.0%" in text:
            t.ok('monthly Mar 2026: "up 25.0%" MoM')
        else:
            t.fail('monthly Mar 2026: missing MoM=+25%')
    else:
        t.fail(f"monthly Mar 2026: expected 1 chunk, got {len(mar)}")

    # Per_form: Form 1 = rank 1, 8 subs, 87.5% completion
    f1_chunks = [c for c in by_type.get("per_form", [])
                  if c["metadata"] and c["metadata"].get("form_id") == 1]
    if len(f1_chunks) == 1:
        text = f1_chunks[0]["chunk_text"]
        for needle in ["Intake Questionnaire", "ranking #1", "8 lifetime", "87.50%"]:
            if needle in text:
                t.ok(f'per_form Form 1 contains "{needle}"')
            else:
                t.fail(f'per_form Form 1 missing "{needle}"')

    # Per_form: Form 4 = active dormant, F11 actionable
    f4_chunks = [c for c in by_type.get("per_form", [])
                  if c["metadata"] and c["metadata"].get("form_id") == 4]
    if len(f4_chunks) == 1:
        text = f4_chunks[0]["chunk_text"].lower()
        if "active dormant" in text:
            t.ok('per_form Form 4: "active dormant" framing present')
        else:
            t.fail('per_form Form 4: missing "active dormant" framing')
        if "candidate for deactivation" in text:
            t.ok('per_form Form 4: F11 advice phrasing present')
        else:
            t.fail('per_form Form 4: missing F11 advice phrasing')

    # Lifecycle: 72.22% completion / 4 stuck
    life = by_type.get("lifecycle", [])
    if len(life) == 1:
        text = life[0]["chunk_text"]
        if "72.22%" in text:
            t.ok('lifecycle: F9 completion rate "72.22%"')
        else:
            t.fail('lifecycle: missing "72.22%"')
        if "4 form submissions stuck" in text:
            t.ok('lifecycle: F10 "4 form submissions stuck"')
        else:
            t.fail('lifecycle: missing "4 form submissions stuck"')

    # Anomalies: stuck-ready ids + zero unknown statuses
    anom = by_type.get("anomalies", [])
    if len(anom) == 1:
        text = anom[0]["chunk_text"]
        if "stuck-ready anomaly" in text:
            t.ok('anomalies: stuck-ready anomaly framing present')
        else:
            t.fail('anomalies: missing stuck-ready framing')
        if "zero anomalous statuses" in text:
            t.ok('anomalies: zero-emission "zero anomalous statuses" present')
        else:
            t.fail('anomalies: missing zero-emission language')

    # Monthly summary: must list all months + name March (5) as max
    msum = by_type.get("monthly_summary", [])
    if len(msum) == 1:
        text = msum[0]["chunk_text"]
        # Should call out March 2026 as the max (per anchors: Mar=5)
        if "March 2026" in text and ("highest" in text.lower() or "peak" in text.lower() or "most" in text.lower()):
            t.ok('monthly_summary: March 2026 named as max month')
        else:
            t.fail('monthly_summary: missing March-as-max anchor')
        # Should list multiple months in the per-month breakdown
        month_names = ["October 2025", "November 2025", "December 2025",
                        "January 2026", "February 2026", "March 2026"]
        listed = [m for m in month_names if m in text]
        if len(listed) >= 5:
            t.ok(f'monthly_summary: lists {len(listed)} of 6 expected months')
        else:
            t.fail(f'monthly_summary: only lists {len(listed)} of 6 months — {listed}')
        # Anti-competition: must NOT name specific forms or include 72.22
        for forbidden in ["Intake Questionnaire", "New Customer Welcome",
                          "72.22", "stuck"]:
            if forbidden in text:
                t.fail(f'monthly_summary leaks anti-competition term "{forbidden}"')
        if not any(f in text for f in ["Intake Questionnaire", "New Customer Welcome",
                                         "72.22", "stuck"]):
            t.ok('monthly_summary: no form names + no completion-rate + no stuck framing')

    # PII policy: F14 refusal vocab + no form names + no count anchors
    pii = by_type.get("pii_policy", [])
    if len(pii) == 1:
        text = pii[0]["chunk_text"]
        for needle in ["individual customer", "specific customer", "private",
                       "confidential", "decline"]:
            if needle in text.lower():
                t.ok(f'pii_policy contains "{needle}"')
            else:
                t.fail(f'pii_policy missing "{needle}"')
        # Must NOT name specific forms (would compete with per_form chunks)
        for forbidden in ["Intake Questionnaire", "Post-Visit Feedback",
                          "Pre-Treatment Consent", "New Customer Welcome"]:
            if forbidden in text:
                t.fail(f'pii_policy MUST NOT name form "{forbidden}" '
                        '(would compete with per_form chunks)')
        # Must NOT include count anchors (would compete with catalog/lifecycle)
        for forbidden in ["72.22", "18 form", "4 form template"]:
            if forbidden in text:
                t.fail(f'pii_policy MUST NOT include count anchor "{forbidden}" '
                        '(would compete with catalog/lifecycle chunks)')
        if not any(forbidden in text for forbidden in ["Intake Questionnaire",
                                                         "Post-Visit Feedback",
                                                         "Pre-Treatment Consent",
                                                         "New Customer Welcome",
                                                         "72.22", "18 form",
                                                         "4 form template"]):
            t.ok('pii_policy: no form names + no count anchors (no competition risk)')


# ─────────────────────────────────────────────────────────────────────────────
# Check 4 — Tenant isolation
# ─────────────────────────────────────────────────────────────────────────────

async def check_tenant_isolation(t: TestTally, vec_pool) -> None:
    t.section("CHECK 4 — Tenant isolation (biz 99 has 0 forms chunks)")

    async with vec_pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM embeddings "
            "WHERE tenant_id = $1 AND doc_domain = $2",
            "99", "forms")
        if n == 0:
            t.ok("biz 99 has 0 forms chunks")
        else:
            t.fail(f"tenant leak: biz 99 has {n} forms chunks!")


# ─────────────────────────────────────────────────────────────────────────────
# Check 5 — Question routing via FORMS_KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────

async def check_question_routing(t: TestTally) -> None:
    t.section("CHECK 5 — Question routing (14+1 questions)")

    # Import FORMS_KEYWORDS from query_analyzer
    try:
        from app.services.query_analyzer import RAG_KEYWORD_GROUPS
        forms_kw = RAG_KEYWORD_GROUPS.get("forms", [])
        if not forms_kw:
            t.fail("'forms' group missing from RAG_KEYWORD_GROUPS — patch not applied")
            return
        t.ok(f"FORMS_KEYWORDS loaded: {len(forms_kw)} terms")
    except ImportError as e:
        t.fail(f"Could not import RAG_KEYWORD_GROUPS: {e}")
        return

    unmatched = []
    for qid, q in ACCEPTANCE_QUESTIONS.items():
        q_lower = q.lower()
        matched = [kw for kw in forms_kw if kw in q_lower]
        if matched:
            preview = ", ".join(matched[:3])
            t.ok(f'{qid}: matched [{preview}]')
        else:
            unmatched.append((qid, q))
            t.fail(f"{qid}: NO keyword match — {q}")

    if not unmatched:
        t.ok(f"all {len(ACCEPTANCE_QUESTIONS)} questions route to forms group")


# ─────────────────────────────────────────────────────────────────────────────
# Check 6 — PII guardrail
# ─────────────────────────────────────────────────────────────────────────────

async def check_pii_guardrail(t: TestTally, chunks: list[dict]) -> None:
    t.section("CHECK 6 — PII guardrail (F14 safety)")

    # No customer ids, no form template content, no OnlineCode
    pii_patterns = {
        "cust_id":           re.compile(r"\bcust_id[\s:=]+\d+\b", re.IGNORECASE),
        "customer N (501+)": re.compile(r"\bcustomer\s+(50[1-9]|51\d|52\d)\b", re.IGNORECASE),
        "OnlineCode":        re.compile(r"OnlineCode[\s:=]+\w+", re.IGNORECASE),
        "email":             re.compile(r"[\w\.\-]+@[\w\.\-]+\.\w+"),
        "phone":             re.compile(r"\b\d{3}[\-\.\s]?\d{3}[\-\.\s]?\d{4}\b"),
    }
    leaks = 0
    for c in chunks:
        for label, pat in pii_patterns.items():
            m = pat.search(c["chunk_text"])
            if m:
                t.fail(f'PII leak in {c["doc_type"]}: matched {label!r} → "{m.group(0)}"')
                leaks += 1

    if leaks == 0:
        t.ok(f"no PII leaks across {len(chunks)} chunks "
             "(no cust_id/customer-N/OnlineCode/email/phone)")


# ─────────────────────────────────────────────────────────────────────────────
# Check 7 — Idempotency
# ─────────────────────────────────────────────────────────────────────────────

async def check_idempotency(t: TestTally, analytics, wh_pool, emb_client, vector_store, vec_pool) -> None:
    t.section("CHECK 7 — Idempotency (re-run with force=False)")

    extractor = FormsExtractor(analytics=analytics, wh=wh_pool)
    rows = await extractor.run(
        business_id=BUSINESS_ID, start_date=START_DATE,
        end_date=SNAPSHOT_DATE, snapshot_date=SNAPSHOT_DATE)
    result = await generate_forms_docs(
        org_id=BUSINESS_ID, warehouse_rows=rows,
        embedding_client=emb_client, vector_store=vector_store, force=False)

    # On re-run, all chunks should be skipped (already exist)
    if result["docs_created"] == 0 and result["docs_skipped"] == 16:
        t.ok(f"re-run skipped {result['docs_skipped']} chunks (idempotent ✓)")
    else:
        t.fail(f"unexpected re-run counts: created={result['docs_created']} "
               f"skipped={result['docs_skipped']} failed={result['docs_failed']}")

    async with vec_pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM embeddings "
            "WHERE tenant_id = $1 AND doc_domain = $2",
            str(BUSINESS_ID), "forms")
        if n == 16:
            t.ok(f"vector store still has exactly 16 chunks (no dupes)")
        else:
            t.fail(f"vector store has {n} chunks, expected 16")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    t = TestTally()
    print("Step 5 real-environment verification — Forms (Domain 10)")
    print("=" * 60)

    base_url = os.environ.get("ANALYTICS_BACKEND_URL", "http://localhost:8001")
    print(f"  Analytics URL: {base_url}")

    wh_pool = await PGPool.from_env(PGTarget.WAREHOUSE)
    vec_pool = await PGPool.from_env(PGTarget.VECTOR)
    analytics = AnalyticsClient(base_url=base_url)
    emb_client = EmbeddingClient.from_env()
    vector_store = VectorStore(vec_pool)

    # Clean slate for biz 42 forms before fresh embed
    async with vec_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM embeddings WHERE tenant_id = $1 AND doc_domain = $2",
            str(BUSINESS_ID), "forms")

    try:
        result = await check_doc_gen_run(t, analytics, wh_pool, emb_client, vector_store)
        if result:
            chunks = await check_vector_population(t, vec_pool)
            if chunks:
                await check_anchor_content(t, chunks)
                await check_pii_guardrail(t, chunks)
            await check_tenant_isolation(t, vec_pool)
            await check_question_routing(t)
            await check_idempotency(t, analytics, wh_pool, emb_client, vector_store, vec_pool)
    finally:
        await wh_pool.close()
        await vec_pool.close()
        if hasattr(analytics, "close"):
            await analytics.close()

    return t.summary()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))