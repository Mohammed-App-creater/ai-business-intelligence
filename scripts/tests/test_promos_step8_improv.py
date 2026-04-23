"""
scripts/tests/test_promos_step8_improv.py
==========================================
Step 8 Phase 2 — Improvised Probes

Fires 10 unplanned questions against the live chat endpoint. These probe
edge cases the Step 6 assertions don't cover: vocabulary variants,
boundary cases with Marketing, negative framings, specific-code queries,
and unusual phrasings.

Scoring is LOOSE — just logs answer + route + sources + latency so you
can eyeball them. No hard pass/fail. The goal is to spot surprising
behavior before Step 8 sign-off.

Usage:
    PYTHONPATH=. python scripts/tests/test_promos_step8_improv.py
    PYTHONPATH=. python scripts/tests/test_promos_step8_improv.py --org-id 42
    PYTHONPATH=. python scripts/tests/test_promos_step8_improv.py \
        --output results/step8_improv.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import httpx

CHAT_ENDPOINT = "http://localhost:8000/api/v1/chat"
ORG_ID = 42
REQUEST_TIMEOUT = 60.0

# ─── 10 improvised probes ──────────────────────────────────────────────────

PROBES = [
    # 1. Vocabulary variant — "coupon" instead of "promo"
    {
        "id": "IMP1",
        "text": "How many coupons did we hand out last month?",
        "category": "Vocabulary variant",
        "note": "Tests 'coupon' synonym routes to promos, not marketing",
    },

    # 2. Vocabulary variant — "savings" framing
    {
        "id": "IMP2",
        "text": "How much did customers save through discounts last month?",
        "category": "Vocabulary variant",
        "note": "Tests customer-benefit framing vs cost-to-business framing",
    },

    # 3. Specific code query
    {
        "id": "IMP3",
        "text": "Tell me everything you know about DM8880.",
        "category": "Specific code",
        "note": "Should pull catalog_health + code_window + code_monthly for DM8880",
    },

    # 4. Negative question
    {
        "id": "IMP4",
        "text": "Which promo codes had zero redemptions in the last 3 months?",
        "category": "Negative question",
        "note": "Should identify DM881 (dormant). Tests negative filtering.",
    },

    # 5. Marketing/Promos boundary
    {
        "id": "IMP5",
        "text": "Did my Welcome Series email campaign's promo code perform well?",
        "category": "Domain boundary",
        "note": "WELCOME10 is in Marketing domain, not Promos. "
                "Should route to marketing, not promos.",
    },

    # 6. Ambiguous time
    {
        "id": "IMP6",
        "text": "What's my promo redemption rate?",
        "category": "Ambiguous time",
        "note": "No time window specified. Should pick sensible default "
                "(likely the full window) — not refuse.",
    },

    # 7. Compound question
    {
        "id": "IMP7",
        "text": "How do promo redemptions compare between Main St and Westside, "
                "and which codes drive the difference?",
        "category": "Compound",
        "note": "Requires per-location rollup AND per-code-per-location "
                "retrieval. Stresses the retriever.",
    },

    # 8. Business-advice framing (not raw data)
    {
        "id": "IMP8",
        "text": "Are my discount percentages too high?",
        "category": "Advice",
        "note": "Subjective. Should pull data to ground the answer; "
                "compare avg discounts across codes.",
    },

    # 9. Impossible question (test honesty)
    {
        "id": "IMP9",
        "text": "How did promo redemptions perform in 2023?",
        "category": "Out of window",
        "note": "Window starts Nov 2025. AI should honestly say no data "
                "for 2023 — NOT hallucinate numbers.",
    },

    # 10. Data-integrity framing
    {
        "id": "IMP10",
        "text": "Is there anything weird about my promo data I should know?",
        "category": "Data integrity",
        "note": "Should surface: dormant DM881, active-but-expired POFL99, "
                "orphan promo_id=999. Tests comprehensive catalog-health.",
    },
]


async def ask(client: httpx.AsyncClient, question: str, org_id: int) -> dict:
    payload = {
        "business_id": str(org_id),
        "org_id":      str(org_id),
        "question":    question,
        "session_id":  str(uuid.uuid4()),
    }
    try:
        resp = await client.post(CHAT_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        return {"answer": f"HTTP {resp.status_code}", "route": None, "sources": []}
    except Exception as e:
        return {"answer": f"Error: {e!r}", "route": None, "sources": []}


async def main(args):
    print(f"── Improvised Probes ({len(PROBES)}) — org {args.org_id} ──")
    print()

    results = []
    async with httpx.AsyncClient() as client:
        for p in PROBES:
            print(f"┌─ [{p['id']}] {p['category']}")
            print(f"│  Q: {p['text']}")
            print(f"│  Note: {p['note']}")

            t0 = time.time()
            res = await ask(client, p["text"], args.org_id)
            elapsed = (time.time() - t0) * 1000

            answer = (res.get("answer") or "").strip().replace("\n", " ")
            preview = answer[:280] + ("..." if len(answer) > 280 else "")
            route = res.get("route") or "?"
            conf = res.get("confidence")
            sources = res.get("sources") or []

            print(f"│  Route: {route}  conf={conf}  latency={elapsed:.0f}ms")
            print(f"│  Sources: {len(sources)} doc(s)")
            if sources[:3]:
                for s in sources[:3]:
                    print(f"│    - {s}")
            print(f"│  A: {preview}")
            print(f"└─")
            print()

            results.append({
                "id": p["id"],
                "category": p["category"],
                "question": p["text"],
                "answer": answer,
                "route": route,
                "confidence": conf,
                "sources": sources,
                "latency_ms": elapsed,
            })

    # Summary
    print("═" * 78)
    print(f"  Improv probes complete — {len(results)} fired")
    print("═" * 78)
    print(f"  Eyeball these for:")
    print(f"    • Surprising refusals on data we KNOW exists")
    print(f"    • Numbers that don't match the fixture story")
    print(f"    • Leaks across domain (promos↔marketing) on boundary questions")
    print(f"    • Hallucinated numbers on out-of-window questions (IMP9)")
    print(f"    • Successful multi-hop retrieval (IMP7, IMP10)")
    print("═" * 78)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({
            "endpoint": CHAT_ENDPOINT,
            "org_id": args.org_id,
            "probes": results,
        }, indent=2, ensure_ascii=False))
        print(f"\n  Results → {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--org-id", type=int, default=ORG_ID)
    p.add_argument("--output", help="Save results as JSON")
    p.add_argument("--endpoint", default=CHAT_ENDPOINT)
    args = p.parse_args()

    CHAT_ENDPOINT = args.endpoint
    asyncio.run(main(args))