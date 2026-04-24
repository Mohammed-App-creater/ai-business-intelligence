"""
scripts/debug_analyzer_matches.py
==================================
Print what the query analyzer matches for every failing question, plus
which doc_domains the retriever resolves those matches to.

This tells us exactly where the multi-domain bleed is happening:
  - Which keywords fire for each question
  - Which keyword groups own those keywords
  - Which doc_domains the retriever maps those groups to
  - Whether unrelated domains (e.g., appointments) are sneaking in

Usage:
    PYTHONPATH=. python scripts/debug_analyzer_matches.py
"""

from __future__ import annotations

import asyncio
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


FAILING_QUESTIONS = [
    ("Q1",  "What were my total expenses last month?"),
    ("Q2",  "How much have I spent this year so far?"),
    ("Q3",  "What's my average monthly spending over the last 6 months?"),
    ("Q4",  "How many expense transactions did I record last month?"),
    ("Q12", "Which expense category grew the most compared to last month?"),
    ("Q15", "Which payment method do I use most often for business bills?"),
    ("Q16", "Which branch costs more to run — Main St or Westside?"),
    ("Q21", "My costs feel higher this quarter — what's driving that?"),
    ("Q23", "Where can I cut costs without hurting my business?"),
]


# For reference, also include 3 PASSING questions so we can see the
# correct routing pattern as a control group.
PASSING_QUESTIONS = [
    ("Q7",  "Which month had my highest spending in the last 6 months?"),
    ("Q10", "How much did I spend on Products vs Rent last month?"),
    ("Q22", "I had an unusually expensive month in February — which category spiked?"),
]


async def main():
    # Import here so dotenv runs first
    from app.services.query_analyzer import QueryAnalyzer, RAG_KEYWORD_GROUPS
    from app.services.retriever import KEYWORD_GROUP_TO_DOMAINS

    analyzer = QueryAnalyzer()

    # Build reverse map: keyword → group (first match wins for display)
    keyword_to_groups: dict[str, list[str]] = {}
    for group_name, keywords in RAG_KEYWORD_GROUPS.items():
        for kw in keywords:
            keyword_to_groups.setdefault(kw.lower(), []).append(group_name)

    def _resolve(matched_keywords: set[str]) -> dict:
        """Mirror retriever._resolve_domains logic."""
        matched_groups: set[str] = set()
        for group_name, keywords in RAG_KEYWORD_GROUPS.items():
            if matched_keywords & set(k.lower() for k in keywords):
                matched_groups.add(group_name)

        domains: list[str] = []
        has_broad = False

        for group in matched_groups:
            group_domains = KEYWORD_GROUP_TO_DOMAINS.get(group)
            if group_domains is None:
                has_broad = True
            else:
                domains.extend(group_domains)

        seen = set()
        unique_domains = []
        for d in domains:
            if d not in seen:
                seen.add(d)
                unique_domains.append(d)

        if not unique_domains and has_broad:
            final = "BROAD (all domains)"
        elif not unique_domains:
            final = "BROAD (fallback)"
        else:
            final = unique_domains

        return {
            "matched_groups": sorted(matched_groups),
            "has_broad_group": has_broad,
            "resolved_domains": final,
        }

    def _search_branch(domains) -> str:
        """Mirror retriever._search branch selection."""
        if not isinstance(domains, list):
            return "broad (0 domains or string fallback)"
        if len(domains) == 0:
            return "broad search, top_k=10"
        if len(domains) == 1:
            if domains[0] == "staff":    return "1-domain: staff, top_k=12"
            if domains[0] == "services": return "1-domain: services, top_k=10"
            if domains[0] == "expenses": return "1-domain: expenses, top_k=15"
            return f"1-domain: {domains[0]}, top_k=5"
        if len(domains) > 3:
            return "broad search, top_k=10"
        return f"multi-domain: {len(domains)} domains, top_k_per_domain=3 → total {3*len(domains)} chunks"

    async def diagnose(qid, question):
        result = await analyzer.analyze(question)
        matched_kws_raw = getattr(result, "matched_keywords", set())
        matched_kws = set(kw.lower() for kw in matched_kws_raw)

        resolved = _resolve(matched_kws)
        branch = _search_branch(resolved["resolved_domains"])

        print(f"\n{'─' * 78}")
        print(f"  [{qid}] {question}")
        print(f"{'─' * 78}")
        print(f"  analyzer.route        : {getattr(result, 'route', '?')}")
        print(f"  analyzer.confidence   : {getattr(result, 'confidence', 0):.2f}")
        print(f"  matched_keywords ({len(matched_kws)}):")
        for kw in sorted(matched_kws)[:15]:
            groups = keyword_to_groups.get(kw, ["<unknown>"])
            print(f"    - {kw!r:<30s} → group(s): {groups}")
        if len(matched_kws) > 15:
            print(f"    ... ({len(matched_kws) - 15} more)")
        print(f"  matched_groups        : {resolved['matched_groups']}")
        print(f"  resolved_domains      : {resolved['resolved_domains']}")
        print(f"  retriever branch      : {branch}")

        # Flag multi-domain pollution
        resolved_doms = resolved["resolved_domains"]
        if isinstance(resolved_doms, list) and len(resolved_doms) > 1:
            non_exp = [d for d in resolved_doms if d != "expenses"]
            if "expenses" in resolved_doms and non_exp:
                print(f"  ⚠️  POLLUTION: expense question also matched {non_exp}")
                print(f"      top_k_per_domain=3 → only 3 expense chunks reach LLM")

    print("=" * 78)
    print("  FAILING QUESTIONS (expect multi-domain pollution)")
    print("=" * 78)
    for qid, q in FAILING_QUESTIONS:
        await diagnose(qid, q)

    print("\n")
    print("=" * 78)
    print("  CONTROL GROUP — PASSING QUESTIONS")
    print("=" * 78)
    for qid, q in PASSING_QUESTIONS:
        await diagnose(qid, q)

    print()
    print("=" * 78)
    print("  Interpretation key")
    print("=" * 78)
    print("""
  If failing questions show multiple non-expense domains in resolved_domains:
    → Keyword overlap — expense vocabulary appears in other groups' lists
    → Fix: scrub ambiguous keywords from non-expense groups,
           OR add domain priority (if 'expenses' matches, drop others)

  If failing questions show ONLY ["expenses"] but retrieval still fails:
    → top_k=15 isn't enough, or chunks are there but low cosine similarity
    → Fix: higher top_k, query rewriting, or doc_type guarantees

  If failing questions resolve to BROAD (all domains):
    → Expense keywords aren't matching at all
    → Fix: add missing vocabulary to RAG_KEYWORD_GROUPS['expenses']
""")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()) or 0)