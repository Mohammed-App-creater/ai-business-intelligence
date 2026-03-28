"""
test_vector_store.py — VectorStore unit tests (mocked asyncpg, no real DB).
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.vector_store import DOMAINS, DOC_TYPES, VectorStore

TENANT = "42"
DOC_ID = "42_revenue_monthly_2026_01"
DOC_DOMAIN = "revenue"
DOC_TYPE = "monthly_summary"
TEXT = "Business ID: 42\nMonth: January 2026\nRevenue: $9200"
EMBEDDING = [0.1] * 1536
METADATA = {"location_id": 0}
PERIOD = date(2026, 1, 1)


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    return conn


@pytest.fixture
def mock_pool(mock_conn):
    pool = MagicMock()
    pool.acquire = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return pool


@pytest.fixture
def store(mock_pool):
    return VectorStore(mock_pool)


# ---------------------------------------------------------------------------
# _vec
# ---------------------------------------------------------------------------


def test_vec_returns_string():
    assert isinstance(VectorStore._vec([0.1, 0.2]), str)


def test_vec_starts_with_open_bracket():
    assert VectorStore._vec([0.1]).startswith("[")


def test_vec_ends_with_close_bracket():
    assert VectorStore._vec([0.1]).endswith("]")


def test_vec_correct_element_count():
    result = VectorStore._vec([0.1] * 1536)
    assert len(result.split(",")) == 1536


def test_vec_handles_negative_values():
    assert "-" in VectorStore._vec([-0.5, 0.5])


def test_vec_handles_zero():
    assert "0.00000000" in VectorStore._vec([0.0])


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_returns_uuid_string(store, mock_conn):
    mock_conn.fetchval.return_value = "abc-uuid"
    out = await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    assert out == "abc-uuid"


@pytest.mark.asyncio
async def test_upsert_sql_has_insert_into_embeddings(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "INSERT INTO embeddings" in sql


@pytest.mark.asyncio
async def test_upsert_sql_has_on_conflict(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "ON CONFLICT" in sql


@pytest.mark.asyncio
async def test_upsert_sql_has_do_update_set(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "DO UPDATE SET" in sql


@pytest.mark.asyncio
async def test_upsert_sql_has_returning(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "RETURNING" in sql


@pytest.mark.asyncio
async def test_upsert_first_param_is_tenant_id(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    assert mock_conn.fetchval.call_args[0][1] == TENANT


@pytest.mark.asyncio
async def test_upsert_embedding_serialized_to_string(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    emb_arg = mock_conn.fetchval.call_args[0][6]
    assert isinstance(emb_arg, str)
    assert emb_arg.startswith("[")


@pytest.mark.asyncio
async def test_upsert_none_metadata_becomes_empty_json_object(store, mock_conn):
    mock_conn.fetchval.return_value = "id"
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=None,
    )
    meta_arg = mock_conn.fetchval.call_args[0][8]
    assert meta_arg == "{}"


@pytest.mark.asyncio
async def test_upsert_none_period_start_passes_none(store, mock_conn):
    mock_conn.fetchval.return_value = "id"
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=None, metadata=METADATA,
    )
    period_arg = mock_conn.fetchval.call_args[0][7]
    assert period_arg is None


@pytest.mark.asyncio
async def test_upsert_sql_includes_doc_domain(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "doc_domain" in sql


@pytest.mark.asyncio
async def test_upsert_sql_includes_period_start(store, mock_conn):
    await store.upsert(
        TENANT, DOC_ID, DOC_DOMAIN, DOC_TYPE, TEXT, EMBEDDING,
        period_start=PERIOD, metadata=METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "period_start" in sql


# ---------------------------------------------------------------------------
# upsert_many
# ---------------------------------------------------------------------------


def _doc(**kwargs):
    base = {
        "tenant_id": TENANT,
        "doc_id": DOC_ID,
        "doc_domain": DOC_DOMAIN,
        "doc_type": DOC_TYPE,
        "chunk_text": TEXT,
        "embedding": EMBEDDING,
        "period_start": PERIOD,
        "metadata": METADATA,
    }
    base.update(kwargs)
    return base


@pytest.mark.asyncio
async def test_upsert_many_empty_returns_zero(store):
    assert await store.upsert_many([]) == 0


@pytest.mark.asyncio
async def test_upsert_many_returns_success_count(store, mock_conn):
    mock_conn.fetchval.side_effect = ["a", "b", "c"]
    docs = [_doc(doc_id="1"), _doc(doc_id="2"), _doc(doc_id="3")]
    assert await store.upsert_many(docs) == 3


@pytest.mark.asyncio
async def test_upsert_many_continues_on_single_failure(store, mock_conn):
    mock_conn.fetchval.side_effect = [RuntimeError("x"), "ok"]
    docs = [_doc(doc_id="bad"), _doc(doc_id="good")]
    assert await store.upsert_many(docs) == 1


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_returns_list(store, mock_conn):
    mock_conn.fetch.return_value = []
    out = await store.search(TENANT, EMBEDDING, top_k=5)
    assert isinstance(out, list)


@pytest.mark.asyncio
async def test_search_always_filters_by_tenant_id(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "tenant_id = $1" in sql
    assert mock_conn.fetch.call_args[0][1] == TENANT


@pytest.mark.asyncio
async def test_search_uses_cosine_distance_operator(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "<=>" in sql


@pytest.mark.asyncio
async def test_search_selects_similarity_column(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "similarity" in sql


@pytest.mark.asyncio
async def test_search_with_doc_domain_adds_condition(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5, doc_domain="staff")
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_domain = $" in sql


@pytest.mark.asyncio
async def test_search_with_doc_type_adds_condition(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5, doc_type="ranking")
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_type = $" in sql


@pytest.mark.asyncio
async def test_search_with_since_date_adds_condition(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5, since_date=PERIOD)
    sql = mock_conn.fetch.call_args[0][0]
    assert "period_start >=" in sql


@pytest.mark.asyncio
async def test_search_no_filters_no_extra_conditions(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_domain = $" not in sql
    assert "doc_type = $" not in sql
    assert "period_start >=" not in sql


@pytest.mark.asyncio
async def test_search_top_k_in_params(store, mock_conn):
    await store.search(TENANT, EMBEDDING, top_k=7)
    args = mock_conn.fetch.call_args[0]
    assert 7 in args


@pytest.mark.asyncio
async def test_search_returns_list_of_dicts(store, mock_conn):
    now = datetime.now()
    mock_conn.fetch.return_value = [
        {
            "id": "u",
            "doc_id": DOC_ID,
            "doc_domain": DOC_DOMAIN,
            "doc_type": DOC_TYPE,
            "chunk_text": TEXT,
            "period_start": PERIOD,
            "metadata": {},
            "created_at": now,
            "updated_at": now,
            "similarity": 0.88,
        }
    ]
    out = await store.search(TENANT, EMBEDDING, top_k=5)
    assert isinstance(out[0], dict)
    assert out[0]["similarity"] == 0.88


# ---------------------------------------------------------------------------
# search_multi_domain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_multi_domain_calls_search_once_per_domain(store):
    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.return_value = []
        await store.search_multi_domain(
            TENANT, EMBEDDING, domains=["revenue", "staff", "clients"],
            top_k_per_domain=2,
        )
    assert m.call_count == 3


@pytest.mark.asyncio
async def test_search_multi_domain_combines_all_results(store):
    async def side_effect(*_a, **kwargs):
        d = kwargs.get("doc_domain", "")
        return [{"doc_id": f"{d}_1", "similarity": 0.5}]

    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.side_effect = side_effect
        out = await store.search_multi_domain(
            TENANT, EMBEDDING, domains=["a", "b"],
            top_k_per_domain=3,
        )
    assert len(out) == 2
    ids = {r["doc_id"] for r in out}
    assert ids == {"a_1", "b_1"}


@pytest.mark.asyncio
async def test_search_multi_domain_sorted_by_similarity_desc(store):
    async def side_effect(*_a, **kwargs):
        if kwargs.get("doc_domain") == "low":
            return [{"doc_id": "x", "similarity": 0.2}]
        return [{"doc_id": "y", "similarity": 0.9}]

    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.side_effect = side_effect
        out = await store.search_multi_domain(
            TENANT, EMBEDDING, domains=["low", "high"],
        )
    assert out[0]["similarity"] >= out[1]["similarity"]


@pytest.mark.asyncio
async def test_search_multi_domain_deduplicates_by_doc_id(store):
    async def side_effect(*_a, **kwargs):
        return [{"doc_id": "same", "similarity": 0.3}]

    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.side_effect = side_effect
        out = await store.search_multi_domain(
            TENANT, EMBEDDING, domains=["revenue", "staff"],
        )
    assert len(out) == 1
    assert out[0]["doc_id"] == "same"


# ---------------------------------------------------------------------------
# search_multi_type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_multi_type_calls_search_once_per_type(store):
    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.return_value = []
        await store.search_multi_type(
            TENANT, EMBEDDING, doc_domain="revenue",
            doc_types=["monthly_summary", "daily_trend"],
        )
    assert m.call_count == 2


@pytest.mark.asyncio
async def test_search_multi_type_passes_correct_domain_to_each_call(store):
    seen: list[str] = []

    async def side_effect(*_a, **kwargs):
        seen.append(kwargs.get("doc_domain", ""))
        return []

    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.side_effect = side_effect
        await store.search_multi_type(
            TENANT, EMBEDDING, doc_domain="revenue",
            doc_types=["a", "b"],
        )
    assert seen == ["revenue", "revenue"]


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists_true_when_fetchval_returns_true(store, mock_conn):
    mock_conn.fetchval.return_value = True
    assert await store.exists(TENANT, DOC_ID) is True


@pytest.mark.asyncio
async def test_exists_false_when_fetchval_returns_false(store, mock_conn):
    mock_conn.fetchval.return_value = False
    assert await store.exists(TENANT, DOC_ID) is False


@pytest.mark.asyncio
async def test_exists_sql_has_exists_keyword(store, mock_conn):
    await store.exists(TENANT, DOC_ID)
    sql = mock_conn.fetchval.call_args[0][0]
    assert "EXISTS" in sql


@pytest.mark.asyncio
async def test_exists_passes_both_tenant_and_doc_id(store, mock_conn):
    await store.exists(TENANT, DOC_ID)
    args = mock_conn.fetchval.call_args[0]
    assert TENANT in args and DOC_ID in args


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_true_when_row_returned(store, mock_conn):
    mock_conn.fetchrow.return_value = {"id": "x"}
    assert await store.delete(TENANT, DOC_ID) is True


@pytest.mark.asyncio
async def test_delete_false_when_fetchrow_none(store, mock_conn):
    mock_conn.fetchrow.return_value = None
    assert await store.delete(TENANT, DOC_ID) is False


@pytest.mark.asyncio
async def test_delete_sql_has_returning_clause(store, mock_conn):
    await store.delete(TENANT, DOC_ID)
    assert "RETURNING" in mock_conn.fetchrow.call_args[0][0]


# ---------------------------------------------------------------------------
# delete_by_domain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_by_domain_returns_count(store, mock_conn):
    mock_conn.fetch.return_value = [{"id": 1}, {"id": 2}]
    assert await store.delete_by_domain(TENANT, DOC_DOMAIN) == 2


@pytest.mark.asyncio
async def test_delete_by_domain_sql_filters_doc_domain(store, mock_conn):
    await store.delete_by_domain(TENANT, DOC_DOMAIN)
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_domain" in sql


# ---------------------------------------------------------------------------
# delete_by_tenant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_by_tenant_returns_count(store, mock_conn):
    mock_conn.fetch.return_value = [{"id": "a"}]
    assert await store.delete_by_tenant(TENANT) == 1


@pytest.mark.asyncio
async def test_delete_by_tenant_sql_filters_only_tenant(store, mock_conn):
    await store.delete_by_tenant(TENANT)
    sql = mock_conn.fetch.call_args[0][0]
    assert "DELETE FROM embeddings" in sql
    assert "tenant_id" in sql
    assert "doc_domain" not in sql


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_count_returns_integer(store, mock_conn):
    mock_conn.fetchval.return_value = 12
    assert await store.count(TENANT) == 12


@pytest.mark.asyncio
async def test_count_none_fetchval_returns_zero(store, mock_conn):
    mock_conn.fetchval.return_value = None
    assert await store.count(TENANT) == 0


@pytest.mark.asyncio
async def test_count_with_domain_adds_filter(store, mock_conn):
    mock_conn.fetchval.return_value = 1
    await store.count(TENANT, doc_domain="staff")
    sql = mock_conn.fetchval.call_args[0][0]
    assert "doc_domain = $" in sql


@pytest.mark.asyncio
async def test_count_with_type_adds_filter(store, mock_conn):
    mock_conn.fetchval.return_value = 1
    await store.count(TENANT, doc_type="ranking")
    sql = mock_conn.fetchval.call_args[0][0]
    assert "doc_type = $" in sql


@pytest.mark.asyncio
async def test_count_no_filters_clean_sql(store, mock_conn):
    mock_conn.fetchval.return_value = 0
    await store.count(TENANT)
    sql = mock_conn.fetchval.call_args[0][0]
    assert "doc_domain = $" not in sql
    assert "doc_type = $" not in sql


# ---------------------------------------------------------------------------
# get_doc_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_doc_ids_returns_list_of_strings(store, mock_conn):
    mock_conn.fetch.return_value = [{"doc_id": "a"}, {"doc_id": "b"}]
    assert await store.get_doc_ids(TENANT) == ["a", "b"]


@pytest.mark.asyncio
async def test_get_doc_ids_with_domain_filter_adds_condition(store, mock_conn):
    mock_conn.fetch.return_value = []
    await store.get_doc_ids(TENANT, doc_domain="revenue")
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_domain = $" in sql


@pytest.mark.asyncio
async def test_get_doc_ids_no_filter_clean_sql(store, mock_conn):
    mock_conn.fetch.return_value = []
    await store.get_doc_ids(TENANT)
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_domain = $" not in sql
    assert "doc_type = $" not in sql


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------


def test_domains_is_frozenset():
    assert isinstance(DOMAINS, frozenset)


def test_doc_types_is_frozenset():
    assert isinstance(DOC_TYPES, frozenset)


def test_domains_contains_revenue():
    assert "revenue" in DOMAINS


def test_domains_contains_staff():
    assert "staff" in DOMAINS


def test_doc_types_contains_monthly_summary():
    assert "monthly_summary" in DOC_TYPES


# ---------------------------------------------------------------------------
# initialize_schema / from_pool
# ---------------------------------------------------------------------------


def test_from_pool(mock_pool):
    s = VectorStore.from_pool(mock_pool)
    assert isinstance(s, VectorStore)
    assert s._pool is mock_pool


@pytest.mark.asyncio
async def test_initialize_schema_executes_statements(store, mock_conn):
    await store.initialize_schema()
    assert mock_conn.execute.called
    assert mock_conn.execute.call_count >= 5
