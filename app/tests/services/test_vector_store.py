"""
test_vector_store.py
====================
Unit tests for VectorStore — mocked pool / connection, no real DB.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.vector_store import VectorStore


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


SAMPLE_TENANT = "42"
SAMPLE_DOC_ID = "42_monthly_2026_01"
SAMPLE_DOC_TYPE = "monthly_summary"
SAMPLE_TEXT = "Business ID: 42\nMonth: January 2026\nRevenue: $9200"
SAMPLE_EMBEDDING = [0.1] * 1536
SAMPLE_METADATA = {"period_start": "2026-01-01"}


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_calls_fetchval(store, mock_conn):
    mock_conn.fetchval.return_value = "some-uuid"
    result = await store.upsert(
        SAMPLE_TENANT,
        SAMPLE_DOC_ID,
        SAMPLE_DOC_TYPE,
        SAMPLE_TEXT,
        SAMPLE_EMBEDDING,
        SAMPLE_METADATA,
    )
    assert result == "some-uuid"
    assert mock_conn.fetchval.called


@pytest.mark.asyncio
async def test_upsert_sql_contains_on_conflict(store, mock_conn):
    await store.upsert(
        SAMPLE_TENANT,
        SAMPLE_DOC_ID,
        SAMPLE_DOC_TYPE,
        SAMPLE_TEXT,
        SAMPLE_EMBEDDING,
        SAMPLE_METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "ON CONFLICT" in sql


@pytest.mark.asyncio
async def test_upsert_sql_contains_insert_into_embeddings(store, mock_conn):
    await store.upsert(
        SAMPLE_TENANT,
        SAMPLE_DOC_ID,
        SAMPLE_DOC_TYPE,
        SAMPLE_TEXT,
        SAMPLE_EMBEDDING,
        SAMPLE_METADATA,
    )
    sql = mock_conn.fetchval.call_args[0][0]
    assert "INSERT INTO embeddings" in sql


@pytest.mark.asyncio
async def test_upsert_passes_tenant_id_as_first_param(store, mock_conn):
    await store.upsert(
        SAMPLE_TENANT,
        SAMPLE_DOC_ID,
        SAMPLE_DOC_TYPE,
        SAMPLE_TEXT,
        SAMPLE_EMBEDDING,
        SAMPLE_METADATA,
    )
    args = mock_conn.fetchval.call_args[0]
    assert args[1] == SAMPLE_TENANT


@pytest.mark.asyncio
async def test_upsert_serializes_embedding_to_vector_string(store, mock_conn):
    await store.upsert(
        SAMPLE_TENANT,
        SAMPLE_DOC_ID,
        SAMPLE_DOC_TYPE,
        SAMPLE_TEXT,
        SAMPLE_EMBEDDING,
        SAMPLE_METADATA,
    )
    args = mock_conn.fetchval.call_args[0]
    embedding_param = args[5]
    assert isinstance(embedding_param, str)
    assert embedding_param.startswith("[")


@pytest.mark.asyncio
async def test_upsert_none_metadata_defaults_to_empty_json(store, mock_conn):
    mock_conn.fetchval.return_value = "uuid"
    await store.upsert(
        SAMPLE_TENANT,
        SAMPLE_DOC_ID,
        SAMPLE_DOC_TYPE,
        SAMPLE_TEXT,
        SAMPLE_EMBEDDING,
        metadata=None,
    )
    args = mock_conn.fetchval.call_args[0]
    meta_param = args[6]
    assert meta_param == "{}"


# ---------------------------------------------------------------------------
# upsert_many
# ---------------------------------------------------------------------------


def _doc(**kwargs):
    base = {
        "tenant_id": SAMPLE_TENANT,
        "doc_id": SAMPLE_DOC_ID,
        "doc_type": SAMPLE_DOC_TYPE,
        "chunk_text": SAMPLE_TEXT,
        "embedding": SAMPLE_EMBEDDING,
        "metadata": SAMPLE_METADATA,
    }
    base.update(kwargs)
    return base


@pytest.mark.asyncio
async def test_upsert_many_returns_count_of_documents(store, mock_conn):
    mock_conn.fetchval.side_effect = ["u1", "u2", "u3"]
    docs = [_doc(doc_id="a"), _doc(doc_id="b"), _doc(doc_id="c")]
    n = await store.upsert_many(docs)
    assert n == 3


@pytest.mark.asyncio
async def test_upsert_many_empty_list_returns_zero(store):
    assert await store.upsert_many([]) == 0


@pytest.mark.asyncio
async def test_upsert_many_continues_on_single_doc_failure(store, mock_conn):
    mock_conn.fetchval.side_effect = [RuntimeError("boom"), "ok-uuid"]
    docs = [_doc(doc_id="bad"), _doc(doc_id="good")]
    n = await store.upsert_many(docs)
    assert n == 1


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_returns_list(store, mock_conn):
    mock_conn.fetch.return_value = []
    result = await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, top_k=5)
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_search_filters_by_tenant_id(store, mock_conn):
    await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "$1" in sql
    assert mock_conn.fetch.call_args[0][1] == SAMPLE_TENANT


@pytest.mark.asyncio
async def test_search_sql_contains_cosine_operator(store, mock_conn):
    await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "<=>" in sql


@pytest.mark.asyncio
async def test_search_includes_similarity_in_select(store, mock_conn):
    await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, top_k=5)
    sql = mock_conn.fetch.call_args[0][0]
    assert "similarity" in sql or "1 -" in sql


@pytest.mark.asyncio
async def test_search_with_doc_type_filter_adds_where_clause(store, mock_conn):
    await store.search(
        SAMPLE_TENANT,
        SAMPLE_EMBEDDING,
        doc_type="monthly_summary",
    )
    sql = mock_conn.fetch.call_args[0][0]
    assert "AND doc_type" in sql


@pytest.mark.asyncio
async def test_search_without_doc_type_no_type_filter(store, mock_conn):
    await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, doc_type=None)
    sql = mock_conn.fetch.call_args[0][0]
    assert "AND doc_type" not in sql


@pytest.mark.asyncio
async def test_search_passes_top_k_as_limit_param(store, mock_conn):
    await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, top_k=7)
    args = mock_conn.fetch.call_args[0]
    assert 7 in args


@pytest.mark.asyncio
async def test_search_converts_records_to_dicts(store, mock_conn):
    now = datetime.now()
    row = {
        "id": "uuid",
        "doc_id": "x",
        "doc_type": "y",
        "chunk_text": "z",
        "metadata": {},
        "similarity": 0.9,
        "created_at": now,
        "updated_at": now,
    }
    mock_conn.fetch.return_value = [row]
    result = await store.search(SAMPLE_TENANT, SAMPLE_EMBEDDING, top_k=5)
    assert isinstance(result[0], dict)
    assert result[0]["similarity"] == 0.9


# ---------------------------------------------------------------------------
# search_multi_type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_multi_type_returns_combined_results(store):
    async def fake_search(*_args, **kwargs):
        dt = kwargs["doc_type"]
        return [
            {"doc_type": dt, "similarity": 0.5, "doc_id": f"{dt}-1"},
            {"doc_type": dt, "similarity": 0.4, "doc_id": f"{dt}-2"},
        ]

    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.side_effect = fake_search
        out = await store.search_multi_type(
            SAMPLE_TENANT,
            SAMPLE_EMBEDDING,
            doc_types=["monthly_summary", "staff_summary"],
            top_k_per_type=3,
        )
    assert len(out) == 4


@pytest.mark.asyncio
async def test_search_multi_type_sorted_by_similarity_desc(store):
    async def fake_search(*_args, **kwargs):
        doc_type = kwargs["doc_type"]
        if doc_type == "monthly_summary":
            return [{"similarity": 0.3, "doc_id": "m1"}]
        return [{"similarity": 0.9, "doc_id": "s1"}]

    with patch.object(VectorStore, "search", new_callable=AsyncMock) as m:
        m.side_effect = fake_search
        out = await store.search_multi_type(
            SAMPLE_TENANT,
            SAMPLE_EMBEDDING,
            doc_types=["monthly_summary", "staff_summary"],
        )
    assert out[0]["similarity"] >= out[1]["similarity"]


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exists_returns_true_when_found(store, mock_conn):
    mock_conn.fetchval.return_value = True
    assert await store.exists(SAMPLE_TENANT, SAMPLE_DOC_ID) is True


@pytest.mark.asyncio
async def test_exists_returns_false_when_not_found(store, mock_conn):
    mock_conn.fetchval.return_value = False
    assert await store.exists(SAMPLE_TENANT, SAMPLE_DOC_ID) is False


@pytest.mark.asyncio
async def test_exists_sql_contains_exists_keyword(store, mock_conn):
    await store.exists(SAMPLE_TENANT, SAMPLE_DOC_ID)
    sql = mock_conn.fetchval.call_args[0][0]
    assert "EXISTS" in sql


@pytest.mark.asyncio
async def test_exists_filters_by_tenant_and_doc_id(store, mock_conn):
    await store.exists(SAMPLE_TENANT, SAMPLE_DOC_ID)
    args = mock_conn.fetchval.call_args[0]
    assert SAMPLE_TENANT in args
    assert SAMPLE_DOC_ID in args


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_returns_true_when_row_deleted(store, mock_conn):
    mock_conn.fetchrow.return_value = {"id": "uuid"}
    assert await store.delete(SAMPLE_TENANT, SAMPLE_DOC_ID) is True


@pytest.mark.asyncio
async def test_delete_returns_false_when_not_found(store, mock_conn):
    mock_conn.fetchrow.return_value = None
    assert await store.delete(SAMPLE_TENANT, SAMPLE_DOC_ID) is False


@pytest.mark.asyncio
async def test_delete_sql_has_returning_clause(store, mock_conn):
    await store.delete(SAMPLE_TENANT, SAMPLE_DOC_ID)
    sql = mock_conn.fetchrow.call_args[0][0]
    assert "RETURNING" in sql


# ---------------------------------------------------------------------------
# delete_by_tenant
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_by_tenant_returns_count(store, mock_conn):
    mock_conn.fetch.return_value = [{"id": 1}, {"id": 2}]
    assert await store.delete_by_tenant(SAMPLE_TENANT) == 2


@pytest.mark.asyncio
async def test_delete_by_tenant_sql_deletes_all_for_tenant(store, mock_conn):
    await store.delete_by_tenant(SAMPLE_TENANT)
    sql = mock_conn.fetch.call_args[0][0]
    assert "DELETE FROM embeddings" in sql
    assert "tenant_id" in sql


# ---------------------------------------------------------------------------
# delete_by_type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_by_type_returns_count(store, mock_conn):
    mock_conn.fetch.return_value = [{"id": "a"}]
    assert await store.delete_by_type(SAMPLE_TENANT, SAMPLE_DOC_TYPE) == 1


@pytest.mark.asyncio
async def test_delete_by_type_sql_filters_tenant_and_type(store, mock_conn):
    await store.delete_by_type(SAMPLE_TENANT, SAMPLE_DOC_TYPE)
    sql = mock_conn.fetch.call_args[0][0]
    assert "DELETE FROM embeddings" in sql
    assert "doc_type" in sql


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_count_returns_integer(store, mock_conn):
    mock_conn.fetchval.return_value = 5
    assert await store.count(SAMPLE_TENANT) == 5


@pytest.mark.asyncio
async def test_count_with_doc_type_adds_filter(store, mock_conn):
    mock_conn.fetchval.return_value = 1
    await store.count(SAMPLE_TENANT, doc_type="monthly_summary")
    sql = mock_conn.fetchval.call_args[0][0]
    assert "doc_type" in sql


@pytest.mark.asyncio
async def test_count_without_doc_type_no_type_filter(store, mock_conn):
    mock_conn.fetchval.return_value = 0
    await store.count(SAMPLE_TENANT, doc_type=None)
    sql = mock_conn.fetchval.call_args[0][0]
    assert "doc_type" not in sql


# ---------------------------------------------------------------------------
# _vec
# ---------------------------------------------------------------------------


def test_vec_returns_string():
    result = VectorStore._vec([0.1, 0.2, 0.3])
    assert isinstance(result, str)


def test_vec_starts_and_ends_with_brackets():
    result = VectorStore._vec([0.1, 0.2, 0.3])
    assert result.startswith("[") and result.endswith("]")


def test_vec_correct_length():
    result = VectorStore._vec([0.1] * 1536)
    assert len(result.split(",")) == 1536


def test_vec_handles_negative_values():
    result = VectorStore._vec([-0.5, 0.5])
    assert "-" in result


# ---------------------------------------------------------------------------
# get_doc_ids
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_doc_ids_returns_list_of_strings(store, mock_conn):
    mock_conn.fetch.return_value = [{"doc_id": "a"}, {"doc_id": "b"}]
    assert await store.get_doc_ids(SAMPLE_TENANT) == ["a", "b"]


@pytest.mark.asyncio
async def test_get_doc_ids_with_doc_type_adds_filter(store, mock_conn):
    mock_conn.fetch.return_value = []
    await store.get_doc_ids(SAMPLE_TENANT, doc_type="monthly_summary")
    sql = mock_conn.fetch.call_args[0][0]
    assert "doc_type" in sql


# ---------------------------------------------------------------------------
# from_pool / initialize_schema
# ---------------------------------------------------------------------------


def test_from_pool_returns_instance(mock_pool):
    s = VectorStore.from_pool(mock_pool)
    assert isinstance(s, VectorStore)
    assert s._pool is mock_pool


@pytest.mark.asyncio
async def test_initialize_schema_executes_statements_from_repo_file(store, mock_conn):
    await store.initialize_schema()
    assert mock_conn.execute.called
    assert mock_conn.execute.call_count >= 4


</think>
<think>
I made a mess of the initialize_schema tests - I should simplify. The user didn't require initialize_schema tests in the list. Let me remove the broken/complex tests and add one clean test that patches Path or uses the real file.

Actually the simplest test for initialize_schema:
1. Patch `Path.read_text` to return `"SELECT 1;"` 
2. Or use the real repo's init_db.sql and count execute calls

Let me read what I wrote for test_initialize_schema - it's broken. I'll replace with:

```python
@pytest.mark.asyncio
async def test_initialize_schema_executes_split_statements(store, mock_conn, monkeypatch):
    from app.services import vector_store as vs
    sample_sql = "CREATE EXTENSION IF NOT EXISTS vector;\nSELECT 1;\n"
    monkeypatch.setattr(
        vs.Path,
        "read_text",
        lambda self, encoding="utf-8": sample_sql if "init_db.sql" in str(self) else Path(self).read_text(encoding=encoding),
    )
```

That's fragile. Better approach:

```python
@pytest.mark.asyncio
async def test_initialize_schema_calls_execute_per_statement(store, mock_conn, monkeypatch):
    from app.services import vector_store as vs_mod
    fake_path = MagicMock()
    fake_path.read_text = MagicMock(return_value="SELECT 1;\nSELECT 2;\n")
    monkeypatch.setattr(vs_mod.Path, "__new__", lambda cls, *a, **k: fake_path)
```

Path is tricky to mock.

Simplest: patch `Path.read_text` on the instance returned by the path construction:

```python
@pytest.mark.asyncio
async def test_initialize_schema(store, mock_conn, monkeypatch):
    from pathlib import Path
    from app.services import vector_store as vs

    def fake_read_text(self, encoding="utf-8"):
        return "SELECT 1;\nSELECT 2;\n"

    monkeypatch.setattr(Path, "read_text", fake_read_text)
    await store.initialize_schema()
```

That would break ALL Path.read_text calls globally in that test - might affect other things. Scope to the method:

```python
read_sql = "SELECT 1;\nSELECT 2;\n"

async def fake_init(self):
    parts = [p.strip() for p in read_sql.split(";") if p.strip()]
    async with self._pool.acquire() as conn:
        for stmt in parts:
            await conn.execute(stmt)

# doesn't test real method
```

Best: patch only `vector_store.Path` - replace the path class's behavior when dividing:

Actually the cleanest test:
```python
@pytest.mark.asyncio
async def test_initialize_schema_executes_non_empty_statements(store, mock_conn, monkeypatch):
    from app.services import vector_store as vs_mod
    content = "FOO;\n\nBAR  ;\n"
    monkeypatch.setattr(
        vs_mod,
        "Path",
        MagicMock(return_value=MagicMock(read_text=MagicMock(return_value=content))),
    )
```

Wait, `Path(__file__).parent...` - we need the mock chain:
`Path.return_value.parent.parent.parent.__truediv__` ...

Too heavy. Use patch.object on VectorStore.initialize_schema to inject sql_text - no that's not testing implementation.

I'll use `unittest.mock.patch` with `pathlib.Path` and a wrapper:

```python
real_path = pathlib.Path
class PathSubclass(real_path):
    def read_text(self, encoding=None, errors=None):
        if self.name == "init_db.sql":
            return "SELECT 1;\nSELECT 2;\n"
        return real_path.read_text(self, encoding=encoding, errors=errors)
```

Can't easily subclass Path.

**Approach:** patch `Path.read_text` where used:

```python
@pytest.mark.asyncio
async def test_initialize_schema_runs_statements(store, mock_conn, monkeypatch):
    from app.services import vector_store

    original_init = vector_store.VectorStore.initialize_schema

    async def wrapped(self):
        sql_text = "SELECT 1;\nSELECT 2;\n"
        parts = [p.strip() for p in sql_text.split(";") if p.strip()]
        async with self._pool.acquire() as conn:
            for stmt in parts:
                await conn.execute(stmt)

    # Test the loop logic by temporarily... 
```

I'll just delete the messy tests and add one that runs the real `initialize_schema` against the repo file - it will call execute multiple times. Count >= 1.

```python
@pytest.mark.asyncio
async def test_initialize_schema_runs_real_sql_file(store, mock_conn):
    await store.initialize_schema()
    assert mock_conn.execute.called
    assert mock_conn.execute.call_count >= 1
```

The repo's init_db.sql has multiple statements separated by `;` - split will produce ~6 statements. Good.

Remove the broken tests `test_initialize_schema_executes_statements` and `test_initialize_schema_reads_infra_file`.

Fix `search_multi_type` fake_search - patch uses keyword args but VectorStore.search is called as:
```python
await self.search(
    tenant_id=tenant_id,
    query_embedding=query_embedding,
    top_k=top_k_per_type,
    doc_type=dt,
)
```
So fake_search needs to accept those kwargs. Good.

Also fix patch - `patch.object(VectorStore, "search", new_callable=AsyncMock)` - the side_effect function receives (self, ...) when bound... Actually patch.object replaces the method, so when store.search is called, it's AsyncMock - the store instance might not use self from VectorStore if we're patching the class. When we call `store.search(...)`, it uses the patched method on the class, so the mock gets (store, ...) as first args.

Actually `store.search` is bound method - if we patch `VectorStore.search`, then `store.search` is still the patched version - AsyncMock when called receives `self` as first positional arg.

Our fake_search uses only keyword args:
```python
async def fake_search(*, tenant_id, query_embedding, top_k, doc_type):
```
But the actual call is positional + keyword mix:
`await self.search(tenant_id=tenant_id, query_embedding=query_embedding, top_k=top_k_per_type, doc_type=dt)`

All keyword - good. But AsyncMock will call side_effect with (self, ...) if it's a method... 

When you patch a class method with AsyncMock(side_effect=fn), the fn receives the same args as the original. For `await store.search(tenant_id=..., ...)`, the mock is invoked - I think the first arg might be the store instance.

Let me check: `patch.object(VectorStore, 'search', new_callable=AsyncMock)` - when `store.search(tenant_id='42', ...)` is called, the mock gets `tenant_id` as kwarg. Does it get self?

For a bound method replacement on the class, Python's behavior: `VectorStore.search` patched means `store.search` is a wrapper that passes self. The mock might receive (self, **kwargs) or just (**kwargs).

I'll use `async def fake_search(self, *args, **kwargs):` and read from kwargs.

```python
async def fake_search(self, *, tenant_id, query_embedding, top_k, doc_type):
```

Actually in tests they use:
```python
async def fake_search(*, tenant_id, query_embedding, top_k, doc_type):
```

If self is passed first, this fails. I'll use:

```python
async def fake_search(*args, **kwargs):
    doc_type = kwargs["doc_type"]
    ...
```

Let me run pytest to see.

Also remove the broken initialize_schema tests and add the simple one.

Fix search_multi_type tests to use flexible fake.
</think>
Removing broken `initialize_schema` tests and adding a single test that runs against the real SQL file. Fixing `search_multi_type` mocks to handle `self` if passed.

<｜tool▁calls▁begin｜><｜tool▁call▁begin｜>
StrReplace