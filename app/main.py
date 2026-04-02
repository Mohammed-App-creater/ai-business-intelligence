from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

# Load repo-root .env before factories read os.environ (uvicorn does not do this).
# override=True: .env wins over stale OPENAI_* / machine env (common on Windows).
_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", override=True)

from fastapi import FastAPI
from pydantic import BaseModel

from app.api.v1.routes.chat import router as v1_chat_router
from app.services.chat_service import ChatService

logger = logging.getLogger(__name__)


class _FallbackGateway:
    async def call(self, *_args, **_kwargs):
        raise RuntimeError("LLM gateway is not configured")

    async def call_with_data(self, *_args, **_kwargs):
        raise RuntimeError("LLM gateway is not configured")


class _FallbackEmbeddingClient:
    async def embed(self, *_args, **_kwargs) -> list[float]:
        raise RuntimeError("Embedding client is not configured")


class _FallbackVectorStore:
    async def search(self, *_args, **_kwargs) -> list[dict]:
        raise RuntimeError("Vector store is not configured")

    async def search_multi_domain(self, *_args, **_kwargs) -> list[dict]:
        raise RuntimeError("Vector store is not configured")


async def create_chat_service() -> ChatService:
    """
    Build app chat service from environment-backed dependencies.
    Falls back to safe stubs so the API remains reachable.
    """
    from app.services.embeddings.embedding_client import EmbeddingClient
    from app.services.llm.llm_gateway import LLMGateway
    from app.services.query_analyzer import QueryAnalyzer
    from app.services.retriever import Retriever

    try:
        gateway = LLMGateway.from_env()
    except Exception as exc:  # noqa: BLE001
        logger.warning("llm_gateway init failed; using fallback: %r", exc)
        gateway = _FallbackGateway()

    analyzer = QueryAnalyzer(gateway=gateway)

    try:
        embedding_client = EmbeddingClient.from_env()
    except Exception as exc:  # noqa: BLE001
        logger.warning("embedding_client init failed; using fallback: %r", exc)
        embedding_client = _FallbackEmbeddingClient()

    vector_pool = None
    try:
        from app.services.db.db_pool import PGPool, PGTarget
        from app.services.vector_store import VectorStore

        vector_pool = await PGPool.from_env(PGTarget.VECTOR)
        vector_store = VectorStore.from_pool(vector_pool)
    except Exception as exc:  # noqa: BLE001
        logger.warning("vector_store init failed; using fallback: %r", exc)
        vector_store = _FallbackVectorStore()

    service = ChatService(analyzer, Retriever(embedding_client, vector_store), gateway)
    service._vector_pool = vector_pool
    return service


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.chat_service = await create_chat_service()
    try:
        yield
    finally:
        vector_pool = getattr(getattr(app.state, "chat_service", None), "_vector_pool", None)
        if vector_pool is not None:
            await vector_pool.close()


app = FastAPI(
    title="AI Business Intelligence API",
    description="AI assistant for SaaS business analytics",
    version="1.0.0",
    lifespan=lifespan,
)

@app.get("/")
def root():
    return {"message": "AI Business Intelligence API is running"}


class ChatRequest(BaseModel):
    business_id: str
    question: str

@app.post("/chat")
def chat(request: ChatRequest):
    return {
        "business_id": request.business_id,
        "question": request.question,
        "response": "AI response placeholder"
    }


app.include_router(v1_chat_router, prefix="/api/v1")