from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

import app.main as main_module


class _StubChatService:
    async def handle(self, request):
        return SimpleNamespace(
            answer=f"Echo: {request.question}",
            route="DIRECT",
            confidence=0.9,
            sources=[],
            conversation_id=request.conversation_id,
            latency_ms=1.0,
        )


def test_main_app_exposes_v1_chat_and_legacy_chat(monkeypatch):
    async def _fake_create_chat_service():
        return _StubChatService()

    monkeypatch.setattr(main_module, "create_chat_service", _fake_create_chat_service)

    with TestClient(main_module.app) as client:
        legacy = client.post("/chat", json={
            "business_id": "1",
            "question": "Hi",
        })
        assert legacy.status_code == 200
        assert legacy.json()["response"] == "AI response placeholder"

        v1 = client.post("/api/v1/chat", json={
            "question": "hello",
            "business_id": "biz_1",
            "org_id": 1,
        })
        assert v1.status_code == 200
        assert v1.json()["answer"] == "Echo: hello"
