from __future__ import annotations

from fastapi import APIRouter, Request

from app.api.v1.schemas import ChatRequest, ChatResponse

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def chat(request: Request, body: ChatRequest) -> ChatResponse:
    chat_service = request.app.state.chat_service
    return await chat_service.handle(body)
