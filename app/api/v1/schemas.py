from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    business_id: str = Field(..., min_length=1)
    org_id: int = Field(..., gt=0)
    conversation_id: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    route: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    sources: list[str] = Field(default_factory=list)
    conversation_id: Optional[str] = None
    latency_ms: float = Field(..., ge=0.0)


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
