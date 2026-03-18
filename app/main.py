from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(
    title="AI Business Intelligence API",
    description="AI assistant for SaaS business analytics",
    version="1.0.0"
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