import { NextRequest, NextResponse } from "next/server";
import { ChatRequest, ChatResponse } from "@/lib/types";

export const runtime = "nodejs";

const BACKEND_URL = process.env.LEO_BACKEND_URL || "http://localhost:8000";
const BACKEND_TOKEN = process.env.LEO_BACKEND_TOKEN || "dev-token";

interface IncomingBody {
  businessId: string;
  sessionId: string;
  question: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function unwrapAnswer(raw: unknown): string {
  if (typeof raw !== "string") return "";
  const trimmed = raw.trim();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return raw;
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed && typeof parsed === "object") {
      if (typeof parsed.response === "string") return parsed.response;
      if (typeof parsed.answer === "string") return parsed.answer;
      if (typeof parsed.message === "string") return parsed.message;
      if (typeof parsed.text === "string") return parsed.text;
    }
    return raw;
  } catch {
    return raw;
  }
}

async function callBackendWithRetry(payload: ChatRequest): Promise<Response> {
  const maxAttempts = 3;
  const baseDelayMs = 500;
  let lastError: unknown;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const res = await fetch(`${BACKEND_URL}/api/v1/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        cache: "no-store",
      });
      if (res.status >= 500 && attempt < maxAttempts) {
        await sleep(baseDelayMs * Math.pow(2, attempt - 1));
        continue;
      }
      return res;
    } catch (err) {
      lastError = err;
      if (attempt < maxAttempts) {
        await sleep(baseDelayMs * Math.pow(2, attempt - 1));
        continue;
      }
    }
  }
  throw lastError ?? new Error("Backend unreachable after retries");
}

export async function POST(req: NextRequest) {
  let body: IncomingBody;
  try {
    body = (await req.json()) as IncomingBody;
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const { businessId, sessionId, question } = body;
  if (!businessId || !sessionId || !question?.trim()) {
    return NextResponse.json(
      { error: "businessId, sessionId and question are required" },
      { status: 400 }
    );
  }

  const payload: ChatRequest = {
    business_id: businessId,
    org_id: businessId,
    session_id: sessionId,
    question,
    token: BACKEND_TOKEN,
  };

  const startedAt = Date.now();
  try {
    const upstream = await callBackendWithRetry(payload);

    if (!upstream.ok) {
      const text = await upstream.text().catch(() => "");
      return NextResponse.json(
        { error: `Backend returned ${upstream.status}`, detail: text },
        { status: upstream.status }
      );
    }

    const upstreamData = await upstream.json();
    const data: ChatResponse = {
      answer: unwrapAnswer(upstreamData.answer ?? upstreamData.response ?? ""),
      sources: upstreamData.sources,
      conversation_id: upstreamData.conversation_id ?? upstreamData.session_id,
      latency_ms: upstreamData.latency_ms ?? Date.now() - startedAt,
    };
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    return NextResponse.json(
      { error: "Failed to reach backend", detail: message },
      { status: 502 }
    );
  }
}
