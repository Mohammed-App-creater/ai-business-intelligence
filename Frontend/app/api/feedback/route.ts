import { NextRequest, NextResponse } from "next/server";
import { FeedbackPayload } from "@/lib/types";
import { getWarehousePool } from "@/lib/warehouse";

export const runtime = "nodejs";

export async function POST(req: NextRequest) {
  let body: FeedbackPayload;
  try {
    body = (await req.json()) as FeedbackPayload;
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const required: (keyof FeedbackPayload)[] = [
    "business_id",
    "session_id",
    "message_id",
    "question",
    "answer",
    "rating",
  ];
  for (const key of required) {
    if (!body[key]) {
      return NextResponse.json({ error: `Missing field: ${key}` }, { status: 400 });
    }
  }
  if (body.rating !== "up" && body.rating !== "down") {
    return NextResponse.json({ error: "rating must be 'up' or 'down'" }, { status: 400 });
  }

  const pool = getWarehousePool();
  try {
    await pool.query(
      `INSERT INTO chat_feedback (
         business_id, session_id, message_id,
         question, answer, rating,
         feedback_text, feedback_categories,
         sources, latency_ms, created_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, NOW())`,
      [
        body.business_id,
        body.session_id,
        body.message_id,
        body.question,
        body.answer,
        body.rating,
        body.feedback_text || null,
        body.feedback_categories ? JSON.stringify(body.feedback_categories) : null,
        body.sources ? JSON.stringify(body.sources) : null,
        body.latency_ms != null ? Math.round(body.latency_ms) : null,
      ]
    );
    return NextResponse.json({ ok: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    console.error("[feedback] insert failed:", message);
    return NextResponse.json(
      { error: "Failed to record feedback", detail: message },
      { status: 500 }
    );
  }
}
