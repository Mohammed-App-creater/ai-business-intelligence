"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Sidebar from "@/components/Sidebar";
import ChatView from "@/components/ChatView";
import FeedbackModal from "@/components/FeedbackModal";
import {
  ChatMessage,
  ChatSession,
  FeedbackRating,
  FeedbackRecord,
  TENANTS,
  TenantId,
} from "@/lib/types";
import { sendChat, submitFeedback } from "@/lib/api";
import {
  deleteSession,
  getActiveSessionId,
  getActiveTenant,
  loadSessions,
  setActiveSessionId,
  setActiveTenant,
  upsertSession,
} from "@/lib/storage";

export default function Page() {
  const [tenantId, setTenantId] = useState<TenantId>("42");
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [hydrated, setHydrated] = useState(false);

  // Feedback modal state
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const [feedbackRating, setFeedbackRating] = useState<FeedbackRating>("up");
  const [feedbackTarget, setFeedbackTarget] = useState<{
    sessionId: string;
    messageId: string;
  } | null>(null);

  // Hydrate from localStorage on mount
  useEffect(() => {
    const t = getActiveTenant() ?? "42";
    setTenantId(t);
    const list = loadSessions(t);
    setSessions(list);
    const active = getActiveSessionId(t);
    setActiveId(active && list.find((s) => s.id === active) ? active : null);
    setHydrated(true);
  }, []);

  // When tenant changes, reload sessions for that tenant
  const onTenantChange = useCallback((t: TenantId) => {
    setTenantId(t);
    setActiveTenant(t);
    const list = loadSessions(t);
    setSessions(list);
    const active = getActiveSessionId(t);
    setActiveId(active && list.find((s) => s.id === active) ? active : null);
  }, []);

  const activeSession = useMemo(
    () => sessions.find((s) => s.id === activeId) ?? null,
    [sessions, activeId]
  );
  const tenantLabel = TENANTS.find((t) => t.id === tenantId)?.label ?? "";

  const onSelectSession = useCallback(
    (id: string) => {
      setActiveId(id);
      setActiveSessionId(tenantId, id);
    },
    [tenantId]
  );

  const onNewSession = useCallback(() => {
    setActiveId(null);
    setActiveSessionId(tenantId, null);
  }, [tenantId]);

  const onDeleteSession = useCallback(
    (id: string) => {
      deleteSession(tenantId, id);
      const list = loadSessions(tenantId);
      setSessions(list);
      if (activeId === id) {
        setActiveId(null);
        setActiveSessionId(tenantId, null);
      }
    },
    [tenantId, activeId]
  );

  const persistSession = useCallback((session: ChatSession) => {
    upsertSession(session);
    setSessions(loadSessions(session.tenantId));
  }, []);

  const onSend = useCallback(
    async (text: string) => {
      const now = new Date().toISOString();

      // Create or load the target session
      let session: ChatSession;
      if (activeSession) {
        session = activeSession;
      } else {
        const id = crypto.randomUUID();
        session = {
          id,
          tenantId,
          title: text.slice(0, 60),
          createdAt: now,
          updatedAt: now,
          messages: [],
        };
        setActiveId(id);
        setActiveSessionId(tenantId, id);
      }

      // Append user message immediately
      const userMsg: ChatMessage = {
        id: crypto.randomUUID(),
        role: "user",
        content: text,
        createdAt: now,
      };
      const withUser: ChatSession = {
        ...session,
        title: session.messages.length === 0 ? text.slice(0, 60) : session.title,
        updatedAt: now,
        messages: [...session.messages, userMsg],
      };
      persistSession(withUser);

      setLoading(true);
      try {
        const res = await sendChat({
          businessId: tenantId,
          sessionId: withUser.id,
          question: text,
        });

        const assistantMsg: ChatMessage = {
          id: crypto.randomUUID(),
          role: "assistant",
          content: res.answer,
          createdAt: new Date().toISOString(),
          sources: res.sources,
          latencyMs: res.latency_ms,
        };
        const updated: ChatSession = {
          ...withUser,
          updatedAt: assistantMsg.createdAt,
          messages: [...withUser.messages, assistantMsg],
        };
        persistSession(updated);
      } catch (err) {
        const errorMsg: ChatMessage = {
          id: crypto.randomUUID(),
          role: "assistant",
          content:
            "**Error:** " +
            (err instanceof Error ? err.message : "Something went wrong calling the backend."),
          createdAt: new Date().toISOString(),
        };
        const updated: ChatSession = {
          ...withUser,
          updatedAt: errorMsg.createdAt,
          messages: [...withUser.messages, errorMsg],
        };
        persistSession(updated);
      } finally {
        setLoading(false);
      }
    },
    [activeSession, tenantId, persistSession]
  );

  // Open the feedback modal for a given assistant message
  const onThumb = useCallback(
    (messageId: string, rating: FeedbackRating) => {
      if (!activeSession) return;
      setFeedbackTarget({ sessionId: activeSession.id, messageId });
      setFeedbackRating(rating);
      setFeedbackOpen(true);
    },
    [activeSession]
  );

  const onFeedbackSubmit = useCallback(
    async ({ text, categories }: { text: string; categories: Record<string, boolean> }) => {
      if (!feedbackTarget) return;
      const session = sessions.find((s) => s.id === feedbackTarget.sessionId);
      if (!session) return;

      // Find the assistant message and the preceding user question
      const idx = session.messages.findIndex((m) => m.id === feedbackTarget.messageId);
      if (idx < 0) return;
      const assistantMsg = session.messages[idx];
      // Walk backwards to find the most recent user message
      let questionMsg: ChatMessage | undefined;
      for (let i = idx - 1; i >= 0; i--) {
        if (session.messages[i].role === "user") {
          questionMsg = session.messages[i];
          break;
        }
      }

      const record: FeedbackRecord = {
        rating: feedbackRating,
        text: text.trim() || undefined,
        categories: Object.keys(categories).length > 0 ? categories : undefined,
        submittedAt: new Date().toISOString(),
      };

      // Update the message locally so the UI reflects the feedback
      const updatedMessages = session.messages.map((m) =>
        m.id === assistantMsg.id ? { ...m, feedback: record } : m
      );
      const updatedSession: ChatSession = {
        ...session,
        messages: updatedMessages,
      };
      persistSession(updatedSession);

      // Fire-and-forget post to warehouse
      try {
        await submitFeedback({
          business_id: session.tenantId,
          session_id: session.id,
          message_id: assistantMsg.id,
          question: questionMsg?.content || "",
          answer: assistantMsg.content,
          rating: feedbackRating,
          feedback_text: record.text,
          feedback_categories: record.categories as Record<string, boolean> | undefined,
          sources: assistantMsg.sources,
          latency_ms: assistantMsg.latencyMs,
        });
      } catch (err) {
        console.error("Feedback submit failed", err);
        // We keep the local feedback record either way; this is a testing tool
      }

      setFeedbackOpen(false);
      setFeedbackTarget(null);
    },
    [feedbackTarget, feedbackRating, sessions, persistSession]
  );

  if (!hydrated) {
    return <div className="h-screen flex items-center justify-center text-muted">Loading…</div>;
  }

  return (
    <div className="h-screen w-screen flex bg-bg text-text">
      <Sidebar
        tenantId={tenantId}
        onTenantChange={onTenantChange}
        sessions={sessions}
        activeSessionId={activeId}
        onSelectSession={onSelectSession}
        onNewSession={onNewSession}
        onDeleteSession={onDeleteSession}
      />
      <ChatView
        tenantLabel={tenantLabel}
        messages={activeSession?.messages ?? []}
        loading={loading}
        onSend={onSend}
        onThumb={onThumb}
      />
      <FeedbackModal
        open={feedbackOpen}
        rating={feedbackRating}
        onClose={() => {
          setFeedbackOpen(false);
          setFeedbackTarget(null);
        }}
        onSubmit={onFeedbackSubmit}
      />
    </div>
  );
}
