"use client";

import { Send, Sparkles } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { ChatMessage } from "@/lib/types";
import Message from "./Message";

interface ChatViewProps {
  tenantLabel: string;
  messages: ChatMessage[];
  loading: boolean;
  onSend: (text: string) => void;
  onThumb: (messageId: string, rating: "up" | "down") => void;
}

export default function ChatView({
  tenantLabel,
  messages,
  loading,
  onSend,
  onThumb,
}: ChatViewProps) {
  const [draft, setDraft] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages.length, loading]);

  // Auto-grow textarea
  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, 200) + "px";
  }, [draft]);

  const submit = () => {
    const text = draft.trim();
    if (!text || loading) return;
    setDraft("");
    onSend(text);
  };

  return (
    <main className="flex-1 flex flex-col h-full">
      {/* Top bar */}
      <header className="px-6 py-3 border-b border-border flex items-center justify-between">
        <div>
          <div className="text-sm font-semibold">LEO AI BI Assistant</div>
          <div className="text-xs text-muted">{tenantLabel}</div>
        </div>
      </header>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-6">
          {messages.length === 0 && !loading ? (
            <div className="h-[60vh] flex flex-col items-center justify-center text-center">
              <div className="w-12 h-12 rounded-full bg-accent flex items-center justify-center mb-4">
                <Sparkles size={20} />
              </div>
              <div className="text-lg font-semibold mb-1">Ready to test</div>
              <div className="text-sm text-muted max-w-md">
                Ask anything about revenue, staff performance, services, retention or campaigns.
                Use the tenant switcher in the sidebar to flip between mock and live data.
              </div>
            </div>
          ) : (
            <div className="divide-y divide-border/50">
              {messages.map((m) => (
                <Message key={m.id} message={m} onThumb={(r) => onThumb(m.id, r)} />
              ))}
              {loading && (
                <div className="py-4 flex gap-3">
                  <div className="shrink-0 w-7 h-7 rounded-full bg-accent flex items-center justify-center">
                    <Sparkles size={14} />
                  </div>
                  <div className="flex items-center gap-1 pt-2">
                    <span className="typing-dot w-1.5 h-1.5 rounded-full bg-muted" />
                    <span className="typing-dot w-1.5 h-1.5 rounded-full bg-muted" />
                    <span className="typing-dot w-1.5 h-1.5 rounded-full bg-muted" />
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Composer */}
      <div className="px-6 pb-6 pt-2">
        <div className="max-w-3xl mx-auto">
          <div className="relative flex items-end gap-2 bg-panel border border-border rounded-2xl p-2 focus-within:border-neutral-500 transition-colors">
            <textarea
              ref={textareaRef}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  submit();
                }
              }}
              placeholder="Ask a question…  (Enter to send, Shift+Enter for newline)"
              rows={1}
              className="flex-1 bg-transparent resize-none outline-none px-3 py-2 text-sm placeholder:text-muted max-h-[200px]"
              disabled={loading}
            />
            <button
              type="button"
              onClick={submit}
              disabled={loading || !draft.trim()}
              className="shrink-0 w-9 h-9 rounded-xl bg-accent hover:bg-accentHover disabled:opacity-40 disabled:cursor-not-allowed flex items-center justify-center transition-colors"
              aria-label="Send"
            >
              <Send size={16} />
            </button>
          </div>
          <div className="mt-2 text-[11px] text-muted text-center">
            Internal testing tool · Responses may be inaccurate
          </div>
        </div>
      </div>
    </main>
  );
}
