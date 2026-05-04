"use client";

import { ThumbsUp, ThumbsDown, Sparkles, User } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { ChatMessage } from "@/lib/types";

interface MessageProps {
  message: ChatMessage;
  onThumb: (rating: "up" | "down") => void;
}

export default function Message({ message, onThumb }: MessageProps) {
  const isUser = message.role === "user";
  const feedback = message.feedback;

  return (
    <div className="flex gap-3 py-4">
      <div
        className={`shrink-0 w-7 h-7 rounded-full flex items-center justify-center ${
          isUser ? "bg-neutral-700" : "bg-accent"
        }`}
      >
        {isUser ? <User size={14} /> : <Sparkles size={14} />}
      </div>

      <div className="flex-1 min-w-0">
        <div className="text-xs text-muted mb-1">
          {isUser ? "You" : "LEO"}
          <span className="mx-1.5">·</span>
          <span>{new Date(message.createdAt).toLocaleTimeString()}</span>
          {!isUser && message.latencyMs != null && (
            <>
              <span className="mx-1.5">·</span>
              <span>{message.latencyMs} ms</span>
            </>
          )}
        </div>

        {isUser ? (
          <div className="text-text whitespace-pre-wrap break-words">{message.content}</div>
        ) : (
          <div className="prose-leo text-text break-words">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
          </div>
        )}

        {/* Sources */}
        {!isUser && message.sources && message.sources.length > 0 && (
          <div className="mt-3 flex flex-wrap gap-1.5">
            {message.sources.map((src, i) => (
              <span
                key={i}
                className="text-[11px] px-2 py-0.5 rounded-full bg-bg border border-border text-muted"
              >
                {src}
              </span>
            ))}
          </div>
        )}

        {/* Feedback buttons (assistant only) */}
        {!isUser && (
          <div className="mt-2 flex items-center gap-1">
            <button
              type="button"
              onClick={() => onThumb("up")}
              className={`p-1.5 rounded-md transition-colors ${
                feedback?.rating === "up"
                  ? "bg-emerald-500/20 text-emerald-400"
                  : "text-muted hover:text-text hover:bg-bg"
              }`}
              aria-label="Thumbs up"
              title="Good response"
            >
              <ThumbsUp size={14} />
            </button>
            <button
              type="button"
              onClick={() => onThumb("down")}
              className={`p-1.5 rounded-md transition-colors ${
                feedback?.rating === "down"
                  ? "bg-red-500/20 text-red-400"
                  : "text-muted hover:text-text hover:bg-bg"
              }`}
              aria-label="Thumbs down"
              title="Bad response"
            >
              <ThumbsDown size={14} />
            </button>
            {feedback && (
              <span className="text-[11px] text-muted ml-1">
                Feedback recorded
                {feedback.text ? ` · "${feedback.text.slice(0, 40)}${feedback.text.length > 40 ? "…" : ""}"` : ""}
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
