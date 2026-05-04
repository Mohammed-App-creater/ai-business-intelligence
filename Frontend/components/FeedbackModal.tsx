"use client";

import { useEffect, useState } from "react";
import { X, ThumbsUp, ThumbsDown } from "lucide-react";
import { FeedbackRating } from "@/lib/types";

const UP_CATEGORIES = [
  { key: "accurate", label: "Accurate" },
  { key: "helpful", label: "Helpful" },
  { key: "clear", label: "Clear & well-written" },
  { key: "fast", label: "Fast response" },
];

const DOWN_CATEGORIES = [
  { key: "inaccurate", label: "Factually inaccurate" },
  { key: "incomplete", label: "Incomplete answer" },
  { key: "wrongData", label: "Wrong data / numbers" },
  { key: "hallucination", label: "Made things up" },
  { key: "unclear", label: "Unclear / confusing" },
  { key: "slow", label: "Too slow" },
];

interface FeedbackModalProps {
  open: boolean;
  rating: FeedbackRating;
  onClose: () => void;
  onSubmit: (data: { text: string; categories: Record<string, boolean> }) => void;
}

export default function FeedbackModal({ open, rating, onClose, onSubmit }: FeedbackModalProps) {
  const [text, setText] = useState("");
  const [categories, setCategories] = useState<Record<string, boolean>>({});

  useEffect(() => {
    if (open) {
      setText("");
      setCategories({});
    }
  }, [open]);

  if (!open) return null;

  const isUp = rating === "up";
  const options = isUp ? UP_CATEGORIES : DOWN_CATEGORIES;
  const headline = isUp ? "What did you like?" : "What went wrong?";
  const Icon = isUp ? ThumbsUp : ThumbsDown;
  const iconColor = isUp ? "text-emerald-400" : "text-red-400";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-md bg-panel border border-border rounded-xl shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div className="flex items-center gap-2.5">
            <Icon size={18} className={iconColor} />
            <h2 className="text-base font-semibold">{headline}</h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-muted hover:text-text transition-colors"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        <div className="p-5 space-y-4">
          <div>
            <div className="text-xs text-muted mb-2 uppercase tracking-wider">
              {isUp ? "Tags (optional)" : "What was wrong (optional)"}
            </div>
            <div className="flex flex-wrap gap-2">
              {options.map((opt) => {
                const active = !!categories[opt.key];
                return (
                  <button
                    key={opt.key}
                    type="button"
                    onClick={() =>
                      setCategories((c) => ({ ...c, [opt.key]: !c[opt.key] }))
                    }
                    className={`px-3 py-1.5 rounded-full text-sm border transition-colors ${
                      active
                        ? isUp
                          ? "bg-emerald-500/20 border-emerald-500/50 text-emerald-300"
                          : "bg-red-500/20 border-red-500/50 text-red-300"
                        : "bg-bg border-border hover:border-neutral-600"
                    }`}
                  >
                    {opt.label}
                  </button>
                );
              })}
            </div>
          </div>

          <div>
            <label className="block text-xs text-muted mb-2 uppercase tracking-wider">
              Additional notes (optional)
            </label>
            <textarea
              value={text}
              onChange={(e) => setText(e.target.value)}
              rows={4}
              placeholder={
                isUp
                  ? "Anything else you liked about this response?"
                  : "Tell us more about what was wrong…"
              }
              className="w-full px-3 py-2 rounded-md bg-bg border border-border focus:outline-none focus:border-neutral-500 text-sm resize-none"
            />
          </div>
        </div>

        <div className="flex justify-end gap-2 px-5 py-4 border-t border-border">
          <button
            type="button"
            onClick={onClose}
            className="px-4 py-2 text-sm rounded-md text-muted hover:text-text transition-colors"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => onSubmit({ text, categories })}
            className="px-4 py-2 text-sm rounded-md bg-accent hover:bg-accentHover text-white transition-colors"
          >
            Submit feedback
          </button>
        </div>
      </div>
    </div>
  );
}
