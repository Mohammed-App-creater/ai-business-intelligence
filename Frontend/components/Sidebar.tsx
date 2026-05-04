"use client";

import { Plus, MessageSquare, Trash2, ChevronDown } from "lucide-react";
import { useState } from "react";
import { ChatSession, TenantId, TENANTS } from "@/lib/types";

interface SidebarProps {
  tenantId: TenantId;
  onTenantChange: (tenantId: TenantId) => void;
  sessions: ChatSession[];
  activeSessionId: string | null;
  onSelectSession: (sessionId: string) => void;
  onNewSession: () => void;
  onDeleteSession: (sessionId: string) => void;
}

export default function Sidebar({
  tenantId,
  onTenantChange,
  sessions,
  activeSessionId,
  onSelectSession,
  onNewSession,
  onDeleteSession,
}: SidebarProps) {
  const [tenantOpen, setTenantOpen] = useState(false);
  const activeTenant = TENANTS.find((t) => t.id === tenantId)!;

  return (
    <aside className="w-72 shrink-0 bg-panel border-r border-border flex flex-col h-full">
      {/* Header / tenant switcher */}
      <div className="p-3 border-b border-border">
        <div className="text-[11px] uppercase tracking-wider text-muted mb-1.5 px-1">
          Tenant
        </div>
        <div className="relative">
          <button
            type="button"
            onClick={() => setTenantOpen((v) => !v)}
            className="w-full flex items-center justify-between gap-2 px-3 py-2 rounded-md bg-bg border border-border hover:border-neutral-600 transition-colors text-left"
          >
            <div className="min-w-0">
              <div className="text-sm font-medium truncate">{activeTenant.label}</div>
              <div className="text-xs text-muted truncate">{activeTenant.description}</div>
            </div>
            <ChevronDown
              size={16}
              className={`text-muted transition-transform ${tenantOpen ? "rotate-180" : ""}`}
            />
          </button>
          {tenantOpen && (
            <div className="absolute left-0 right-0 top-full mt-1 z-20 bg-panel border border-border rounded-md shadow-lg overflow-hidden">
              {TENANTS.map((t) => (
                <button
                  key={t.id}
                  type="button"
                  onClick={() => {
                    onTenantChange(t.id);
                    setTenantOpen(false);
                  }}
                  className={`w-full text-left px-3 py-2 hover:bg-bg transition-colors ${
                    t.id === tenantId ? "bg-bg" : ""
                  }`}
                >
                  <div className="text-sm font-medium">{t.label}</div>
                  <div className="text-xs text-muted">{t.description}</div>
                </button>
              ))}
            </div>
          )}
        </div>

        <button
          type="button"
          onClick={onNewSession}
          className="mt-3 w-full flex items-center justify-center gap-2 px-3 py-2 rounded-md bg-accent hover:bg-accentHover transition-colors text-sm font-medium text-white"
        >
          <Plus size={16} />
          New chat
        </button>
      </div>

      {/* Session list */}
      <div className="flex-1 overflow-y-auto py-2">
        {sessions.length === 0 ? (
          <div className="px-4 py-8 text-center text-sm text-muted">
            No conversations yet.
            <br />
            Start a new chat to begin.
          </div>
        ) : (
          <ul className="px-2 space-y-0.5">
            {sessions.map((s) => (
              <li key={s.id}>
                <div
                  className={`group flex items-center gap-2 rounded-md px-2 py-2 cursor-pointer transition-colors ${
                    s.id === activeSessionId
                      ? "bg-bg border border-border"
                      : "hover:bg-bg/60 border border-transparent"
                  }`}
                  onClick={() => onSelectSession(s.id)}
                >
                  <MessageSquare size={14} className="text-muted shrink-0" />
                  <div className="flex-1 min-w-0">
                    <div className="text-sm truncate">{s.title}</div>
                    <div className="text-[11px] text-muted">
                      {new Date(s.updatedAt).toLocaleString()}
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      if (confirm("Delete this conversation?")) {
                        onDeleteSession(s.id);
                      }
                    }}
                    className="opacity-0 group-hover:opacity-100 text-muted hover:text-red-400 transition-opacity"
                    aria-label="Delete conversation"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>

      <div className="px-3 py-2 border-t border-border text-[11px] text-muted">
        LEO AI BI · Internal Test UI
      </div>
    </aside>
  );
}
