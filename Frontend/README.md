# LEO AI BI — Internal Test UI

A minimal Next.js front end for internal testing of the LEO AI BI Assistant
backend (architecture v1.2). Looks like Claude, but stripped down — no
attachments, no auth, just a send button.

## What it does

- **Chat** with the FastAPI backend at `POST /api/v1/chat`.
- **Tenant switcher** (sidebar dropdown): tenant **42** is mock data, tenant
  **40** is live UAT data. Switching changes the `business_id` sent on every
  request.
- **Chat history** sidebar: every session is stored in `localStorage`, scoped
  per tenant. Click any past chat to resume; deleting is one click.
- **Thumbs up / thumbs down** on every assistant response. Either opens a
  feedback modal (tags + free text) and posts the result to the
  `chat_feedback` table in the Analytics Warehouse.

There's no user concept — sessions are bucketed by tenant only, exactly as
the spec described.

## Stack

- Next.js 16 (App Router, Turbopack) + React 19 + TypeScript
- Tailwind CSS 3
- `pg` for direct Postgres writes from `/api/feedback`
- Markdown rendering for assistant replies (`react-markdown` + `remark-gfm`)
- Session ids generated with the built-in `crypto.randomUUID()` — no `uuid` dep

## Setup

```bash
# 1. install
npm install

# 2. configure env
cp .env.local.example .env.local
# then edit .env.local with your backend + warehouse details

# 3. create the feedback table in your warehouse
psql "$WAREHOUSE_URL" -f infra/chat_feedback.sql

# 4. run
npm run dev
# open http://localhost:3000
```

### Environment variables

| Variable                | Purpose                                                   |
| ----------------------- | --------------------------------------------------------- |
| `LEO_BACKEND_URL`       | Base URL of the FastAPI backend (e.g. `http://localhost:8000`) |
| `LEO_BACKEND_TOKEN`     | SaaS session token sent in every `ChatRequest.token`      |
| `WAREHOUSE_HOST`        | PostgreSQL warehouse host                                 |
| `WAREHOUSE_PORT`        | PostgreSQL port (default `5432`)                          |
| `WAREHOUSE_DB`          | Warehouse database name                                   |
| `WAREHOUSE_USER`        | Warehouse user                                            |
| `WAREHOUSE_PASSWORD`    | Warehouse password                                        |

## Architecture

```
Browser ── localStorage (per-tenant session list)
   │
   │  POST /api/chat       (Next.js API route)
   ├──► FastAPI backend ──► LLM, retrieval, history, etc.
   │
   │  POST /api/feedback   (Next.js API route)
   └──► Analytics Warehouse (chat_feedback table)
```

- **`/api/chat`** is a thin proxy. It builds the `ChatRequest` schema from
  v1.2 (`business_id`, `session_id`, `question`, `token`) and forwards to
  `${LEO_BACKEND_URL}/api/v1/chat`. Latency is filled in on the proxy if the
  backend doesn't return it.
- **`/api/feedback`** writes one row to `chat_feedback`. The schema is
  intentionally extensible — `feedback_categories` is `JSONB`, so you can add
  new feedback dimensions later without a migration.

## The `chat_feedback` table

See `infra/chat_feedback.sql`. Columns:

| Column                | Type           | Notes                                          |
| --------------------- | -------------- | ---------------------------------------------- |
| `id`                  | `BIGSERIAL`    | PK                                             |
| `business_id`         | `VARCHAR(64)`  | Tenant id (e.g. `'40'`, `'42'`)                |
| `session_id`          | `UUID`         | Conversation id (matches `chat_history`)       |
| `message_id`          | `UUID`         | The assistant message being rated              |
| `question`            | `TEXT`         | The user question that prompted the answer     |
| `answer`              | `TEXT`         | Full assistant response                        |
| `rating`              | `VARCHAR(10)`  | `'up'` or `'down'`                             |
| `feedback_text`       | `TEXT`         | Free-text reason (optional)                    |
| `feedback_categories` | `JSONB`        | Tag flags, e.g. `{"inaccurate": true}`         |
| `sources`             | `JSONB`        | The `sources` array from the response          |
| `latency_ms`          | `INTEGER`      | Response latency at time of generation         |
| `llm_model`           | `VARCHAR(64)`  | Optional, fill in if your backend reports it   |
| `created_at`          | `TIMESTAMPTZ`  | Default `NOW()`                                |

Three indexes for the obvious analytical access patterns: lookup by session,
aggregations by rating over time, and lookup by message id.

## Adding new feedback dimensions later

Two paths, both cheap:

1. **New tag on the existing table** — just add a key to
   `feedback_categories`. No migration needed. Update the modal's
   `UP_CATEGORIES` / `DOWN_CATEGORIES` arrays in `components/FeedbackModal.tsx`.
2. **New first-class column** — add a column to `chat_feedback`, extend
   `FeedbackPayload` in `lib/types.ts`, and add a parameter to the `INSERT`
   in `app/api/feedback/route.ts`.

## Notes for testing

- Sessions are client-only (`localStorage`). Clearing browser storage clears
  history. This is intentional for a testing tool — no per-user accounts.
- The backend's own `chat_history` table is unaffected by this UI. The UI
  keeps its own copy of turns so it can render past conversations even if
  the backend rotates / archives history.
- If the backend is down, the assistant message slot is filled with the
  error so you can see what happened without losing the conversation.
