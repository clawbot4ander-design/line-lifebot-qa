# LifeBot Fast LINE QA

Fast Zeabur webhook for ordinary diabetes patient-education questions.

This service answers LINE text messages directly with a configured LLM provider
(Gemini or DeepSeek) and does not start Hermes Agent. Keep Hermes Agent for scheduled diabetes news, academic search,
Obsidian/Google Drive archiving, image generation, and audio generation.

## Endpoints

- `GET /` health check
- `POST /line/webhook` LINE Messaging API webhook

## Zeabur Environment Variables

```bash
LINE_CHANNEL_SECRET=...
LINE_CHANNEL_ACCESS_TOKEN=...
APP_VERSION=2026-04-30-all-guidelines-v14
LLM_PROVIDER=gemini
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-3.1-flash-lite-preview
GEMINI_TIMEOUT=20
LINE_QUERY_PLANNING_ENABLED=1
LINE_LLM_RERANK_ENABLED=1
LINE_LLM_RERANK_TOP_K=5
LINE_EVIDENCE_REVIEW_ENABLED=1
LINE_RETRIEVAL_QUERY_MAX_CHARS=1400
LINE_TIMEOUT=12
LINE_MEMORY_ENABLED=1
LINE_CONTEXT_ENABLED=1
LINE_SESSION_SCOPE=user
LINE_CONTEXT_MAX_MESSAGES=8
LINE_CONTEXT_TTL_SECONDS=43200
DATABASE_URL=postgresql://...
LINE_KNOWLEDGE_ENABLED=1
LINE_KNOWLEDGE_STRICT=1
LINE_KNOWLEDGE_DIRS=/app/data,/app/data/ada,/app/data/aace,/app/data/kdigo,/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines
LINE_KNOWLEDGE_DIR=/app/data/guidelines
LINE_KNOWLEDGE_EXTRA_PATHS=0
LINE_KNOWLEDGE_PARENT_CONTEXT_CHARS=900
LINE_KNOWLEDGE_CANDIDATE_SNIPPETS=15
LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS=700
LINE_KNOWLEDGE_MAX_SNIPPETS=5
LINE_KNOWLEDGE_EXCERPT_CHARS=900
```

Minimum variables to add or verify in Zeabur:

```bash
APP_VERSION=2026-04-30-all-guidelines-v14
LLM_PROVIDER=gemini
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-3.1-flash-lite-preview
LINE_MEMORY_ENABLED=1
LINE_CONTEXT_ENABLED=1
LINE_SESSION_SCOPE=user
LINE_KNOWLEDGE_ENABLED=1
LINE_KNOWLEDGE_STRICT=1
LINE_KNOWLEDGE_DIRS=/app/data,/app/data/ada,/app/data/aace,/app/data/kdigo,/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines
LINE_KNOWLEDGE_DIR=/app/data/guidelines
LINE_KNOWLEDGE_EXTRA_PATHS=0
LINE_QUERY_PLANNING_ENABLED=1
LINE_LLM_RERANK_ENABLED=1
LINE_EVIDENCE_REVIEW_ENABLED=1
```

The same minimal set is also saved in `zeabur.env.example`.

`GOOGLE_API_KEY` is also accepted as a Gemini fallback. DeepSeek remains
available as an optional provider by setting `LLM_PROVIDER=deepseek`,
`DEEPSEEK_API_KEY`, and `DEEPSEEK_MODEL=deepseek-v4-pro` or
`deepseek-v4-flash`.

## Background Knowledge

The webhook loads all mounted guideline Markdown files from `LINE_KNOWLEDGE_DIRS`
or `LINE_KNOWLEDGE_DIR`, then performs local file-based retrieval before each
LLM answer. This is meant for LINE DM patient-education grounding, not for
long-term user memory.

Default source inside Zeabur/container:

```text
/app/data
/app/data/ada
/app/data/aace
/app/data/kdigo
/app/data/guidelines
/app/data/adaguidelines
/app/data/kdigoguidelines
/app/data/aaceguidelines
```

Useful settings:

```bash
LINE_KNOWLEDGE_ENABLED=1
LINE_KNOWLEDGE_STRICT=1
LINE_KNOWLEDGE_DIRS=/app/data,/app/data/ada,/app/data/aace,/app/data/kdigo,/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines
LINE_KNOWLEDGE_DIR=/app/data/guidelines
LINE_KNOWLEDGE_EXTRA_PATHS=0
LINE_KNOWLEDGE_CHUNK_CHARS=1800
LINE_KNOWLEDGE_PARENT_CONTEXT_CHARS=900
LINE_KNOWLEDGE_SOURCE_MIN_CANDIDATES=2
LINE_KNOWLEDGE_CANDIDATE_SNIPPETS=15
LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS=700
LINE_KNOWLEDGE_MAX_SNIPPETS=5
LINE_KNOWLEDGE_EXCERPT_CHARS=900
```

Health check includes `knowledge.available`, `knowledge.files`, and
`knowledge.chunks` so deployment can verify the files are mounted correctly.
For production, make sure you have permission to use the guideline files in this
kind of application and mount/copy them into the deployed service path.

### Zeabur Guideline Knowledge Setup

Do not commit full guideline Markdown files into a public GitHub repo unless you
have permission to redistribute them. Recommended deployment:

1. In Zeabur, create or attach a Volume for this service.
2. Mount it at `/app/data`.
3. Prefer splitting guideline Markdown files into three source folders:
   `/app/data/ada`, `/app/data/aace`, and `/app/data/kdigo`. Legacy folders
   `/app/data/adaguidelines`, `/app/data/aaceguidelines`, and
   `/app/data/kdigoguidelines` are still scanned.
4. Set:

```bash
LINE_KNOWLEDGE_ENABLED=1
LINE_KNOWLEDGE_STRICT=1
LINE_KNOWLEDGE_DIRS=/app/data,/app/data/ada,/app/data/aace,/app/data/kdigo,/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines
LINE_KNOWLEDGE_EXTRA_PATHS=0
```

After redeploy, `GET /` should show:

```json
"knowledge": {
  "enabled": true,
  "available": true,
  "files": 17,
  "dir_files": 17,
  "extra_files": 0,
  "sources": [
    "ADA Standards of Care in Diabetes 2026",
    "KDIGO 2024 Clinical Practice Guideline for CKD",
    "AACE 2026 Consensus Statement: Algorithm for Management of Adults With T2D"
  ]
}
```

If `available` is `false` or `files` is `0`, the bot is running but the mounted
guideline folder is still missing or empty.

Strict mode is enabled by default. When `LINE_KNOWLEDGE_STRICT=1`, the bot only
answers from retrieved guideline snippets. If the loaded guideline knowledge
base does not contain enough relevant support, it should decline instead of
using the configured model's general medical knowledge.

## Guideline Reasoning Flow

The bot uses the configured LLM only for reasoning over the local guideline
knowledge base, not as an independent medical source.

Default behavior:

```bash
LINE_QUERY_PLANNING_ENABLED=1
LINE_LLM_RERANK_ENABLED=1
LINE_LLM_RERANK_TOP_K=5
LINE_EVIDENCE_REVIEW_ENABLED=1
LINE_RETRIEVAL_QUERY_MAX_CHARS=1400
LINE_KNOWLEDGE_CANDIDATE_SNIPPETS=15
LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS=700
```

Per message, the flow is:

1. Use the current question plus short-term LINE context to create a clinical
   intent JSON: clinical intent, question type, patient context, evidence facets,
   must-retrieve topics, and answer strategy.
2. Use that clinical intent JSON to create a guideline search query with likely
   English terms, abbreviations, section words, and evidence targets.
3. Search the mounted guideline Markdown files with
   multi-query retrieval, source-aware scoring, section-aware scoring, and
   indexed metadata such as source, title, section, and table row type.
4. Split table rows into separate retrievable snippets and attach parent section
   context so medication tables, eGFR thresholds, contraindications, footnotes,
   and dosing/use considerations can rank independently without losing their
   guideline context.
5. Merge candidates with source-balanced, coverage-aware, and MMR-style
   selection so KDIGO/AACE snippets are less likely to be crowded out by repeated
   ADA chapter snippets.
6. Retrieve a candidate pool, then ask the configured LLM to rerank only those candidates
   using the clinical intent JSON and decide whether the snippets cover all core
   concepts in the question.
7. Apply a local coverage safety net so a conservative LLM reranker cannot
   reject an answer when selected snippets already cover required facets such
   as CKD, medication, and eGFR thresholds.
8. Ask the configured LLM to organize only the selected guideline snippets into an evidence
   review, including source names, coverage gaps, and the clinical intent answer strategy.
9. Generate the final Traditional Chinese LINE answer from the guideline
   snippets and evidence review.

The final answer prompt still forbids the configured model from using built-in medical
knowledge, unmounted guidelines, news, or unsupported inference.

## LINE User Name Memory

The webhook can remember one display name per LINE `source.userId`.
Use PostgreSQL in Zeabur by adding a PostgreSQL service and exposing
`DATABASE_URL` to this service.

Supported user commands:

```text
我叫小明
我是小明
請記住我叫小明
My name is John
I am John
Call me John
你記得我的名字嗎？
忘記我的名字
```

The bot only saves the user's display name/call name. It does not save diabetes
preferences, blood glucose values, medication, addresses, phone numbers, or
medical records. If `DATABASE_URL` is not set, the service falls back to SQLite
at `LINE_MEMORY_DB` for local testing.

## Short-Term Conversation Context

The webhook can keep recent LINE messages for continuity, so follow-up questions
like `那這樣可以吃嗎？` can use the previous answer as context. This is short-term
conversation context, separate from name memory.

Default behavior:

```bash
LINE_CONTEXT_ENABLED=1
LINE_SESSION_SCOPE=user
LINE_CONTEXT_MAX_MESSAGES=8
LINE_CONTEXT_TTL_SECONDS=43200
```

This keeps the latest 8 user/bot messages for 12 hours per LINE user, group, or
room. `LINE_SESSION_SCOPE` controls the context boundary:

- `user`: one session per LINE user, even inside groups. This is the safest
  default for health questions.
- `chat`: one shared session per DM, group, or room.
- `chat_user`: one session per user inside each group or room, and one session
  per DM user.

The webhook processes one active request at a time per session key, so rapid
follow-up messages from the same session are written back in order. Different
sessions can still run concurrently in the app process.

Users can clear the current context by sending:

```text
重新開始
清除剛剛對話
清除上下文
```

## LINE Webhook URL

After Zeabur deploys this service, set the LINE webhook to:

```text
https://<your-zeabur-domain>/line/webhook
```

The previous local Hermes bridge can stay available for testing, but the fast
QA production webhook should point to this Zeabur service.

## Local Test

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 127.0.0.1 --port 8790
```

Open:

```text
http://127.0.0.1:8790/
```

The health check should include:

```json
{
  "app_version": "2026-04-30-all-guidelines-v14",
  "llm_provider": "gemini",
  "model": "gemini-3.1-flash-lite-preview",
  "features": {
    "english_name_memory": true,
    "trailing_question_removal": true,
    "short_term_context": true,
    "guideline_strict_grounding": true,
    "guideline_query_planning": true,
    "clinical_intent_planning": true,
    "guideline_evidence_review": true,
    "all_mounted_guideline_sources": true,
    "ada_only_sources": false,
    "multi_guideline_sources": true,
    "source_aware_reranking": true,
    "source_balanced_retrieval": true,
    "section_aware_retrieval": true,
    "table_aware_retrieval": true,
    "multi_query_retrieval": true,
    "intent_query_variants": true,
    "metadata_indexing": true,
    "coverage_aware_retrieval": true,
    "mmr_style_diversity": true,
    "local_coverage_answerability": true,
    "comparative_threshold_answering": true,
    "threshold_review_override": true,
    "deepseek_provider": true,
    "llm_reranker": true,
    "coverage_answerability_check": true,
    "parent_child_table_context": true,
    "guideline_strict_grounding_current": true,
    "guideline_query_planning_current": true,
    "guideline_evidence_review_current": true
  }
}
```

If LINE still replies to `I am ander` with a generic diabetes answer, the deployed
service is likely still running an older build or `LINE_MEMORY_ENABLED=0`.
