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
APP_VERSION=2026-05-01-no-multiagent-hermes-brain-v25
LLM_PROVIDER=gemini
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-3.1-flash-lite-preview
GEMINI_TIMEOUT=20
LINE_QUERY_PLANNING_ENABLED=1
LINE_LLM_RERANK_ENABLED=1
LINE_LLM_RERANK_TOP_K=5
LINE_RECURSIVE_COVERAGE_ENABLED=1
LINE_RECURSIVE_COVERAGE_MAX_QUERIES=4
LINE_RECURSIVE_COVERAGE_MAX_HITS=4
LINE_EVIDENCE_REVIEW_ENABLED=1
LINE_LONG_CONTEXT_VERIFICATION_ENABLED=1
LINE_WHOLE_SECTION_CONTEXT_ENABLED=1
LINE_WHOLE_SECTION_CONTEXT_MAX_SECTIONS=2
LINE_WHOLE_SECTION_CONTEXT_CHARS=9000
LINE_DEBUG_SEARCH_ENABLED=1
LINE_DEBUG_SEARCH_MAX_HITS=12
# Optional: if set, /debug/search requires x-debug-token header.
LINE_DEBUG_TOKEN=
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
LINE_KNOWLEDGE_PARENT_SECTION_CHARS=1800
LINE_KNOWLEDGE_VECTOR_DIM=768
LINE_KNOWLEDGE_VECTOR_WEIGHT=0.55
LINE_KNOWLEDGE_INVERTED_INDEX_ENABLED=1
LINE_KNOWLEDGE_POSTING_MAX_TOKENS=72
LINE_KNOWLEDGE_POSTING_TARGET_CHUNKS=800
LINE_DENSE_EMBEDDING_ENABLED=0
LINE_DENSE_EMBEDDING_PROVIDER=gemini
LINE_DENSE_EMBEDDING_MODEL=text-embedding-004
LINE_DENSE_EMBEDDING_CACHE=/tmp/line_lifebot_dense_embeddings.jsonl
LINE_DENSE_EMBEDDING_WEIGHT=1.15
LINE_KNOWLEDGE_CANDIDATE_SNIPPETS=15
LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS=700
LINE_KNOWLEDGE_MAX_SNIPPETS=5
LINE_KNOWLEDGE_EXCERPT_CHARS=900
```

Minimum variables to add or verify in Zeabur:

```bash
APP_VERSION=2026-05-01-no-multiagent-hermes-brain-v25
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
LINE_RECURSIVE_COVERAGE_ENABLED=1
LINE_EVIDENCE_REVIEW_ENABLED=1
LINE_LONG_CONTEXT_VERIFICATION_ENABLED=1
LINE_WHOLE_SECTION_CONTEXT_ENABLED=1
LINE_DEBUG_SEARCH_ENABLED=1
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
LINE_KNOWLEDGE_KDIGO_CKD_BOOST=1.85
LINE_KNOWLEDGE_KDIGO_CKD_MEDICATION_BOOST=1.35
LINE_KNOWLEDGE_AACE_MEDICATION_BOOST=1.25
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
LINE_RECURSIVE_COVERAGE_ENABLED=1
LINE_RECURSIVE_COVERAGE_MAX_QUERIES=4
LINE_RECURSIVE_COVERAGE_MAX_HITS=4
LINE_EVIDENCE_REVIEW_ENABLED=1
LINE_LONG_CONTEXT_VERIFICATION_ENABLED=1
LINE_WHOLE_SECTION_CONTEXT_ENABLED=1
LINE_WHOLE_SECTION_CONTEXT_MAX_SECTIONS=2
LINE_WHOLE_SECTION_CONTEXT_CHARS=9000
LINE_DEBUG_SEARCH_ENABLED=1
LINE_DEBUG_SEARCH_MAX_HITS=12
LINE_RETRIEVAL_QUERY_MAX_CHARS=1400
LINE_KNOWLEDGE_CANDIDATE_SNIPPETS=15
LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS=700
```

Per message, the flow is:

1. Use the current question plus short-term LINE context to create a clinical
   intent JSON plus a Hermes Clinical Search Brain plan: clinical concepts,
   target chapters, evidence targets, avoid-routes, required facets, and answer
   strategy. For example, lower-extremity arterial obstruction routes to PAD /
   ASCVD evidence in ADA S10 and ADA S12 instead of general glucose-lowering
   medication tables.
2. Use that clinical intent JSON and brain plan to create a guideline search query with likely
   English terms, abbreviations, section words, and evidence targets.
3. Search the mounted guideline Markdown files with hierarchical hybrid
   retrieval: multi-query BM25-style scoring, local hashed vector scoring,
   optional dense embedding scoring, source-aware scoring, chapter/section map chunks, recommendation chunks,
   section-aware scoring, and structured metadata tags
   such as source, year, ADA chapter, recommendation id/grade, table row type, CKD/eGFR/UACR, medication,
   MASLD/MASH, pregnancy, older adults, and hospital/perioperative context.
4. Apply clinical concept routing for common medical intents such as staging,
   treatment, screening, monitoring, indications, PAD/lower-extremity arterial
   disease, retinopathy, neuropathy, foot care, CKD, MASLD/MASH, pregnancy, and diabetes technology. This avoids adding
   a one-off keyword for every failed user phrase.
5. Use parent-child retrieval. Recommendations, text chunks, section summaries,
   and table rows rank independently, but selected hits carry the parent section excerpt so recommendations,
   rationale, table footnotes, and safety limitations are read together.
6. Merge candidates with source-balanced, coverage-aware, and MMR-style
   selection so KDIGO/AACE snippets are less likely to be crowded out by repeated
   ADA chapter snippets.
   CKD/eGFR/UACR/albuminuria questions additionally boost KDIGO candidates, while
   pharmacologic questions keep AACE/ADA medication context in the candidate set.
7. Retrieve a candidate pool, then ask the configured LLM to rerank only those candidates
   using the clinical intent JSON and decide whether the snippets cover all core
   concepts in the question.
8. Apply recursive coverage retrieval. If selected hits still miss required
   facets, the app runs targeted second-pass searches for likely missing
   sections, tables, thresholds, medications, or special populations.
9. Apply a local coverage safety net so a conservative LLM reranker cannot
   reject an answer when selected snippets already cover required facets such
   as CKD, medication, and eGFR thresholds.
10. Ask the configured LLM to organize only the selected guideline snippets into an evidence
   review, including source names, coverage gaps, and the clinical intent answer strategy.
11. For broad questions, add whole-section context from the selected guideline
    sections, so questions such as "which patients should use CGM?" can include
    the full ADA S7 CGM subsection rather than only isolated table rows.
12. Run long-context verification over the selected snippets plus parent section
    context. If the verifier still finds missing evidence, the app refuses to
    answer rather than filling gaps from model memory.
13. Generate the final Traditional Chinese LINE answer from the guideline
    snippets, evidence review, and long-context verification.

The final answer prompt still forbids the configured model from using built-in medical
knowledge, unmounted guidelines, news, or unsupported inference.

This version does not use the conditional multi-agent pipeline. The Hermes clinical
search brain runs before retrieval only, translating clinical language such as
`HHNK` to `HHS / hyperosmolar hyperglycemic state` and `酮酸中毒` to
`DKA / diabetic ketoacidosis`, then routing the search to the appropriate guideline
chapters such as ADA S16.

## Keyword Modules

Important guideline retrieval terms live in JSON modules under `keywords/`.
These files are loaded automatically and used for query expansion, source-aware
retrieval, and coverage-oriented search variants.

Current modules:

```text
keywords/ada_2026_chapters.json
keywords/core_diabetes_ada_aace.json
keywords/aace_kdigo_chapters.json
keywords/ckd_kdigo.json
keywords/complications_special_populations.json
```

The modules cover ADA 2026 chapter routing, AACE 2026 algorithm routing, KDIGO
chapter/practice-point routing, ADA/AACE diabetes care terms, CKD terms, and
cross-guideline special populations/safety terms. They connect Chinese user
language with guideline terms such as `dc26s011`, `MASLD`, `MASH`,
`NAFLD`, `NASH`, `CKD`, `DKD`, `eGFR`, `UACR`, `SGLT2 inhibitor`, `GLP-1 RA`,
`finerenone`, `ASCVD`, `heart failure`, `hypoglycemia`, `GDM`, `older adults`,
`perioperative`, and `foot care`.

Each entry uses this shape:

```json
{
  "id": "ckd_staging_albuminuria",
  "triggers": ["腎", "CKD", "eGFR"],
  "expansions": ["chronic kidney disease", "UACR", "albuminuria"],
  "variant_queries": ["KDIGO CKD classification eGFR albuminuria UACR"]
}
```

Optional additional keyword files can be mounted with:

```bash
LINE_KEYWORD_PATHS=/app/data/keywords
```

The health check reports `knowledge.keyword_files`, `knowledge.keyword_entries`,
`knowledge.metadata_tagged_chunks`, `knowledge.ontology_tagged_chunks`,
`knowledge.inverted_index_terms`, `knowledge.vector_index_chunks`, and
`knowledge.dense_vector_index_chunks` so
deployment can verify the modules and hybrid retrieval index are loaded.

## Debug Search

Use `/debug/search` to inspect why a question can or cannot be answered:

```text
/debug/search?q=糖尿病的新科技——連續血糖監測，適用哪些病人呢？
/debug/search?q=...&llm=true
```

The response includes the retrieval query, query variants, required facets,
candidate hits, selected hits, recursive coverage notes, whole-section context
notes, and missing facets. If `LINE_DEBUG_TOKEN` is set, include it as the
`x-debug-token` header.

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

## Zeabur Deployment Repo

Zeabur is expected to bind the fork under the `zinojeng` GitHub account:

```text
https://github.com/zinojeng/line-lifebot-qa
```

The original working repo remains:

```text
https://github.com/clawbot4ander-design/line-lifebot-qa
```

On the Mac mini, keep two remotes:

```bash
git remote add zeabur https://github.com/zinojeng/line-lifebot-qa.git
```

If the `zeabur` remote already exists:

```bash
git remote set-url zeabur https://github.com/zinojeng/line-lifebot-qa.git
```

After each code change, push both remotes:

```bash
git push origin main && git push zeabur main
```

`origin` updates the original repo. `zeabur` updates the fork that triggers
Zeabur auto deploy.

Zeabur source settings should be:

```text
Repository: zinojeng/line-lifebot-qa
Branch: main
Root directory: blank
Monitor path: *
Dockerfile override: blank
ENTRYPOINT/CMD: blank
```

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
  "app_version": "2026-05-01-no-multiagent-hermes-brain-v25",
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
    "chapter_section_index": true,
    "recommendation_aware_retrieval": true,
    "table_aware_retrieval": true,
    "multi_query_retrieval": true,
    "intent_query_variants": true,
    "clinical_concept_routing": true,
    "hermes_clinical_search_brain": true,
    "metadata_indexing": true,
    "automatic_ontology_extraction": true,
    "dense_embedding_index": true,
    "coverage_aware_retrieval": true,
    "mmr_style_diversity": true,
    "local_coverage_answerability": true,
    "comparative_threshold_answering": true,
    "threshold_review_override": true,
    "deepseek_provider": true,
    "llm_reranker": true,
    "coverage_answerability_check": true,
    "parent_child_table_context": true,
    "parent_child_section_retrieval": true,
    "structured_metadata_extraction": true,
    "recursive_coverage_retrieval": true,
    "whole_section_context": true,
    "local_hashed_vector_index": true,
    "inverted_index_retrieval": true,
    "long_context_verification": true,
    "debug_search_endpoint": true,
    "guideline_strict_grounding_current": true,
    "guideline_query_planning_current": true,
    "guideline_evidence_review_current": true
  }
}
```

If LINE still replies to `I am ander` with a generic diabetes answer, the deployed
service is likely still running an older build or `LINE_MEMORY_ENABLED=0`.
