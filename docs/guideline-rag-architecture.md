# Guideline-aware Hierarchical Hybrid RAG 架構

## 目標

line-lifebot-qa 不是一般 chatbot。它的目標是：

```text
使用者自然語言問題
→ 找到 ADA / AACE / KDIGO 指南中的相關證據
→ 用繁體中文回答
→ 不用模型記憶硬補醫療建議
```

## 為什麼不能只用單純 RAG

醫學指南問題常常跨章節。例如 T2D + CKD 用藥可能涉及：

- ADA S9 pharmacologic approaches
- ADA S10 cardiovascular risk
- ADA S11 CKD
- KDIGO diabetes and CKD
- AACE T2D algorithm
- eGFR / UACR / medication safety table

單純 top-k chunks 很容易只抓到一個相似片段，漏掉正式 recommendation、表格 footnote 或特殊族群。

## 目前索引層

目前 repo 已經實作多層索引：

```text
section_summary
recommendation
text
table_row
whole_section context
metadata / ontology tags
local hashed vector index
optional dense embedding index
inverted index retrieval
```

## Retrieval pipeline

目前流程：

```text
User question
↓
Clinical intent + Hermes Clinical Search Brain
↓
Query expansion / concept routing / multi-query variants
↓
Inverted index candidate retrieval
↓
BM25-style scoring + local hashed vector scoring
↓
Domain adjustment + source-aware scoring
↓
Coverage-aware reranking
↓
Recursive coverage retrieval
↓
Whole-section parent context
↓
Evidence review / long-context verification
↓
Grounded Traditional Chinese answer
```

## 目前保留的安全設計

- strict grounding 預設開啟。
- 如果 retrieved snippets 不足，系統應拒答。
- LLM 可協助理解與整理，但不能用內建知識補醫療建議。
- `/debug/search` 可檢查 retrieval query、query variants、required facets、missing facets、selected hits。

## 後續可做

- 將 dense embedding cache 搬到 persistent storage。
- 建立 regression question set。
- 讓 long-context verification 只在 high-risk 或 coverage 不足時啟動。
- 將 stable maintenance workflow 做成 Codex skill。

