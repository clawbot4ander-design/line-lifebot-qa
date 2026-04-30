# Conditional Multi-agent Pipeline

## 結論

line-lifebot-qa 採用條件式 multi-agent pipeline，而不是讓多個 agent 自由對話。

設計原則：

```text
窄任務、可觀測、可關閉、不要每次都跑全部 agent。
```

## 目前 agents

### 1. Hermes Clinical Search Brain

搜尋前的大腦。

負責：

- 將使用者白話轉成臨床概念
- 決定 target chapters
- 產生 evidence targets
- 指定 avoid routes
- 補 required facets
- 產生 search queries

例：

```text
腳血管塞住
→ PAD / lower-extremity arterial disease / ASCVD
→ ADA S10 + ADA S12
→ antiplatelet, statin, BP, smoking cessation, revascularization, limb outcomes
```

### 2. Evidence Coverage Agent

搜尋後的覆蓋度檢查。

負責比較：

```text
required facets
vs
covered facets from selected guideline hits
```

輸出：

```text
answerable
required_facets
covered_facets
missing_facets
source_labels
sections
chunk_types
```

這個 agent 是防止「看起來有片段，但其實沒有覆蓋核心問題」。

### 3. Retrieval Failure Analyzer Agent

只有 debug/失敗分析時使用。

負責判斷：

- no candidates
- no selected hits
- selected hits missing required facets
- reranker dropped covered evidence
- LLM reranker / evidence review too conservative
- whole-section context missing

輸出：

```text
reasons
suggestions
candidate_coverage
selected_coverage
```

### 4. Regression Test Agent

部署前或修改 retrieval 後手動跑。

Endpoint：

```text
/debug/regression
/debug/regression?llm=true
```

目前固定測試：

- PAD / 下肢動脈阻塞藥物治療
- 視網膜病變分期與治療
- CGM 適用對象
- T2D + CKD + eGFR 25 用藥
- MASLD / MASH 合併糖尿病治療

## 為什麼先不加更多 agents

目前不建議再加很多 agent，原因：

- 每個 agent 都會增加 latency 或 debug 複雜度。
- 現在最重要的是「知道為什麼搜尋失敗」和「防止更新後退步」。
- Coverage / Failure / Regression 已經形成品質閉環。

下一階段可考慮：

- Citation Audit Agent：確認最終答案每個重點都有來源片段。
- Ontology Builder Agent：離線掃描新 guideline，自動更新 concept profile。
- Deployment Monitor Agent：Zeabur deploy 後自動跑 health + regression。

## Feature flags

```bash
LINE_MULTI_AGENT_ENABLED=1
LINE_DEBUG_SEARCH_ENABLED=1
```

Health check：

```json
{
  "conditional_multi_agent_pipeline": true,
  "evidence_coverage_agent": true,
  "retrieval_failure_analyzer_agent": true,
  "regression_test_agent": true
}
```

