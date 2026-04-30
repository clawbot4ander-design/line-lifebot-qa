# 搜尋速度優化

## 目前瓶頸

搜尋慢主要來自兩層：

1. 本地 retrieval
2. LLM pipeline

本地 retrieval 之前的問題是：

```text
每個 query variant
→ 掃全部 chunks
→ 計算 BM25-style score / hashed vector score / domain adjustment
```

當 guideline chunks 約 4,000 多個，multi-query variants 一多，就會變慢。

LLM pipeline 則包含：

```text
clinical intent
query planning
LLM rerank
evidence review
long-context verification
final answer
```

這些 API 呼叫本身也會增加 latency。

## 已做優化：Inverted Index Retrieval

版本：

```text
2026-05-01-fast-index-v23
```

新增：

```text
inverted_index_retrieval
```

以前：

```text
每個 query variant → 掃全部 4351 chunks
```

現在：

```text
每個 query variant → 先用倒排索引找候選 chunks → 只對候選 chunks 計分
```

## Benchmark

本地 4 個核心問題：

- PAD / 下肢動脈阻塞
- 視網膜病變分期與治療
- CGM 適用對象
- T2D + CKD + eGFR 25 用藥選擇

結果：

```text
倒排索引關閉：64.8 秒
倒排索引開啟：24.9 秒
```

約快：

```text
2.6x
```

Warm process 下不同候選量：

```text
target 1200 chunks: 24.9 秒
target 800 chunks: 21.0 秒
target 600 chunks: 19.0 秒
```

目前建議 Zeabur 預設：

```bash
LINE_KNOWLEDGE_INVERTED_INDEX_ENABLED=1
LINE_KNOWLEDGE_POSTING_MAX_TOKENS=72
LINE_KNOWLEDGE_POSTING_TARGET_CHUNKS=800
```

若想更快可試：

```bash
LINE_KNOWLEDGE_POSTING_TARGET_CHUNKS=600
```

## 下一步速度優化

最有效的下一步不是再縮 retrieval，而是做 adaptive LLM pipeline：

```text
高信心 / coverage 完整
→ 跳過 evidence review 或 long-context verification

低信心 / 高風險 / missing facets
→ 啟用 evidence review + long-context verification
```

可選 fast mode：

```bash
LINE_EVIDENCE_REVIEW_ENABLED=0
LINE_LONG_CONTEXT_VERIFICATION_ENABLED=0
```

但這會降低醫療安全檢查，建議只在測試或低風險情境使用。

