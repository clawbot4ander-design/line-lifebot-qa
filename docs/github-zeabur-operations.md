# GitHub / Zeabur 維運筆記

## Remote 設定

目前 repo 有兩個 remote：

```bash
origin  https://github.com/clawbot4ander-design/line-lifebot-qa.git
zeabur  https://github.com/zinojeng/line-lifebot-qa.git
```

Zeabur 建議綁：

```text
zinojeng/line-lifebot-qa
branch: main
```

推送時：

```bash
git push origin main
git push zeabur main
```

## 為什麼要推 zeabur remote

Zeabur 目前看得到 `zinojeng` 的 repos。如果 Zeabur 綁的是 fork：

```text
zinojeng/line-lifebot-qa
```

那只 push 到：

```text
clawbot4ander-design/line-lifebot-qa
```

Zeabur 不一定會自動部署。

## guideline folder layout

建議 Zeabur container 中：

```text
/app/data/ada
/app/data/aace
/app/data/kdigo
```

環境變數：

```bash
LINE_KNOWLEDGE_DIRS=/app/data,/app/data/ada,/app/data/aace,/app/data/kdigo,/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines
```

## Health check

開啟：

```text
/
```

應確認：

```json
{
  "app_version": "2026-05-01-fast-index-v23",
  "features": {
    "multi_guideline_sources": true,
    "hermes_clinical_search_brain": true,
    "inverted_index_retrieval": true,
    "debug_search_endpoint": true
  }
}
```

knowledge 區塊應看到：

```text
sources
source_file_counts
chunk_type_counts
metadata_tagged_chunks
ontology_tagged_chunks
inverted_index_terms
vector_index_chunks
```

## Debug search

若搜尋答不出來，先看：

```text
/debug/search?q=問題
```

若有設定 `LINE_DEBUG_TOKEN`，需帶 header：

```text
x-debug-token: <token>
```

重點看：

- `clinical_intent`
- `query_variants`
- `required_facets`
- `covered_facets`
- `missing_facets`
- `selected_hits`
- `whole_section_note`

