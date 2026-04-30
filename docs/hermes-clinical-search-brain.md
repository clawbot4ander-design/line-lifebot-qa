# Hermes Clinical Search Brain

## 核心問題

使用者不會總是使用 guideline 原文。

例如：

```text
下肢的動脈阻塞的話，臨床證據顯示的藥物治療有哪些？
```

系統不能只看到「藥物治療」就搜尋 ADA S9 glucose-lowering medication table。

它應該先理解為：

```text
PAD / lower-extremity arterial disease / ASCVD / diabetic foot PAD
```

## Brain plan 輸出

Hermes Clinical Search Brain 在搜尋前產生：

```text
concepts
target_chapters
evidence_targets
avoid_routes
required_facets
search_queries
```

## PAD 例子

對「下肢動脈阻塞」類問題，brain plan 會導向：

```text
target_chapters:
- ADA S10 Cardiovascular Disease and Risk Management
- ADA S12 Retinopathy, Neuropathy, and Foot Care

evidence_targets:
- PAD and ASCVD secondary prevention
- antiplatelet therapy
- aspirin / clopidogrel
- low-dose rivaroxaban plus aspirin when appropriate
- statin / lipid lowering
- blood pressure management
- smoking cessation
- semaglutide / GLP-1 RA limb outcome evidence
- ABI / toe pressure / vascular assessment / revascularization

avoid_routes:
- do not answer PAD drug therapy from general glucose-lowering medication tables alone
- do not treat lower-extremity arterial obstruction as neuropathy-only foot care
```

## 為什麼這比補 keyword 好

補 keyword 是：

```text
塞住 → PAD
阻塞 → PAD
跛行 → PAD
腳血管不好 → PAD
```

這會永遠補不完。

Clinical Search Brain 是：

```text
辨識「下肢血管疾病」這個臨床概念
→ 直接走 PAD / ASCVD 搜尋路徑
```

所以之後使用者說：

- 腳血管塞住
- 下肢動脈阻塞
- 走路小腿痛
- 間歇性跛行
- 下肢缺血
- 糖尿病腳血液循環不好

都應該進同一個 clinical concept。

## 目前版本

目前版本：

```text
2026-05-01-clinical-brain-v22
```

相關 feature flag：

```json
"hermes_clinical_search_brain": true
```

