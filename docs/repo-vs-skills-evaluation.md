# Repo Docs vs Skills 評估

## 結論

這一階段建議把技術文件放在 GitHub repo 的 `docs/`。

Codex / Hermes skill 可以晚一點做，而且比較適合放「操作流程」而不是完整技術文件。

## 比較

| 方案 | 優點 | 缺點 | 適合用途 |
| --- | --- | --- | --- |
| GitHub repo `docs/` | 跟程式碼一起 version control；Zeabur/GitHub/collaborator 都看得到；容易 review | 不是 agent 自動執行能力 | 架構文件、部署文件、設計決策、benchmark |
| Codex / Hermes skill | Agent 可依 SKILL.md 自動遵循流程；適合 debug checklist | 通常是本機或特定環境設定；多人同步較麻煩 | 固定工作流，例如「新增 guideline concept」「debug search」「Zeabur deploy checklist」 |
| 獨立 GitHub repo | 文件獨立乾淨；可做大型知識庫 | 跟程式碼容易不同步；現在會增加維護成本 | 多專案共用的 guideline RAG framework |

## 建議策略

現在：

```text
line-lifebot-qa/docs
```

放：

- 架構決策
- RAG pipeline
- Clinical Search Brain
- 搜尋速度優化
- GitHub / Zeabur 操作
- 測試案例與 benchmark

之後如果某些流程穩定，再建立 skill，例如：

```text
skills/hermes-guideline-rag-maintainer/SKILL.md
```

skill 可以包含：

- 新增 clinical concept 的步驟
- 如何跑 `/debug/search`
- 如何判斷是不是 retrieval failure
- 如何更新 Zeabur environment
- 如何跑 regression questions

## 為什麼現在不先做 skill

現在的系統還在快速演進：

- folder layout 剛改成 ADA / AACE / KDIGO
- retrieval 從 keyword RAG 變成 hierarchical hybrid index
- 又加入 clinical brain、ontology tags、inverted index
- Zeabur 綁定與 GitHub fork workflow 也剛穩定

若太早做 skill，容易把還在變動的流程固定死。先用 repo docs 做共同記憶，比較穩。

