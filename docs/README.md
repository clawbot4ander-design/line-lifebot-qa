# line-lifebot-qa Technical Notes

這個資料夾保存 line-lifebot-qa 的技術設計文件。建議先用 GitHub repo 內的 `docs/` 作為主要文件來源，再視需要把穩定流程萃取成 Codex / Hermes skill。

## 建議結論

目前最佳選擇是：

```text
GitHub repo docs first, skill later.
```

原因：

- GitHub repo 會跟程式碼、Zeabur 部署、版本 commit 一起同步。
- 文件可以被所有 collaborator 看到，不依賴某台 Mac mini 的本機 Codex skill。
- 技術設計仍在快速演進，先放 docs 比較容易 review、改版、回溯。
- 等流程穩定後，再把「如何 debug 搜尋、如何新增 clinical concept、如何驗證 Zeabur」做成 skill。

## 文件列表

- [Repo vs Skills 評估](./repo-vs-skills-evaluation.md)
- [Guideline-aware RAG 架構](./guideline-rag-architecture.md)
- [Hermes Clinical Search Brain](./hermes-clinical-search-brain.md)
- [搜尋速度優化](./search-speed-optimization.md)
- [GitHub / Zeabur 維運筆記](./github-zeabur-operations.md)

