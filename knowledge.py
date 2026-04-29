from __future__ import annotations

from dataclasses import dataclass
import html
from pathlib import Path
import math
import os
import re
import threading
from typing import Iterable


DEFAULT_KNOWLEDGE_DIR = os.getenv("LINE_KNOWLEDGE_DIR", "/app/data/adaguidelines")
DEFAULT_EXTRA_KNOWLEDGE_PATHS = (
    "/app/data/AACE 2026.md,"
    "/app/data/KDIGO-2026-Diabetes-and-CKD-Guideline-Update-Public-Review-Draft-March-2026.md"
)

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9+-]*|\d+(?:\.\d+)?|[\u4e00-\u9fff]{1,4}")
HEADING_RE = re.compile(r"^#{1,4}\s+(.+)$")

QUERY_EXPANSIONS: dict[str, tuple[str, ...]] = {
    "血糖": ("glucose", "glycemic", "hyperglycemia", "hypoglycemia", "blood glucose"),
    "低血糖": ("hypoglycemia", "glucagon", "level 1", "level 2", "level 3"),
    "高血糖": ("hyperglycemia", "glucose", "DKA", "HHS", "ketone"),
    "酮酸": ("ketoacidosis", "DKA", "ketone"),
    "飯": ("meal", "nutrition", "postprandial", "carbohydrate"),
    "飲食": ("nutrition", "diet", "medical nutrition therapy", "carbohydrate", "meal"),
    "運動": ("physical activity", "exercise", "sedentary", "fitness"),
    "藥": ("pharmacologic", "medication", "insulin", "metformin", "GLP-1", "SGLT2"),
    "胰島素": ("insulin", "hypoglycemia", "injection"),
    "腎": ("kidney", "CKD", "albuminuria", "eGFR", "renal"),
    "腎絲球": ("eGFR", "estimated glomerular filtration rate", "GFR", "kidney function"),
    "過濾率": ("eGFR", "estimated glomerular filtration rate", "GFR", "kidney function"),
    "腎病變": ("CKD", "chronic kidney disease", "kidney outcomes", "albuminuria", "eGFR"),
    "腎衰竭": ("kidney failure", "advanced CKD", "stage G5", "dialysis", "eGFR"),
    "洗腎": ("dialysis", "kidney failure", "stage G5"),
    "透析": ("dialysis", "kidney failure", "stage G5"),
    "GLP": ("GLP-1", "GLP-1 RA", "glucagon-like peptide 1 receptor agonist", "semaglutide"),
    "眼": ("retinopathy", "eye", "ophthalmologist", "retinal"),
    "腳": ("foot", "neuropathy", "ulcer", "podiatrist"),
    "心臟": ("cardiovascular", "heart", "ASCVD", "blood pressure", "lipid"),
    "血壓": ("blood pressure", "hypertension"),
    "膽固醇": ("lipid", "cholesterol", "statin", "triglyceride"),
    "懷孕": ("pregnancy", "gestational", "preconception"),
    "兒童": ("children", "adolescents", "pediatric", "youth"),
    "老人": ("older adults", "geriatric", "frailty"),
    "住院": ("hospital", "inpatient", "admission"),
    "篩檢": ("screening", "diagnosis", "A1C", "fasting plasma glucose"),
    "診斷": ("diagnosis", "classification", "A1C", "OGTT"),
    "併發症": ("complications", "retinopathy", "kidney", "neuropathy", "cardiovascular"),
    "體重": ("weight", "obesity", "lifestyle", "weight management"),
    "血糖機": ("blood glucose monitoring", "BGM", "glucose meter"),
    "連續血糖": ("continuous glucose monitoring", "CGM"),
}


@dataclass(frozen=True)
class KnowledgeChunk:
    source: str
    source_label: str
    title: str
    section: str
    chunk_type: str
    text: str
    tokens: tuple[str, ...]


@dataclass(frozen=True)
class KnowledgeHit:
    source: str
    source_label: str
    title: str
    section: str
    chunk_type: str
    excerpt: str
    score: float


class KnowledgeBase:
    def __init__(self, root: Path, extra_paths: list[Path] | None = None, chunk_chars: int = 1800) -> None:
        self.root = root
        self.extra_paths = extra_paths or []
        self.chunk_chars = chunk_chars
        self.chunks: list[KnowledgeChunk] = []
        self.source_files: list[Path] = []
        self.document_frequency: dict[str, int] = {}
        self.average_length = 1.0
        self.load()

    def load(self) -> None:
        chunks: list[KnowledgeChunk] = []
        source_files = knowledge_source_files(self.root, self.extra_paths)
        for path in source_files:
            chunks.extend(self._chunks_from_file(path))
        self.source_files = source_files
        self.chunks = chunks

        df: dict[str, int] = {}
        lengths = []
        for chunk in chunks:
            unique = set(chunk.tokens)
            lengths.append(len(chunk.tokens))
            for token in unique:
                df[token] = df.get(token, 0) + 1
        self.document_frequency = df
        self.average_length = sum(lengths) / len(lengths) if lengths else 1.0

    def _chunks_from_file(self, path: Path) -> list[KnowledgeChunk]:
        text = path.read_text(encoding="utf-8", errors="ignore")
        title = path.stem
        source_label = guideline_source_label(path.name)
        current_section = ""
        blocks: list[tuple[str, list[str]]] = []
        section_lines: list[str] = []

        for raw in text.splitlines():
            line = raw.strip()
            heading = HEADING_RE.match(line)
            if heading:
                if section_lines:
                    blocks.append((current_section, section_lines))
                    section_lines = []
                current_section = normalize_heading(heading.group(1))
                if current_section and title == path.stem:
                    title = current_section
                continue
            if line and not line.startswith("> *") and "Downloaded" not in line:
                section_lines.append(line)
        if section_lines:
            blocks.append((current_section, section_lines))

        chunks: list[KnowledgeChunk] = []
        for section, lines in blocks:
            if section.lower() in {"references", "reference"}:
                continue
            buffer: list[str] = []
            size = 0
            for line in lines:
                if size + len(line) > self.chunk_chars and buffer:
                    chunk_text = "\n".join(buffer)
                    chunks.append(
                        KnowledgeChunk(
                            path.name,
                            source_label,
                            title,
                            section or title,
                            "text",
                            chunk_text,
                            tuple(tokenize(chunk_text)),
                        )
                    )
                    buffer = []
                    size = 0
                buffer.append(line)
                size += len(line) + 1
            if buffer:
                chunk_text = "\n".join(buffer)
                chunks.append(
                    KnowledgeChunk(
                        path.name,
                        source_label,
                        title,
                        section or title,
                        "text",
                        chunk_text,
                        tuple(tokenize(chunk_text)),
                    )
                )
            chunks.extend(table_chunks_from_lines(path.name, source_label, title, section or title, lines))
        return chunks

    def search(self, query: str, limit: int = 3, excerpt_chars: int = 520) -> list[KnowledgeHit]:
        query_tokens = list(expand_query_tokens(query))
        if not query_tokens or not self.chunks:
            return []

        scored: list[tuple[float, KnowledgeChunk]] = []
        for chunk in self.chunks:
            score = self._score(query_tokens, chunk)
            score *= domain_adjustment(query, chunk)
            if score > 0:
                scored.append((score, chunk))
        scored.sort(key=lambda item: item[0], reverse=True)

        hits: list[KnowledgeHit] = []
        seen_sources: set[tuple[str, str]] = set()
        for score, chunk in scored:
            key = (chunk.source, chunk.section)
            if key in seen_sources:
                continue
            seen_sources.add(key)
            hits.append(
                KnowledgeHit(
                    source=chunk.source,
                    source_label=chunk.source_label,
                    title=chunk.title,
                    section=chunk.section,
                    chunk_type=chunk.chunk_type,
                    excerpt=best_excerpt(chunk.text, query_tokens, excerpt_chars),
                    score=score,
                )
            )
            if len(hits) >= limit:
                break
        return hits

    def _score(self, query_tokens: list[str], chunk: KnowledgeChunk) -> float:
        token_counts: dict[str, int] = {}
        for token in chunk.tokens:
            token_counts[token] = token_counts.get(token, 0) + 1

        score = 0.0
        doc_count = len(self.chunks)
        chunk_len = max(len(chunk.tokens), 1)
        k1 = 1.4
        b = 0.72
        for token in query_tokens:
            tf = token_counts.get(token, 0)
            if not tf:
                continue
            df = self.document_frequency.get(token, 0)
            idf = math.log(1 + (doc_count - df + 0.5) / (df + 0.5))
            denom = tf + k1 * (1 - b + b * chunk_len / self.average_length)
            score += idf * (tf * (k1 + 1) / denom)
        return score


_knowledge_lock = threading.Lock()
_knowledge_cache: KnowledgeBase | None = None
_knowledge_cache_key: tuple[str, int] | None = None


def knowledge_enabled() -> bool:
    return os.getenv("LINE_KNOWLEDGE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}


def knowledge_strict_enabled() -> bool:
    return os.getenv("LINE_KNOWLEDGE_STRICT", "1").strip().lower() not in {"0", "false", "no", "off"}


def knowledge_dir() -> Path:
    return Path(os.getenv("LINE_KNOWLEDGE_DIR", DEFAULT_KNOWLEDGE_DIR)).expanduser()


def extra_knowledge_paths() -> list[Path]:
    raw = os.getenv("LINE_KNOWLEDGE_EXTRA_PATHS")
    if raw is None:
        raw = DEFAULT_EXTRA_KNOWLEDGE_PATHS
    if raw.strip().lower() in {"", "0", "false", "no", "off"}:
        return []
    return [Path(part.strip()).expanduser() for part in re.split(r"[,;\n]+", raw) if part.strip()]


def knowledge_source_files(root: Path, extra_paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    if root.exists():
        files.extend(sorted(path for path in root.glob("*.md") if path.is_file()))
    files.extend(path for path in extra_paths if path.exists() and path.is_file())

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in files:
        key = str(path.resolve()) if path.exists() else str(path)
        if key not in seen:
            seen.add(key)
            deduped.append(path)
    return deduped


def guideline_source_label(source_name: str) -> str:
    lower = source_name.lower()
    if "kdigo" in lower:
        return "KDIGO 2026 Diabetes and CKD Guideline Update"
    if "aace" in lower:
        return "AACE 2026"
    if "ada" in lower or re.search(r"dc26s\d+", lower):
        return "ADA Standards of Care in Diabetes 2026"
    return "本地糖尿病指南知識庫"


def public_metadata(value: str) -> str:
    value = re.sub(r"public\s+review\s+draft", "", value, flags=re.I)
    value = re.sub(r"\bdraft\b", "", value, flags=re.I)
    value = re.sub(r"\s+", " ", value)
    value = value.replace(" - ", " ").replace("--", "-")
    return value.strip(" -_")


def table_chunks_from_lines(
    source: str,
    source_label: str,
    title: str,
    section: str,
    lines: list[str],
) -> list[KnowledgeChunk]:
    chunks: list[KnowledgeChunk] = []
    table_label = ""
    row_buffer: list[str] = []
    in_html_row = False

    for line in lines:
        label_match = re.search(r"\b(Table\s+\d+(?:\.\d+)?[^<\n]*)", line, flags=re.I)
        if label_match:
            table_label = clean_cell_text(label_match.group(1))[:160]

        lowered = line.lower()
        rows: list[list[str]] = []
        if "<tr" in lowered:
            in_html_row = True
            row_buffer = [line]
        elif in_html_row:
            row_buffer.append(line)

        if in_html_row and "</tr>" in lowered:
            rows = table_rows_from_html(" ".join(row_buffer))
            in_html_row = False
            row_buffer = []
        elif not in_html_row:
            rows = markdown_table_rows_from_line(line)

        for cells in rows:
            if len(cells) < 2:
                continue
            row_text = " | ".join(cells)
            if not row_text or re.fullmatch(r"[-:| ]+", row_text):
                continue
            prefix = f"{table_label}: " if table_label else "Table row: "
            chunk_text = prefix + row_text
            chunks.append(
                KnowledgeChunk(
                    source,
                    source_label,
                    title,
                    section,
                    "table_row",
                    chunk_text,
                    tuple(tokenize(chunk_text)),
                )
            )
    return chunks


def table_rows_from_html(value: str) -> list[list[str]]:
    rows: list[list[str]] = []
    row_matches = re.findall(r"<tr[^>]*>(.*?)</tr>", value, flags=re.I | re.S)
    for row in row_matches or [value]:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, flags=re.I | re.S)
        cleaned = [clean_cell_text(cell) for cell in cells if clean_cell_text(cell)]
        if cleaned:
            rows.append(cleaned)
    return rows


def markdown_table_rows_from_line(line: str) -> list[list[str]]:
    stripped = line.strip()
    if "|" not in stripped or re.fullmatch(r"\|?\s*[-:| ]+\s*\|?", stripped):
        return []
    cells = [clean_cell_text(cell) for cell in stripped.strip("|").split("|")]
    return [[cell for cell in cells if cell]]


def clean_cell_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" •\t\r\n")


def load_knowledge_base() -> KnowledgeBase | None:
    if not knowledge_enabled():
        return None
    root = knowledge_dir()
    extras = extra_knowledge_paths()
    chunk_chars = int(os.getenv("LINE_KNOWLEDGE_CHUNK_CHARS", "1800"))
    if not root.exists() and not any(path.exists() for path in extras):
        return None

    global _knowledge_cache, _knowledge_cache_key
    cache_key = ("|".join([str(root), *[str(path) for path in extras]]), chunk_chars)
    if _knowledge_cache and _knowledge_cache_key == cache_key:
        return _knowledge_cache
    with _knowledge_lock:
        if _knowledge_cache and _knowledge_cache_key == cache_key:
            return _knowledge_cache
        _knowledge_cache = KnowledgeBase(root, extra_paths=extras, chunk_chars=chunk_chars)
        _knowledge_cache_key = cache_key
        return _knowledge_cache


def search_knowledge(query: str) -> list[KnowledgeHit]:
    kb = load_knowledge_base()
    if not kb:
        return []
    limit = int(os.getenv("LINE_KNOWLEDGE_MAX_SNIPPETS", "3"))
    excerpt_chars = int(os.getenv("LINE_KNOWLEDGE_EXCERPT_CHARS", "520"))
    if knowledge_strict_enabled():
        limit = max(limit, int(os.getenv("LINE_KNOWLEDGE_STRICT_MIN_SNIPPETS", "5")))
        excerpt_chars = max(excerpt_chars, int(os.getenv("LINE_KNOWLEDGE_STRICT_EXCERPT_CHARS", "900")))
    return kb.search(query, limit=limit, excerpt_chars=excerpt_chars)


def search_knowledge_candidates(query: str) -> list[KnowledgeHit]:
    kb = load_knowledge_base()
    if not kb:
        return []
    limit = int(os.getenv("LINE_KNOWLEDGE_CANDIDATE_SNIPPETS", "15"))
    excerpt_chars = int(os.getenv("LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS", "700"))
    return kb.search(query, limit=limit, excerpt_chars=excerpt_chars)


def knowledge_no_answer_text() -> str:
    return (
        "目前我在已載入的糖尿病指南知識庫中，找不到足夠直接的依據回答這個問題。"
        "為了避免提供不準確的資訊，我先不延伸回答。"
        "若這是個人健康、用藥、急症或檢查判讀問題，請以你的醫療團隊評估為準。"
    )


def knowledge_answerable(query: str) -> bool:
    if not knowledge_strict_enabled():
        return True
    return bool(search_knowledge_candidates(query))


def knowledge_prompt(query: str) -> str:
    return knowledge_prompt_from_hits(search_knowledge(query))


def knowledge_prompt_from_hits(hits: list[KnowledgeHit]) -> str:
    if not hits:
        if knowledge_strict_enabled():
            return (
                "\n\n背景知識檢索：沒有找到足夠相關的糖尿病指南片段。"
                "\n嚴格回答規則：請不要使用模型內建知識、一般醫學常識或推測補完；"
                f"請只回覆這段文字：{knowledge_no_answer_text()}"
            )
        return (
            "\n\n背景知識檢索：沒有找到足夠相關的糖尿病指南片段。"
            "\n回答時請只給一般衛教原則，並說明需要醫療團隊依個人狀況判斷。"
        )

    lines = [
        "\n\n背景知識檢索：以下為本次問題相關的糖尿病指南片段，可能包含 ADA、AACE 或 KDIGO 等來源。",
        "嚴格回答規則：只能根據以下片段回答；不要使用模型內建知識、一般醫學常識或推測補完。",
        "若以下片段不足以直接回答使用者問題，請明確說指南片段不足，並停止回答，不要改用其他來源補充。",
        "回答方式：先用 1 句話直接回答，再用 2 到 4 個重點整理指南片段支持的內容；若有藥物限制或 eGFR 門檻，請清楚列出，但不要提供個人化劑量。",
        "來源標示：回答中請自然標示依據來源，例如「根據 ADA 2026 片段」、「根據 KDIGO 2026 片段」或「根據 AACE 2026 片段」；不要使用 draft 或 public review draft 字樣。",
    ]
    for index, hit in enumerate(hits, start=1):
        lines.extend(
            [
                f"\n[{index}] {public_metadata(hit.title)}",
                f"來源指南：{hit.source_label}",
                f"章節：{public_metadata(hit.section)}",
                f"片段類型：{hit.chunk_type}",
                f"片段：{hit.excerpt}",
            ]
        )
    return "\n".join(lines)


def knowledge_candidates_prompt(hits: list[KnowledgeHit]) -> str:
    if not hits:
        return "\n\n候選指南片段：無。"
    lines = [
        "\n\n候選指南片段：以下為初步召回的候選片段，請只用來做 rerank/coverage，不可用模型內建知識補充。",
    ]
    for index, hit in enumerate(hits, start=1):
        lines.extend(
            [
                f"\n[{index}] {public_metadata(hit.title)}",
                f"來源指南：{hit.source_label}",
                f"章節：{public_metadata(hit.section)}",
                f"片段類型：{hit.chunk_type}",
                f"召回分數：{hit.score:.2f}",
                f"片段：{hit.excerpt}",
            ]
        )
    return "\n".join(lines)


def knowledge_status() -> dict[str, object]:
    kb = load_knowledge_base()
    root = knowledge_dir()
    extras = extra_knowledge_paths()
    extra_existing = [path for path in extras if path.exists() and path.is_file()]
    return {
        "enabled": knowledge_enabled(),
        "dir": str(root),
        "extra_paths": [str(path) for path in extras],
        "available": bool(kb),
        "strict": knowledge_strict_enabled(),
        "chunks": len(kb.chunks) if kb else 0,
        "files": len(kb.source_files) if kb else 0,
        "dir_files": len(list(root.glob("*.md"))) if root.exists() else 0,
        "extra_files": len(extra_existing),
        "sources": sorted({chunk.source_label for chunk in kb.chunks}) if kb else [],
    }


def tokenize(text: str) -> Iterable[str]:
    for token in TOKEN_RE.findall(text.lower()):
        token = token.strip()
        if len(token) <= 1 and not token.isdigit() and not re.match(r"[\u4e00-\u9fff]", token):
            continue
        yield token


def expand_query_tokens(query: str) -> Iterable[str]:
    yielded: set[str] = set()
    expanded = [query]
    for key, terms in QUERY_EXPANSIONS.items():
        if key in query:
            expanded.extend(terms)
    for token in tokenize(" ".join(expanded)):
        if token not in yielded:
            yielded.add(token)
            yield token


def normalize_heading(value: str) -> str:
    value = re.sub(r"\s+", " ", value)
    return value.replace("*", "").replace("_", "").strip()


def best_excerpt(text: str, query_tokens: list[str], max_chars: int) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_chars:
        return compact

    sentence_excerpt = best_sentence_excerpt(compact, query_tokens, max_chars)
    if sentence_excerpt:
        return sentence_excerpt

    lowered = compact.lower()
    positions = [lowered.find(token.lower()) for token in query_tokens if lowered.find(token.lower()) >= 0]
    center = min(positions) if positions else 0
    start = max(0, center - max_chars // 3)
    end = min(len(compact), start + max_chars)
    start = max(0, end - max_chars)
    excerpt = compact[start:end].strip()
    if start > 0:
        excerpt = "..." + excerpt
    if end < len(compact):
        excerpt += "..."
    return excerpt


def best_sentence_excerpt(text: str, query_tokens: list[str], max_chars: int) -> str:
    sentences = [part.strip() for part in re.split(r"(?<=[.!?。！？])\s+", text) if part.strip()]
    if len(sentences) < 2:
        return ""

    lowered_tokens = [token.lower() for token in query_tokens]
    best_score = 0
    best_index = -1
    for index, sentence in enumerate(sentences):
        lowered = sentence.lower()
        score = sum(1 for token in lowered_tokens if token and token in lowered)
        if "glp-1" in lowered and any(token in lowered_tokens for token in ["egfr", "ckd", "kidney", "renal"]):
            score += 4
        elif "glp-1" in lowered:
            score += 2
        if score > best_score:
            best_score = score
            best_index = index

    if best_index < 0 or best_score == 0:
        return ""

    selected = [sentences[best_index]]
    left = best_index - 1
    right = best_index + 1
    while len(" ".join(selected)) < max_chars and (left >= 0 or right < len(sentences)):
        if left >= 0:
            candidate = sentences[left]
            if len(" ".join([candidate, *selected])) <= max_chars:
                selected.insert(0, candidate)
            left -= 1
        if len(" ".join(selected)) >= max_chars:
            break
        if right < len(sentences):
            candidate = sentences[right]
            if len(" ".join([*selected, candidate])) <= max_chars:
                selected.append(candidate)
            right += 1

    excerpt = " ".join(selected).strip()
    if left >= 0:
        excerpt = "..." + excerpt
    if right < len(sentences):
        excerpt += "..."
    return excerpt


def domain_adjustment(query: str, chunk: KnowledgeChunk) -> float:
    haystack = f"{chunk.source} {chunk.source_label} {chunk.title} {chunk.section} {chunk.text[:700]}".lower()
    query_lower = query.lower()
    adjustment = 1.0

    if chunk.chunk_type == "table_row":
        adjustment *= 1.25
    if re.search(r"\b(reference|references|acknowledg|appendix)\b", haystack):
        adjustment *= 0.35
    if re.search(r"\b(recommendation|recommendations|treatment|therapy|selection|screening|diagnosis|pharmacologic|management|interventions)\b", haystack):
        adjustment *= 1.18
    if re.search(r"\b(egfr|albuminuria|uacr|mg/g|ml/min|contraindicat|avoid|dose|dosage|adjust|threshold|initiat|discontinu)\b", haystack):
        adjustment *= 1.18

    if "住院" not in query and "hospital" in haystack:
        adjustment *= 0.55
    if ("兒童" not in query and "青少年" not in query and "孩子" not in query) and (
        "children" in haystack or "adolescents" in haystack
    ):
        adjustment *= 0.7
    if "懷孕" not in query and "妊娠" not in query and "孕" not in query and "pregnancy" in haystack:
        adjustment *= 0.7

    if "低血糖" in query and ("dc26s006" in haystack or "hypoglycemia" in haystack):
        adjustment *= 1.45
    if ("飲食" in query or "吃" in query or "飯" in query) and (
        "dc26s005" in haystack
        or "eating patterns" in haystack
        or "meal planning" in haystack
        or "nutrition therapy" in haystack
    ):
        adjustment *= 1.6
    if ("腎" in query or "尿蛋白" in query or "ckd" in query_lower or "egfr" in query_lower) and (
        "kdigo" in haystack or "dc26s011" in haystack or "kidney" in haystack or "chronic kidney disease" in haystack
    ):
        adjustment *= 1.5
    if ("腎" in query or "egfr" in query_lower or "腎絲球" in query or "腎衰竭" in query) and (
        "dc26s009" in haystack
        or "dc26s011" in haystack
        or "kdigo" in haystack
        or "chronic kidney disease" in haystack
        or "glucose-lowering therapy for people with chronic kidney disease" in haystack
    ):
        adjustment *= 1.45
    if "glp" in query_lower and (
        "dc26s009" in haystack
        or "dc26s011" in haystack
        or "aace" in haystack
        or "kdigo" in haystack
        or "glp-1" in haystack
        or "glucose-lowering therapy" in haystack
    ):
        adjustment *= 1.6
    if ("藥" in query or "medication" in query_lower or "pharmacologic" in query_lower) and (
        "aace" in haystack or "dc26s009" in haystack or "pharmacologic" in haystack
    ):
        adjustment *= 1.25
    if ("眼" in query or "視網膜" in query) and ("retinopathy" in haystack or "dc26s012" in haystack):
        adjustment *= 1.4
    if ("腳" in query or "足" in query) and ("foot" in haystack or "neuropathy" in haystack):
        adjustment *= 1.35
    if ("懷孕" in query or "妊娠" in query or "孕" in query) and ("dc26s015" in haystack or "pregnancy" in haystack):
        adjustment *= 1.6

    return adjustment
