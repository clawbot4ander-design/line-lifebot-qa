from __future__ import annotations

from dataclasses import dataclass
import html
from pathlib import Path
import hashlib
import math
import os
import re
import threading
from typing import Iterable


DEFAULT_KNOWLEDGE_DIR = os.getenv("LINE_KNOWLEDGE_DIR", "/app/data/guidelines")
DEFAULT_KNOWLEDGE_DIRS = "/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines"
DEFAULT_EXTRA_KNOWLEDGE_PATHS = ""

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9+-]*|\d+(?:\.\d+)?|[\u4e00-\u9fff]{1,4}")
HEADING_RE = re.compile(r"^#{1,4}\s+(.+)$")

QUERY_EXPANSIONS: dict[str, tuple[str, ...]] = {
    "血糖": ("glucose", "glycemic", "hyperglycemia", "hypoglycemia", "blood glucose"),
    "低血糖": ("hypoglycemia", "glucagon", "level 1", "level 2", "level 3"),
    "高血糖": ("hyperglycemia", "glucose", "DKA", "HHS", "ketone"),
    "處理": ("treatment", "management", "recommendation", "action", "recheck", "repeat"),
    "治療": ("treatment", "therapy", "management", "recommendation"),
    "控制": ("goal", "target", "glycemic goals", "A1C", "blood glucose", "CGM", "BGM", "time in range"),
    "目標": ("goal", "target", "glycemic goals", "A1C goal", "glucose target", "time in range"),
    "血糖控制": ("glycemic goals", "glycemic management", "A1C", "CGM", "BGM", "time in range"),
    "血糖控制目標": ("glycemic goals", "A1C goal", "glucose target", "CGM metrics", "time in range"),
    "酮酸": ("ketoacidosis", "DKA", "ketone"),
    "飯": ("meal", "nutrition", "postprandial", "carbohydrate"),
    "飲食": ("nutrition", "diet", "medical nutrition therapy", "carbohydrate", "meal"),
    "運動": ("physical activity", "exercise", "sedentary", "fitness"),
    "藥": ("pharmacologic", "medication", "insulin", "metformin", "GLP-1", "SGLT2"),
    "胰島素": ("insulin", "hypoglycemia", "injection"),
    "腎": ("kidney", "CKD", "albuminuria", "eGFR", "renal"),
    "尿蛋白": ("albuminuria", "UACR", "urine albumin-to-creatinine ratio", "proteinuria"),
    "白蛋白尿": ("albuminuria", "UACR", "urine albumin-to-creatinine ratio"),
    "腎絲球": ("eGFR", "estimated glomerular filtration rate", "GFR", "kidney function"),
    "過濾率": ("eGFR", "estimated glomerular filtration rate", "GFR", "kidney function"),
    "腎病變": ("CKD", "chronic kidney disease", "kidney outcomes", "albuminuria", "eGFR"),
    "腎衰竭": (
        "kidney failure",
        "advanced CKD",
        "stage G5",
        "dialysis",
        "eGFR",
        "glycemic goals",
        "A1C less reliable",
    ),
    "洗腎": (
        "dialysis",
        "kidney failure",
        "stage G5",
        "glycemic goals",
        "A1C less reliable",
        "glycated albumin",
        "fructosamine",
        "CGM",
        "BGM",
    ),
    "透析": (
        "dialysis",
        "kidney failure",
        "stage G5",
        "glycemic goals",
        "A1C less reliable",
        "glycated albumin",
        "fructosamine",
        "CGM",
        "BGM",
    ),
    "GLP": ("GLP-1", "GLP-1 RA", "glucagon-like peptide 1 receptor agonist", "semaglutide"),
    "眼": ("retinopathy", "eye", "ophthalmologist", "retinal"),
    "腳": ("foot", "neuropathy", "ulcer", "podiatrist"),
    "心臟": ("cardiovascular", "heart", "ASCVD", "blood pressure", "lipid"),
    "心血管": ("cardiovascular", "ASCVD", "heart failure", "MACE", "cardiorenal"),
    "心衰竭": ("heart failure", "HF", "HFrEF", "HFpEF", "heart failure hospitalization"),
    "血壓": ("blood pressure", "hypertension"),
    "膽固醇": ("lipid", "cholesterol", "statin", "triglyceride"),
    "懷孕": ("pregnancy", "gestational", "preconception"),
    "懷孕糖尿病": ("gestational diabetes mellitus", "GDM", "screening", "diagnosis", "OGTT", "24-28 weeks"),
    "妊娠糖尿病": ("gestational diabetes mellitus", "GDM", "screening", "diagnosis", "OGTT", "24-28 weeks"),
    "兒童": ("children", "adolescents", "pediatric", "youth"),
    "老人": ("older adults", "geriatric", "frailty"),
    "住院": ("hospital", "inpatient", "admission"),
    "篩檢": ("screening", "diagnosis", "A1C", "fasting plasma glucose"),
    "診斷": ("diagnosis", "classification", "A1C", "OGTT", "diagnostic criteria"),
    "診斷標準": ("diagnostic criteria", "screening", "classification", "A1C", "fasting plasma glucose", "OGTT"),
    "併發症": ("complications", "retinopathy", "kidney", "neuropathy", "cardiovascular"),
    "體重": ("weight", "obesity", "lifestyle", "weight management"),
    "肥胖": ("obesity", "adiposity", "weight management", "anti-obesity medication", "metabolic surgery"),
    "血糖機": ("blood glucose monitoring", "BGM", "glucose meter"),
    "連續血糖": ("continuous glucose monitoring", "CGM"),
}

QUERY_INTENT_VARIANTS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (
        ("血糖控制", "控制目標", "血糖目標", "目標", "glycemic goal", "glycemic target", "glucose target"),
        (
            "glycemic goals A1C goal setting and modifying glycemic goals individualized goals hypoglycemia risk",
            "blood glucose target preprandial postprandial time in range CGM metrics BGM",
        ),
    ),
    (
        ("洗腎", "透析", "腎衰竭", "dialysis", "kidney failure", "stage g5", "eskd", "esrd"),
        (
            "dialysis kidney failure advanced CKD stage G5 A1C less reliable glycemic goals",
            "glycated albumin fructosamine CGM BGM kidney failure dialysis",
        ),
    ),
    (
        ("腎", "腎絲球", "腎病變", "尿蛋白", "egfr", "ckd", "albuminuria", "uacr"),
        (
            "chronic kidney disease CKD eGFR albuminuria UACR kidney outcomes",
            "SGLT2 GLP-1 RA finerenone kidney cardiovascular risk CKD progression",
        ),
    ),
    (
        ("藥", "用藥", "glp", "sglt", "胰島素", "insulin", "metformin", "pharmacologic", "medication"),
        (
            "pharmacologic treatment medication selection efficacy hypoglycemia risk weight kidney cardiovascular",
            "dose adjustment contraindication avoid kidney function eGFR treatment plan",
        ),
    ),
    (
        ("低血糖", "hypoglycemia"),
        (
            "hypoglycemia treatment glucose 15 minutes repeat glucagon level 1 level 2 level 3",
            "hypoglycemia risk assessment impaired awareness high risk CGM",
        ),
    ),
    (
        ("高血糖", "酮酸", "dka", "hhs", "ketone", "hyperglycemia"),
        (
            "hyperglycemic crises DKA HHS ketone diagnosis treatment emergency",
            "hyperglycemia symptoms insulin fluids ketones hospital",
        ),
    ),
    (
        ("飲食", "吃", "飯", "營養", "nutrition", "diet", "carbohydrate"),
        (
            "medical nutrition therapy eating patterns carbohydrate meal planning protein sodium",
            "nutrition therapy weight glycemic management cardiovascular kidney disease",
        ),
    ),
    (
        ("運動", "活動", "exercise", "physical activity"),
        (
            "physical activity exercise sedentary time resistance aerobic hypoglycemia prevention",
            "fitness physical function cardiometabolic health activity recommendations",
        ),
    ),
    (
        ("眼", "視網膜", "retinopathy", "eye"),
        (
            "diabetic retinopathy screening eye examination retinal treatment",
            "ophthalmologist vision pregnancy retinopathy monitoring",
        ),
    ),
    (
        ("腳", "足", "神經", "neuropathy", "foot"),
        (
            "neuropathy foot care ulcer screening monofilament peripheral arterial disease",
            "diabetic foot evaluation prevention referral",
        ),
    ),
    (
        ("心", "血壓", "膽固醇", "cardiovascular", "ascvd", "hypertension", "lipid", "statin"),
        (
            "cardiovascular disease ASCVD heart failure blood pressure lipid statin risk management",
            "hypertension treatment goal cholesterol triglyceride cardiovascular risk",
        ),
    ),
    (
        ("懷孕", "妊娠", "孕", "pregnancy", "gestational"),
        (
            "pregnancy gestational diabetes preconception glycemic goals insulin CGM",
            "management of diabetes in pregnancy screening diagnosis postpartum",
        ),
    ),
    (
        ("兒童", "青少年", "孩子", "children", "adolescents", "youth"),
        (
            "children adolescents pediatric youth type 1 type 2 diabetes management screening",
            "school technology hypoglycemia growth puberty glycemic goals",
        ),
    ),
    (
        ("老人", "長者", "older", "geriatric", "frailty"),
        (
            "older adults treatment goals frailty hypoglycemia cognitive impairment deintensification",
            "older adults A1C goal CGM BGM complex health status",
        ),
    ),
    (
        ("住院", "hospital", "inpatient"),
        (
            "hospital inpatient glycemic management insulin hypoglycemia hyperglycemia perioperative",
            "hospital care glucose target critical illness noncritical illness",
        ),
    ),
    (
        ("診斷", "篩檢", "diagnosis", "screening", "a1c", "ogtt"),
        (
            "diagnosis classification screening A1C fasting plasma glucose OGTT criteria",
            "prediabetes type 1 type 2 gestational diabetes screening diagnostic criteria",
        ),
    ),
    (
        ("血糖機", "連續血糖", "cgm", "bgm", "glucose monitoring", "technology"),
        (
            "diabetes technology CGM BGM time in range time below range time above range",
            "blood glucose monitoring continuous glucose monitoring accuracy interference",
        ),
    ),
    (
        ("體重", "肥胖", "減重", "weight", "obesity"),
        (
            "obesity weight management lifestyle pharmacotherapy metabolic surgery diabetes",
            "GLP-1 dual GIP GLP-1 weight loss obesity treatment",
        ),
    ),
)


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


@dataclass(frozen=True)
class QueryVariant:
    label: str
    text: str
    weight: float = 0.82


class KnowledgeBase:
    def __init__(self, roots: list[Path], extra_paths: list[Path] | None = None, chunk_chars: int = 1800) -> None:
        self.roots = roots
        self.root = roots[0] if roots else Path(".")
        self.extra_paths = extra_paths or []
        self.chunk_chars = chunk_chars
        self.chunks: list[KnowledgeChunk] = []
        self.source_files: list[Path] = []
        self.document_frequency: dict[str, int] = {}
        self.average_length = 1.0
        self.load()

    def load(self) -> None:
        chunks: list[KnowledgeChunk] = []
        source_files = knowledge_source_files(self.roots, self.extra_paths)
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
        source_label = guideline_source_label(path.name, text)
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
                            chunk_tokens(source_label, title, section or title, "text", chunk_text),
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
                        chunk_tokens(source_label, title, section or title, "text", chunk_text),
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
        seen_sources: set[tuple[str, ...]] = set()
        for score, chunk in scored:
            key = chunk_dedup_key(chunk)
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

    def search_multi(self, query: str, limit: int = 3, excerpt_chars: int = 520) -> list[KnowledgeHit]:
        variants = query_variant_specs(query)
        candidates: dict[tuple[str, ...], KnowledgeHit] = {}
        for variant in variants:
            variant_limit = max(limit * 2, limit + 8)
            for rank, hit in enumerate(self.search(variant.text, limit=variant_limit, excerpt_chars=excerpt_chars), start=1):
                key = hit_dedup_key(hit)
                fused_score = hit.score * variant.weight + 35.0 / (rank + 1)
                existing = candidates.get(key)
                if not existing or fused_score > existing.score:
                    candidates[key] = KnowledgeHit(
                        source=hit.source,
                        source_label=hit.source_label,
                        title=hit.title,
                        section=hit.section,
                        chunk_type=hit.chunk_type,
                        excerpt=hit.excerpt,
                        score=fused_score,
                    )
        return coverage_rerank_hits(query, list(candidates.values()), limit)

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
    return knowledge_dirs()[0]


def knowledge_dirs() -> list[Path]:
    raw = os.getenv("LINE_KNOWLEDGE_DIRS")
    if raw is None:
        legacy_dir = os.getenv("LINE_KNOWLEDGE_DIR")
        raw = legacy_dir if legacy_dir else DEFAULT_KNOWLEDGE_DIRS
    dirs = [Path(part.strip()).expanduser() for part in re.split(r"[,;\n]+", raw) if part.strip()]
    return dirs or [Path(DEFAULT_KNOWLEDGE_DIR).expanduser()]


def extra_knowledge_paths() -> list[Path]:
    raw = os.getenv("LINE_KNOWLEDGE_EXTRA_PATHS")
    if raw is None:
        raw = DEFAULT_EXTRA_KNOWLEDGE_PATHS
    if raw.strip().lower() in {"", "0", "false", "no", "off"}:
        return []
    return [Path(part.strip()).expanduser() for part in re.split(r"[,;\n]+", raw) if part.strip()]


def knowledge_source_files(roots: list[Path], extra_paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.exists() and root.is_dir():
            files.extend(sorted(path for path in root.rglob("*.md") if path.is_file()))
    for path in extra_paths:
        if path.exists() and path.is_file() and path.suffix.lower() == ".md":
            files.append(path)
        elif path.exists() and path.is_dir():
            files.extend(sorted(item for item in path.rglob("*.md") if item.is_file()))
    files = [path for path in files if is_supported_guideline_file(path)]

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in files:
        key = str(path.resolve()) if path.exists() else str(path)
        if key not in seen:
            seen.add(key)
            deduped.append(path)
    return deduped


def is_supported_guideline_file(path: Path) -> bool:
    lower = path.name.lower()
    if lower.startswith("icon") or "/." in str(path):
        return False
    return path.suffix.lower() == ".md"


def guideline_source_label(source_name: str, text: str = "") -> str:
    lower = f"{source_name}\n{text[:5000]}".lower()
    if "kdigo" in lower:
        if "2026" in lower and ("public review" in lower or "draft" in lower):
            return "KDIGO 2026 Diabetes and CKD Guideline Update (Public Review Draft)"
        if "2024" in lower and "ckd" in lower:
            return "KDIGO 2024 Clinical Practice Guideline for CKD"
        if "2022" in lower and ("diabetes" in lower or "ckd" in lower):
            return "KDIGO 2022 Clinical Practice Guideline for Diabetes Management in CKD"
        return "KDIGO Clinical Practice Guideline"
    if "aace" in lower:
        if "2026" in lower:
            return "AACE 2026 Consensus Statement: Algorithm for Management of Adults With T2D"
        if "2022" in lower:
            return "AACE 2022 Clinical Practice Guideline: Diabetes Mellitus Comprehensive Care Plan"
        return "AACE Clinical Diabetes Guidance"
    if "ada" in lower or re.search(r"dc26s\d+", lower):
        return "ADA Standards of Care in Diabetes 2026"
    return "本地臨床指南知識庫"


def public_metadata(value: str) -> str:
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
    parent_context = section_parent_context(lines)

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
            if parent_context:
                chunk_text = f"{chunk_text}\nParent section context: {parent_context}"
            chunks.append(
                KnowledgeChunk(
                    source,
                    source_label,
                    title,
                    section,
                    "table_row",
                    chunk_text,
                    chunk_tokens(source_label, title, section, "table_row", chunk_text),
                )
            )
    return chunks


def section_parent_context(lines: list[str]) -> str:
    context_lines: list[str] = []
    for line in lines:
        stripped = clean_cell_text(line)
        if not stripped:
            continue
        if "<tr" in line.lower() or "</tr>" in line.lower() or re.fullmatch(r"\|?\s*[-:| ]+\s*\|?", line.strip()):
            continue
        if "|" in line and len(line.split("|")) >= 3:
            continue
        context_lines.append(stripped)
        if len(" ".join(context_lines)) >= int(os.getenv("LINE_KNOWLEDGE_PARENT_CONTEXT_CHARS", "900")):
            break
    return " ".join(context_lines)[: int(os.getenv("LINE_KNOWLEDGE_PARENT_CONTEXT_CHARS", "900"))]


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
    roots = knowledge_dirs()
    extras = extra_knowledge_paths()
    chunk_chars = int(os.getenv("LINE_KNOWLEDGE_CHUNK_CHARS", "1800"))
    if not any(root.exists() for root in roots) and not any(path.exists() for path in extras):
        return None

    global _knowledge_cache, _knowledge_cache_key
    cache_key = ("|".join([*[str(root) for root in roots], *[str(path) for path in extras]]), chunk_chars)
    if _knowledge_cache and _knowledge_cache_key == cache_key:
        return _knowledge_cache
    with _knowledge_lock:
        if _knowledge_cache and _knowledge_cache_key == cache_key:
            return _knowledge_cache
        _knowledge_cache = KnowledgeBase(roots, extra_paths=extras, chunk_chars=chunk_chars)
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
    return kb.search_multi(query, limit=limit, excerpt_chars=excerpt_chars)


def search_knowledge_candidates(query: str) -> list[KnowledgeHit]:
    kb = load_knowledge_base()
    if not kb:
        return []
    limit = int(os.getenv("LINE_KNOWLEDGE_CANDIDATE_SNIPPETS", "15"))
    excerpt_chars = int(os.getenv("LINE_KNOWLEDGE_CANDIDATE_EXCERPT_CHARS", "700"))
    return kb.search_multi(query, limit=limit, excerpt_chars=excerpt_chars)


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
        "\n\n背景知識檢索：以下為本次問題相關的已載入臨床指南片段。",
        "嚴格回答規則：只能根據以下片段回答；不要使用模型內建知識、一般醫學常識或推測補完。",
        "若以下片段不足以直接回答使用者問題，請明確說指南片段不足，並停止回答，不要改用其他來源補充。",
        "回答方式：先用 1 句話直接回答，再用 2 到 4 個重點整理指南片段支持的內容；若有藥物限制或 eGFR 門檻，請清楚列出，但不要提供個人化劑量。",
        "來源標示：回答中請自然標示依據來源，例如「根據 ADA 2026 / KDIGO / AACE 片段」；不要編造未出現在片段中的來源。",
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
    roots = knowledge_dirs()
    extras = extra_knowledge_paths()
    extra_existing = [path for path in extras if path.exists() and path.is_file()]
    dir_file_count = sum(len(list(root.rglob("*.md"))) for root in roots if root.exists() and root.is_dir())
    return {
        "enabled": knowledge_enabled(),
        "dir": str(roots[0]) if roots else "",
        "dirs": [str(root) for root in roots],
        "extra_paths": [str(path) for path in extras],
        "available": bool(kb),
        "strict": knowledge_strict_enabled(),
        "chunks": len(kb.chunks) if kb else 0,
        "files": len(kb.source_files) if kb else 0,
        "dir_files": dir_file_count,
        "extra_files": len(extra_existing),
        "sources": sorted({chunk.source_label for chunk in kb.chunks}) if kb else [],
    }


def tokenize(text: str) -> Iterable[str]:
    for token in TOKEN_RE.findall(text.lower()):
        token = token.strip()
        if len(token) <= 1 and not token.isdigit() and not re.match(r"[\u4e00-\u9fff]", token):
            continue
        yield token


def chunk_tokens(source_label: str, title: str, section: str, chunk_type: str, text: str) -> tuple[str, ...]:
    metadata = f"{source_label} {title} {section} {chunk_type}"
    return tuple(tokenize(f"{metadata}\n{text}"))


def chunk_dedup_key(chunk: KnowledgeChunk) -> tuple[str, ...]:
    if chunk.chunk_type == "table_row":
        digest = hashlib.sha1(chunk.text[:500].encode("utf-8", errors="ignore")).hexdigest()[:12]
        return (chunk.source, chunk.section, chunk.chunk_type, digest)
    return (chunk.source, chunk.section, chunk.chunk_type)


def hit_dedup_key(hit: KnowledgeHit) -> tuple[str, ...]:
    if hit.chunk_type == "table_row":
        digest = hashlib.sha1(hit.excerpt[:500].encode("utf-8", errors="ignore")).hexdigest()[:12]
        return (hit.source, hit.section, hit.chunk_type, digest)
    return (hit.source, hit.section, hit.chunk_type)


def query_variants(query: str) -> list[str]:
    return [variant.text for variant in query_variant_specs(query)]


def query_variant_specs(query: str) -> list[QueryVariant]:
    variants: list[QueryVariant] = [QueryVariant("original", query, 1.0)]
    query_lower = query.lower()

    expansion_terms: list[str] = []
    for key, terms in QUERY_EXPANSIONS.items():
        if key in query:
            expansion_terms.extend(terms)
    if expansion_terms:
        variants.append(QueryVariant("synonyms", " ".join([query, *dedupe_terms(expansion_terms)]), 0.9))

    for triggers, intent_queries in QUERY_INTENT_VARIANTS:
        if any(trigger in query or trigger in query_lower for trigger in triggers):
            variants.extend(QueryVariant("section_intent", f"{query} {intent_query}", 0.84) for intent_query in intent_queries)

    pregnancy_query = any(term in query for term in ("懷孕", "妊娠", "孕")) or any(
        term in query_lower for term in ("pregnancy", "gestational", "gdm")
    )
    diagnosis_query = any(term in query for term in ("診斷", "篩檢", "標準")) or any(
        term in query_lower for term in ("diagnosis", "screening", "criteria", "ogtt")
    )
    if pregnancy_query and diagnosis_query:
        variants.append(
            QueryVariant(
                "clinical_context",
                f"{query} gestational diabetes mellitus GDM screening diagnosis Table 2.8 one-step two-step OGTT 24-28 weeks fasting 1 h 2 h Carpenter-Coustan IADPSG",
                0.88,
            )
        )

    dialysis_query = any(term in query for term in ("洗腎", "透析", "腎衰竭")) or any(
        term in query_lower for term in ("dialysis", "hemodialysis", "kidney failure", "eskd", "esrd")
    )
    glycemic_goal_query = any(term in query for term in ("血糖控制", "控制目標", "血糖目標", "目標")) or any(
        term in query_lower for term in ("glycemic goal", "glycemic target", "glucose target", "a1c goal")
    )
    if dialysis_query and glycemic_goal_query:
        variants.extend(
            [
                QueryVariant(
                    "disease_context",
                    f"{query} diabetes CKD stage 5 dialysis ESKD glycemic targets individualized goals hypoglycemia risk",
                    0.9,
                ),
                QueryVariant(
                    "measurement_method",
                    f"{query} A1C reliability advanced CKD dialysis glycated albumin fructosamine CGM BGM glucose monitoring",
                    0.9,
                ),
            ]
        )

    variants.extend(coverage_query_variants(query, query_lower))

    if len(variants) == 1:
        tokens = list(expand_query_tokens(query))
        if tokens:
            variants.append(QueryVariant("expanded_tokens", " ".join(tokens), 0.84))

    deduped: list[QueryVariant] = []
    seen: set[str] = set()
    for variant in variants:
        compact = re.sub(r"\s+", " ", variant.text).strip()
        key = compact.lower()
        if compact and key not in seen:
            seen.add(key)
            deduped.append(QueryVariant(variant.label, compact, variant.weight))
    return deduped[:8]


def coverage_query_variants(query: str, query_lower: str) -> list[QueryVariant]:
    variants: list[QueryVariant] = []
    kidney_query = any(term in query for term in ("腎", "尿蛋白", "白蛋白尿", "腎絲球")) or any(
        term in query_lower for term in ("ckd", "kidney", "renal", "egfr", "uacr", "albuminuria")
    )
    medication_query = any(term in query for term in ("藥", "用藥", "胰島素", "降血糖")) or any(
        term in query_lower for term in ("medication", "pharmacologic", "sglt", "glp", "metformin", "insulin", "finerenone")
    )
    cardiovascular_query = any(term in query for term in ("心", "心血管", "心衰竭", "血壓", "膽固醇")) or any(
        term in query_lower for term in ("ascvd", "cardiovascular", "heart failure", "hypertension", "lipid")
    )
    older_query = any(term in query for term in ("老人", "長者", "高齡")) or any(
        term in query_lower for term in ("older", "geriatric", "frailty")
    )

    if kidney_query and medication_query:
        variants.extend(
            [
                QueryVariant(
                    "coverage_ckd_medication",
                    f"{query} SGLT2 inhibitor eGFR threshold GLP-1 receptor agonist CKD metformin renal function finerenone nsMRA albuminuria UACR",
                    0.86,
                ),
                QueryVariant(
                    "coverage_ckd_safety",
                    f"{query} advanced CKD hypoglycemia risk insulin kidney impairment acute illness perioperative pregnancy older adults contraindication temporary hold",
                    0.78,
                ),
            ]
        )
    if cardiovascular_query and medication_query:
        variants.append(
            QueryVariant(
                "coverage_cardiorenal",
                f"{query} ASCVD heart failure CKD cardiorenal benefit SGLT2 inhibitor GLP-1 receptor agonist blood pressure lipid risk management",
                0.82,
            )
        )
    if older_query:
        variants.append(
            QueryVariant(
                "coverage_older_adults",
                f"{query} older adults frailty cognitive impairment functional status hypoglycemia deintensification individualized A1C goal CGM",
                0.84,
            )
        )
    return variants


def dedupe_terms(terms: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for term in terms:
        key = term.lower().strip()
        if key and key not in seen:
            seen.add(key)
            result.append(term)
    return result


def coverage_rerank_hits(query: str, hits: list[KnowledgeHit], limit: int) -> list[KnowledgeHit]:
    if not hits:
        return []

    sorted_hits = sorted(hits, key=lambda hit: hit.score, reverse=True)
    target_facets = required_facets(query)
    selected: list[KnowledgeHit] = []
    covered: set[str] = set()
    remaining = sorted_hits[: max(limit * 5, limit + 20)]
    max_score = max((hit.score for hit in remaining), default=1.0)

    while remaining and len(selected) < limit:
        best_index = 0
        best_value = -1.0
        for index, hit in enumerate(remaining):
            facets = hit_facets(hit)
            new_target_facets = (facets & target_facets) - covered
            new_general_facets = facets - covered
            score_component = hit.score / max_score
            general_bonus = 0.12 * min(len(new_general_facets), 3) if target_facets else 0.03 * min(len(new_general_facets), 2)
            coverage_bonus = 0.42 * len(new_target_facets) + general_bonus
            diversity_bonus = 0.0
            if selected and all(hit.source != item.source for item in selected):
                diversity_bonus += 0.08
            if selected and all(hit.section != item.section for item in selected):
                diversity_bonus += 0.08
            redundancy_penalty = 0.0
            if target_facets and not (facets & target_facets):
                redundancy_penalty += 0.45
            if target_facets:
                redundancy_penalty += 0.12 * len(target_facets - facets)
            if "hypoglycemia" in target_facets and "hypoglycemia" not in facets:
                redundancy_penalty += 0.35
            if "foot_care" in target_facets and "foot_care" not in facets:
                redundancy_penalty += 0.35
            if "frequency" in target_facets and "frequency" not in facets:
                redundancy_penalty += 0.28
            if any(hit.source == item.source and hit.section == item.section and hit.chunk_type == item.chunk_type for item in selected):
                redundancy_penalty += 0.35
            if any(text_similarity(hit.excerpt, item.excerpt) > 0.62 for item in selected):
                redundancy_penalty += 0.25
            value = score_component + coverage_bonus + diversity_bonus - redundancy_penalty
            if value > best_value:
                best_value = value
                best_index = index
        chosen = remaining.pop(best_index)
        selected.append(chosen)
        covered.update(hit_facets(chosen))

    return selected


def required_facets(query: str) -> set[str]:
    lower = query.lower()
    facets: set[str] = set()
    if any(term in query for term in ("洗腎", "透析", "腎衰竭", "腎", "腎絲球")) or any(
        term in lower for term in ("dialysis", "kidney", "ckd", "egfr", "eskd", "esrd")
    ):
        facets.add("kidney_context")
    if any(term in query for term in ("血糖控制", "控制目標", "血糖目標", "目標")) or any(
        term in lower for term in ("glycemic goal", "glycemic target", "glucose target", "a1c goal")
    ):
        facets.add("glycemic_target")
    if any(term in query for term in ("洗腎", "透析", "腎衰竭")) or any(
        term in lower for term in ("dialysis", "kidney failure", "eskd", "esrd")
    ):
        facets.update({"a1c_reliability", "monitoring"})
    if any(term in query for term in ("連續血糖", "血糖機", "監測")) or any(
        term in lower for term in ("cgm", "bgm", "smbg", "monitoring", "time in range")
    ):
        facets.add("monitoring")
    if any(term in query for term in ("藥", "用藥", "胰島素")) or any(
        term in lower for term in ("medication", "pharmacologic", "sglt", "glp", "insulin", "metformin")
    ):
        facets.add("medication")
    if any(term in query for term in ("egfr", "門檻", "多少", "幾")) or any(
        term in lower for term in ("threshold", "criteria", "mg/dl", "ml/min", "egfr")
    ):
        facets.add("threshold")
    if any(term in query for term in ("診斷", "篩檢", "標準")) or any(
        term in lower for term in ("diagnosis", "screening", "criteria", "ogtt")
    ):
        facets.add("diagnosis")
    if any(term in query for term in ("腳", "足", "足部", "神經")) or any(
        term in lower for term in ("foot", "neuropathy", "monofilament", "ulcer")
    ):
        facets.add("foot_care")
    if any(term in query for term in ("多久", "幾次", "頻率", "一次", "每年")) or any(
        term in lower for term in ("frequency", "annually", "months", "yearly", "every")
    ):
        facets.add("frequency")
    if any(term in query for term in ("懷孕", "妊娠", "孕")) or any(term in lower for term in ("pregnancy", "gestational", "gdm")):
        facets.add("pregnancy")
    if "低血糖" in query or "hypoglycemia" in lower:
        facets.update({"hypoglycemia", "treatment"})
    if any(term in query for term in ("處理", "治療", "怎麼辦")) or any(term in lower for term in ("treatment", "management")):
        facets.add("treatment")
    return facets


def hit_facets(hit: KnowledgeHit) -> set[str]:
    haystack = f"{hit.source} {hit.source_label} {hit.title} {hit.section} {hit.chunk_type} {hit.excerpt}".lower()
    facets: set[str] = set()
    if re.search(r"\b(ckd|kidney|renal|egfr|albuminuria|uacr|dialysis|eskd|esrd)\b", haystack):
        facets.add("kidney_context")
    if re.search(r"\b(glycemic goal|glycemic target|glucose target|a1c goal|individualized goal|treatment goals)\b", haystack):
        facets.add("glycemic_target")
    if re.search(r"\b(a1c.*less reliable|less reliable.*a1c|glycated albumin|fructosamine|red blood cell turnover)\b", haystack):
        facets.add("a1c_reliability")
    if re.search(r"\b(cgm|bgm|smbg|glucose monitoring|time in range|tir|time below range|time above range)\b", haystack):
        facets.add("monitoring")
    if re.search(r"\b(sglt2|glp-1|insulin|metformin|finerenone|glucagon|pharmacologic|medication|dose|dosage)\b", haystack):
        facets.add("medication")
    if re.search(r"\b(mg/dl|mmol/l|ml/min|%|threshold|criteria|fasting|1 h|2 h|3 h|≥|<=|<|>)\b", haystack):
        facets.add("threshold")
    if re.search(r"\b(diagnosis|diagnostic|screening|ogtt|classification|criteria)\b", haystack):
        facets.add("diagnosis")
    if re.search(r"\b(foot|neuropathy|monofilament|ulcer|protective sensation|peripheral artery|pad|lops|podiatrist)\b", haystack):
        facets.add("foot_care")
    if re.search(r"\b(annually|every \d|months?|yearly|frequency|examination frequency|at least yearly)\b", haystack):
        facets.add("frequency")
    if re.search(r"\b(pregnancy|gestational|gdm|preconception|postpartum)\b", haystack):
        facets.add("pregnancy")
    if "hypoglycemia" in haystack:
        facets.add("hypoglycemia")
    if re.search(r"\b(treatment|therapy|management|intervention|recommendation|recommended|prescribed)\b", haystack):
        facets.add("treatment")
    if hit.chunk_type == "table_row":
        facets.add("table")
    if "kdigo" in haystack:
        facets.add("source_kdigo")
    elif "aace" in haystack:
        facets.add("source_aace")
    elif "ada standards" in haystack or re.search(r"\bdc26s\d+\b", haystack):
        facets.add("source_ada")
    return facets


def text_similarity(left: str, right: str) -> float:
    left_tokens = set(tokenize(left))
    right_tokens = set(tokenize(right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


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
    glycemic_goal_query = any(term in query for term in ("血糖控制", "控制目標", "血糖目標", "目標")) or any(
        term in query_lower for term in ("glycemic goal", "glycemic target", "glucose target", "a1c goal")
    )
    dialysis_query = any(term in query for term in ("洗腎", "透析", "腎衰竭")) or any(
        term in query_lower for term in ("dialysis", "kidney failure", "stage g5", "eskd", "esrd")
    )
    pregnancy_diagnosis_query = (
        any(term in query for term in ("懷孕", "妊娠", "孕"))
        or any(term in query_lower for term in ("pregnancy", "gestational", "gdm"))
    ) and (
        any(term in query for term in ("診斷", "篩檢", "標準"))
        or any(term in query_lower for term in ("diagnosis", "screening", "criteria", "ogtt"))
    )

    if chunk.chunk_type == "table_row":
        adjustment *= 1.25
    if re.search(r"\b(reference|references|acknowledg|appendix)\b", haystack):
        adjustment *= 0.35
    if re.search(r"\b(recommendation|recommendations|treatment|therapy|selection|screening|diagnosis|pharmacologic|management|interventions)\b", haystack):
        adjustment *= 1.18
    if re.search(r"\b(egfr|albuminuria|uacr|mg/g|ml/min|contraindicat|avoid|dose|dosage|adjust|threshold|initiat|discontinu)\b", haystack):
        adjustment *= 1.18
    if glycemic_goal_query and ("glycemic goals" in haystack or "setting and modifying glycemic goals" in haystack):
        adjustment *= 2.8
    if glycemic_goal_query and ("dc26s006" in haystack or "glycemic goals, hypoglycemia" in haystack):
        adjustment *= 1.7
    if dialysis_query and glycemic_goal_query and (
        "a1c levels are also less reliable" in haystack
        or "glycated albumin" in haystack
        or "fructosamine" in haystack
        or "prevalent ckd and substantial comorbidity" in haystack
    ):
        adjustment *= 3.2
    if dialysis_query and glycemic_goal_query and "dc26s011" in haystack and "glycemic goals" in haystack:
        adjustment *= 3.5
    if pregnancy_diagnosis_query and (
        "gestational diabetes" in haystack
        or "gdm" in haystack
        or "ogtt" in haystack
        or "diagnosis and classification" in haystack
    ):
        adjustment *= 2.8
    if pregnancy_diagnosis_query and (
        "dc26s002" in haystack
        or "table 2.8" in haystack
        or "screening for and diagnosis of gdm" in haystack
        or "one-step strategy" in haystack
        or "two-step strategy" in haystack
    ):
        adjustment *= 3.4
    if pregnancy_diagnosis_query and "preconception" in haystack and "preconception" not in query_lower:
        adjustment *= 0.25
    if pregnancy_diagnosis_query and "checklist" in haystack and "checklist" not in query_lower:
        adjustment *= 0.4
    if pregnancy_diagnosis_query and "postpartum" in haystack and "postpartum" not in query_lower and "產後" not in query:
        adjustment *= 0.65

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
    if ("低血糖" in query or "hypoglycemia" in query_lower) and (
        "hypoglycemia treatment" in haystack
        or "glucose is the preferred treatment" in haystack
        or "15 min" in haystack
        or "glucagon should be prescribed" in haystack
        or "fast-acting carbohydrates" in haystack
    ):
        adjustment *= 2.6
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
    if ("sglt" in query_lower or "sglt2" in query_lower) and ("egfr" in query_lower or "腎" in query) and (
        "can be initiated if egfr is above 20" in haystack
        or "glucose-lowering therapy for people with chronic kidney disease" in haystack
        or "sglt2 inhibitors are recommended" in haystack
    ):
        adjustment *= 2.4
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
