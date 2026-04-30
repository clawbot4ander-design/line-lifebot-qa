from __future__ import annotations

from dataclasses import dataclass
import html
import json
from pathlib import Path
import hashlib
import math
import os
import re
import threading
import urllib.error
import urllib.request
from typing import Iterable


DEFAULT_KNOWLEDGE_DIR = os.getenv("LINE_KNOWLEDGE_DIR", "/app/data/guidelines")
DEFAULT_KNOWLEDGE_DIRS = (
    "/app/data,"
    "/app/data/ada,/app/data/aace,/app/data/kdigo,"
    "/app/data/guidelines,/app/data/adaguidelines,/app/data/kdigoguidelines,/app/data/aaceguidelines"
)
DEFAULT_EXTRA_KNOWLEDGE_PATHS = ""
DEFAULT_KEYWORD_DIR = Path(__file__).resolve().parent / "keywords"
GEMINI_EMBEDDING_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9+-]*|\d+(?:\.\d+)?|[\u4e00-\u9fff]{1,4}")
HEADING_RE = re.compile(r"^#{1,4}\s+(.+)$")
RECOMMENDATION_START_RE = re.compile(
    r"^\s*>?\s*(?:[-*]\s*)?(?:\*\*)?(?:(?P<ada>\d{1,2}\.\d+[a-z]?)|"
    r"(?P<label>recommendation|practice point)\s+(?P<other>\d[\dA-Za-z.-]*))",
    flags=re.I,
)

QUERY_EXPANSIONS: dict[str, tuple[str, ...]] = {
    "血糖": ("glucose", "glycemic", "hyperglycemia", "hypoglycemia", "blood glucose"),
    "低血糖": ("hypoglycemia", "glucagon", "level 1", "level 2", "level 3"),
    "高血糖": ("hyperglycemia", "glucose", "DKA", "HHS", "ketone"),
    "類固醇": (
        "glucocorticoid",
        "glucocorticoid therapy",
        "steroid-induced hyperglycemia",
        "corticosteroid",
        "prednisone",
        "prednisolone",
        "dexamethasone",
        "NPH insulin",
        "basal insulin",
        "prandial insulin",
        "correction insulin",
        "point-of-care blood glucose monitoring",
    ),
    "HHNK": ("HHS", "hyperosmolar hyperglycemic state", "hyperglycemic hyperosmolar state", "hyperglycemic crises", "DKA", "intravenous fluids", "insulin", "electrolytes"),
    "hhnk": ("HHS", "hyperosmolar hyperglycemic state", "hyperglycemic hyperosmolar state", "hyperglycemic crises", "DKA", "intravenous fluids", "insulin", "electrolytes"),
    "高滲透壓": ("HHS", "hyperosmolar hyperglycemic state", "hyperglycemic hyperosmolar state", "hyperglycemic crises", "serum osmolality", "intravenous fluids"),
    "高血糖高滲透壓": ("HHS", "hyperosmolar hyperglycemic state", "hyperglycemic hyperosmolar state", "hyperglycemic crises", "serum osmolality", "intravenous fluids"),
    "處理": ("treatment", "management", "recommendation", "action", "recheck", "repeat"),
    "治療": ("treatment", "therapy", "management", "recommendation"),
    "控制": ("goal", "target", "glycemic goals", "A1C", "blood glucose", "CGM", "BGM", "time in range"),
    "目標": ("goal", "target", "glycemic goals", "A1C goal", "glucose target", "time in range"),
    "血糖控制": ("glycemic goals", "glycemic management", "A1C", "CGM", "BGM", "time in range"),
    "血糖控制目標": ("glycemic goals", "A1C goal", "glucose target", "CGM metrics", "time in range"),
    "酮酸": ("diabetic ketoacidosis", "ketoacidosis", "DKA", "ketone", "hyperglycemic crises", "intravenous fluids", "insulin", "electrolytes"),
    "酮酸中毒": ("diabetic ketoacidosis", "DKA", "ketoacidosis", "hyperglycemic crises", "ketone", "anion gap", "acidosis", "intravenous fluids", "insulin", "electrolytes"),
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
    "視網膜": ("diabetic retinopathy", "retinopathy", "retinal", "macular edema", "DME", "PDR", "NPDR"),
    "視網膜病變": ("diabetic retinopathy", "retinopathy", "nonproliferative diabetic retinopathy", "proliferative diabetic retinopathy", "NPDR", "PDR", "diabetic macular edema", "DME"),
    "黃斑": ("diabetic macular edema", "DME", "macular edema", "foveal center", "anti-VEGF"),
    "分期": ("staging", "stage", "severity", "classification", "mild", "moderate", "severe", "nonproliferative", "proliferative"),
    "分級": ("staging", "stage", "severity", "classification", "mild", "moderate", "severe", "nonproliferative", "proliferative"),
    "新的治療": ("treatment", "therapy", "intervention", "anti-VEGF", "laser photocoagulation", "vitrectomy", "emerging therapies"),
    "腳": ("foot", "neuropathy", "ulcer", "podiatrist"),
    "下肢": ("lower extremity", "lower limb", "peripheral artery disease", "PAD", "limb ischemia"),
    "動脈阻塞": ("peripheral artery disease", "PAD", "lower-extremity arterial disease", "limb ischemia", "revascularization"),
    "血管阻塞": ("peripheral artery disease", "PAD", "atherosclerotic cardiovascular disease", "limb ischemia"),
    "跛行": ("claudication", "peripheral artery disease", "PAD", "walking distance", "limb ischemia"),
    "缺血": ("ischemia", "limb ischemia", "peripheral artery disease", "PAD", "revascularization"),
    "心臟": ("cardiovascular", "heart", "ASCVD", "blood pressure", "lipid"),
    "心血管": ("cardiovascular", "ASCVD", "heart failure", "MACE", "cardiorenal"),
    "心衰竭": ("heart failure", "HF", "HFrEF", "HFpEF", "heart failure hospitalization"),
    "血壓": ("blood pressure", "hypertension"),
    "膽固醇": ("lipid", "cholesterol", "statin", "triglyceride"),
    "肝": ("liver", "hepatic", "steatotic liver disease", "MASLD", "MASH", "NAFLD", "NASH"),
    "脂肪肝": ("MASLD", "metabolic dysfunction-associated steatotic liver disease", "NAFLD", "fatty liver"),
    "代謝性脂肪肝": ("MASLD", "metabolic dysfunction-associated steatotic liver disease", "NAFLD"),
    "脂肪性肝炎": ("MASH", "metabolic dysfunction-associated steatohepatitis", "NASH", "steatohepatitis"),
    "肝炎": ("MASH", "NASH", "steatohepatitis", "liver disease"),
    "肝硬化": ("cirrhosis", "compensated cirrhosis", "liver fibrosis", "advanced fibrosis"),
    "懷孕": ("pregnancy", "gestational", "preconception"),
    "懷孕糖尿病": ("gestational diabetes mellitus", "GDM", "screening", "diagnosis", "OGTT", "24-28 weeks"),
    "妊娠糖尿病": ("gestational diabetes mellitus", "GDM", "screening", "diagnosis", "OGTT", "24-28 weeks"),
    "兒童": ("children", "adolescents", "pediatric", "youth"),
    "老人": ("older adults", "geriatric", "frailty"),
    "住院": ("hospital", "inpatient", "admission", "glucocorticoid therapy", "inpatient hyperglycemia"),
    "住院高血糖": (
        "hospital hyperglycemia",
        "inpatient hyperglycemia",
        "glucocorticoid therapy",
        "steroid-induced hyperglycemia",
        "NPH insulin",
        "basal insulin",
        "prandial insulin",
        "correction insulin",
        "point-of-care blood glucose monitoring",
    ),
    "高血糖急症": ("hyperglycemic crises", "DKA", "diabetic ketoacidosis", "HHS", "hyperosmolar hyperglycemic state", "diagnostic criteria", "intravenous fluids", "insulin", "electrolytes"),
    "篩檢": ("screening", "diagnosis", "A1C", "fasting plasma glucose"),
    "診斷": ("diagnosis", "classification", "A1C", "OGTT", "diagnostic criteria"),
    "診斷標準": ("diagnostic criteria", "screening", "classification", "A1C", "fasting plasma glucose", "OGTT"),
    "併發症": ("complications", "retinopathy", "kidney", "neuropathy", "cardiovascular"),
    "體重": ("weight", "obesity", "lifestyle", "weight management"),
    "肥胖": ("obesity", "adiposity", "weight management", "anti-obesity medication", "metabolic surgery"),
    "血糖機": ("blood glucose monitoring", "BGM", "glucose meter"),
    "連續血糖": ("continuous glucose monitoring", "continuous glucose monitor", "CGM", "rtCGM", "isCGM"),
    "連續血糖監測": ("continuous glucose monitoring", "continuous glucose monitor", "CGM", "rtCGM", "isCGM"),
    "新科技": ("diabetes technology", "CGM", "continuous glucose monitoring", "automated insulin delivery", "AID"),
    "科技": ("diabetes technology", "CGM", "continuous glucose monitoring", "BGM", "insulin pump", "AID"),
    "適用": ("recommended", "offered", "indicated", "use of CGM", "on insulin therapy", "individual needs"),
    "適合": ("recommended", "offered", "indicated", "use of CGM", "on insulin therapy", "individual needs"),
    "哪些病人": ("people with diabetes", "children adolescents adults", "on insulin therapy", "noninsulin therapies that can cause hypoglycemia", "pregnancy"),
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
            "diabetic retinopathy staging severity nonproliferative proliferative NPDR PDR diabetic macular edema DME",
            "retinopathy treatment anti-VEGF panretinal laser photocoagulation vitrectomy macular focal grid photocoagulation corticosteroid",
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
        ("下肢", "動脈阻塞", "血管阻塞", "跛行", "缺血", "pad", "peripheral artery", "claudication"),
        (
            "peripheral artery disease PAD lower-extremity arterial disease ASCVD antiplatelet aspirin clopidogrel rivaroxaban statin lipid blood pressure smoking cessation",
            "ADA section 10 cardiovascular disease risk management PAD ASCVD antiplatelet therapy lipid-lowering therapy blood pressure",
            "ADA section 12 foot care PAD lower extremity pulses claudication ABI toe pressures vascular assessment revascularization semaglutide STRIDE limb outcomes",
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
            "Use of continuous glucose monitoring recommendations CGM recommended diabetes onset children adolescents adults insulin therapy noninsulin therapies hypoglycemia",
            "CGM indicated people with diabetes individual circumstances preferences needs pregnancy periodic professional CGM",
        ),
    ),
    (
        ("體重", "肥胖", "減重", "weight", "obesity"),
        (
            "obesity weight management lifestyle pharmacotherapy metabolic surgery diabetes",
            "GLP-1 dual GIP GLP-1 weight loss obesity treatment",
        ),
    ),
    (
        ("脂肪肝", "脂肪性肝炎", "代謝性脂肪肝", "肝硬化", "肝纖維", "masld", "mash", "nafld", "nash", "steatotic liver"),
        (
            "MASLD metabolic dysfunction-associated steatotic liver disease NAFLD diabetes treatment obesity weight loss",
            "MASH metabolic dysfunction-associated steatohepatitis NASH GLP-1 receptor agonist pioglitazone tirzepatide cirrhosis fibrosis",
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
    parent_text: str
    metadata: tuple[str, ...]
    tokens: tuple[str, ...]


@dataclass(frozen=True)
class KnowledgeHit:
    source: str
    source_label: str
    title: str
    section: str
    chunk_type: str
    excerpt: str
    parent_excerpt: str
    metadata: tuple[str, ...]
    score: float


@dataclass(frozen=True)
class QueryVariant:
    label: str
    text: str
    weight: float = 0.82


@dataclass(frozen=True)
class KeywordEntry:
    module: str
    entry_id: str
    triggers: tuple[str, ...]
    expansions: tuple[str, ...]
    variant_queries: tuple[str, ...]


class KnowledgeBase:
    def __init__(self, roots: list[Path], extra_paths: list[Path] | None = None, chunk_chars: int = 1800) -> None:
        self.roots = roots
        self.root = roots[0] if roots else Path(".")
        self.extra_paths = extra_paths or []
        self.chunk_chars = chunk_chars
        self.chunks: list[KnowledgeChunk] = []
        self.source_files: list[Path] = []
        self.vector_index: list[dict[int, float]] = []
        self.token_postings: dict[str, tuple[int, ...]] = {}
        self.dense_vector_index: list[list[float]] = []
        self.dense_embedding_error = ""
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
        self.vector_index = [hashed_vector(chunk.tokens) for chunk in chunks]
        self.dense_vector_index, self.dense_embedding_error = build_dense_vector_index(chunks)

        df: dict[str, int] = {}
        postings: dict[str, list[int]] = {}
        lengths = []
        for index, chunk in enumerate(chunks):
            unique = set(chunk.tokens)
            lengths.append(len(chunk.tokens))
            for token in unique:
                df[token] = df.get(token, 0) + 1
                postings.setdefault(token, []).append(index)
        self.document_frequency = df
        self.token_postings = {token: tuple(indexes) for token, indexes in postings.items()}
        self.average_length = sum(lengths) / len(lengths) if lengths else 1.0

    def _chunks_from_file(self, path: Path) -> list[KnowledgeChunk]:
        text = path.read_text(encoding="utf-8", errors="ignore")
        title = path.stem
        source_label = guideline_source_label(str(path), text)
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
            if line and not skippable_guideline_line(line):
                section_lines.append(line)
        if section_lines:
            blocks.append((current_section, section_lines))

        chunks: list[KnowledgeChunk] = []
        for section, lines in blocks:
            if section.lower() in {"references", "reference"}:
                continue
            parent_text = "\n".join(lines)
            summary_chunk = section_summary_chunk(path.name, source_label, title, section or title, lines, parent_text)
            if summary_chunk:
                chunks.append(summary_chunk)
            chunks.extend(recommendation_chunks_from_lines(path.name, source_label, title, section or title, lines, parent_text))
            buffer: list[str] = []
            size = 0
            for line in lines:
                if size + len(line) > self.chunk_chars and buffer:
                    chunk_text = "\n".join(buffer)
                    metadata = structured_metadata(source_label, title, section or title, "text", chunk_text, parent_text)
                    chunks.append(
                        KnowledgeChunk(
                            path.name,
                            source_label,
                            title,
                            section or title,
                            "text",
                            chunk_text,
                            parent_text,
                            metadata,
                            chunk_tokens(source_label, title, section or title, "text", chunk_text, metadata),
                        )
                    )
                    buffer = []
                    size = 0
                buffer.append(line)
                size += len(line) + 1
            if buffer:
                chunk_text = "\n".join(buffer)
                metadata = structured_metadata(source_label, title, section or title, "text", chunk_text, parent_text)
                chunks.append(
                    KnowledgeChunk(
                        path.name,
                        source_label,
                        title,
                        section or title,
                        "text",
                        chunk_text,
                        parent_text,
                        metadata,
                        chunk_tokens(source_label, title, section or title, "text", chunk_text, metadata),
                    )
                )
            chunks.extend(table_chunks_from_lines(path.name, source_label, title, section or title, lines, parent_text))
        return chunks

    def search(self, query: str, limit: int = 3, excerpt_chars: int = 520) -> list[KnowledgeHit]:
        query_tokens = list(expand_query_tokens(query))
        if not query_tokens or not self.chunks:
            return []

        query_vector = hashed_vector(query_tokens)
        vector_weight = float(os.getenv("LINE_KNOWLEDGE_VECTOR_WEIGHT", "0.55"))
        dense_query_vector = dense_embed_query(query) if self.dense_vector_index else []
        dense_vector_weight = float(os.getenv("LINE_DENSE_EMBEDDING_WEIGHT", "1.15"))
        scored: list[tuple[float, KnowledgeChunk]] = []
        candidate_indexes = self.search_candidate_indexes(query_tokens, use_dense=bool(dense_query_vector))
        for index in candidate_indexes:
            chunk = self.chunks[index]
            score = self._score(query_tokens, chunk)
            if query_vector and index < len(self.vector_index):
                score += sparse_cosine(query_vector, self.vector_index[index]) * vector_weight
            if dense_query_vector and index < len(self.dense_vector_index):
                score += dense_cosine(dense_query_vector, self.dense_vector_index[index]) * dense_vector_weight
            score *= domain_adjustment(query, chunk)
            if score > 0:
                scored.append((score, chunk))
        scored.sort(key=lambda item: item[0], reverse=True)

        raw_hits: list[KnowledgeHit] = []
        seen_sources: set[tuple[str, ...]] = set()
        for score, chunk in scored:
            key = chunk_dedup_key(chunk)
            if key in seen_sources:
                continue
            seen_sources.add(key)
            raw_hits.append(
                KnowledgeHit(
                    source=chunk.source,
                    source_label=chunk.source_label,
                    title=chunk.title,
                    section=chunk.section,
                    chunk_type=chunk.chunk_type,
                    excerpt=best_excerpt(chunk.text, query_tokens, excerpt_chars),
                    parent_excerpt=parent_excerpt_for_chunk(chunk, query_tokens),
                    metadata=chunk.metadata,
                    score=score,
                )
            )
            if len(raw_hits) >= max(limit * 4, limit + 20):
                break
        return source_balanced_hits(raw_hits, limit)

    def search_candidate_indexes(self, query_tokens: list[str], use_dense: bool = False) -> Iterable[int]:
        if use_dense or not inverted_index_enabled() or not self.token_postings:
            return range(len(self.chunks))

        unique_tokens = sorted(
            set(query_tokens),
            key=lambda token: (self.document_frequency.get(token, len(self.chunks) + 1), -len(token), token),
        )
        max_tokens = max(1, int(os.getenv("LINE_KNOWLEDGE_POSTING_MAX_TOKENS", "72")))
        target_chunks = max(50, int(os.getenv("LINE_KNOWLEDGE_POSTING_TARGET_CHUNKS", "1200")))
        candidate_indexes: set[int] = set()
        used = 0
        for token in unique_tokens:
            postings = self.token_postings.get(token)
            if not postings:
                continue
            if len(postings) >= max(len(self.chunks) * 0.85, 1):
                continue
            candidate_indexes.update(postings)
            used += 1
            if used >= max_tokens or len(candidate_indexes) >= target_chunks:
                break
        if not candidate_indexes:
            return range(len(self.chunks))
        return sorted(candidate_indexes)

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
                        parent_excerpt=hit.parent_excerpt,
                        metadata=hit.metadata,
                        score=fused_score,
                    )
        if "hyperglycemic_crisis" in query_concepts(query, query.lower()):
            priority_queries = (
                "dc26s016 Diabetic Ketoacidosis and Hyperglycemic Hyperosmolar State recommendation 16.16 HHS DKA intravenous fluids insulin electrolytes",
                "dc26s016 Table 16.1 Diagnostic criteria for DKA and HHS ketones pH bicarbonate osmolality",
            )
            for priority_query in priority_queries:
                for rank, hit in enumerate(self.search(priority_query, limit=max(limit, 10), excerpt_chars=excerpt_chars), start=1):
                    key = hit_dedup_key(hit)
                    fused_score = hit.score * 50.0 + 10000.0 / (rank + 1)
                    existing = candidates.get(key)
                    if not existing or fused_score > existing.score:
                        candidates[key] = KnowledgeHit(
                            source=hit.source,
                            source_label=hit.source_label,
                            title=hit.title,
                            section=hit.section,
                            chunk_type=hit.chunk_type,
                            excerpt=hit.excerpt,
                            parent_excerpt=hit.parent_excerpt,
                            metadata=hit.metadata,
                            score=fused_score,
                        )
        if "hospital_steroid_hyperglycemia" in query_concepts(query, query.lower()):
            priority_queries = (
                "dc26s016 glucocorticoid therapy hospitalized hyperglycemia NPH insulin prednisone prednisolone dexamethasone basal insulin prandial correction insulin point-of-care blood glucose monitoring",
                "dc26s009 recommendation 9.36 glucocorticoid treatment plan steroid-induced hyperglycemia NPH morning dose insulin frequent reassessment",
            )
            for priority_query in priority_queries:
                for rank, hit in enumerate(self.search(priority_query, limit=max(limit, 10), excerpt_chars=excerpt_chars), start=1):
                    key = hit_dedup_key(hit)
                    fused_score = hit.score * 50.0 + 10000.0 / (rank + 1)
                    existing = candidates.get(key)
                    if not existing or fused_score > existing.score:
                        candidates[key] = KnowledgeHit(
                            source=hit.source,
                            source_label=hit.source_label,
                            title=hit.title,
                            section=hit.section,
                            chunk_type=hit.chunk_type,
                            excerpt=hit.excerpt,
                            parent_excerpt=hit.parent_excerpt,
                            metadata=hit.metadata,
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
_keyword_lock = threading.Lock()
_keyword_cache: tuple[tuple[str, ...], list[KeywordEntry]] | None = None


def knowledge_enabled() -> bool:
    return os.getenv("LINE_KNOWLEDGE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}


def knowledge_strict_enabled() -> bool:
    return os.getenv("LINE_KNOWLEDGE_STRICT", "1").strip().lower() not in {"0", "false", "no", "off"}


def knowledge_dir() -> Path:
    return knowledge_dirs()[0]


def knowledge_dirs() -> list[Path]:
    raw = os.getenv("LINE_KNOWLEDGE_DIRS")
    legacy_dir = os.getenv("LINE_KNOWLEDGE_DIR")
    if raw is None:
        raw = DEFAULT_KNOWLEDGE_DIRS
    raw_parts = [part.strip() for part in re.split(r"[,;\n]+", raw) if part.strip()]
    if legacy_dir and legacy_dir.strip().lower() not in {"", "0", "false", "no", "off"}:
        legacy_path = Path(legacy_dir.strip()).expanduser()
        raw_parts.insert(0, str(legacy_path))
        if legacy_path.name.lower() in {"adaguidelines", "guidelines"}:
            parent = legacy_path.parent
            raw_parts.extend(
                [
                    str(parent),
                    str(parent / "ada"),
                    str(parent / "aace"),
                    str(parent / "kdigo"),
                    str(parent / "guidelines"),
                    str(parent / "adaguidelines"),
                    str(parent / "kdigoguidelines"),
                    str(parent / "aaceguidelines"),
                ]
            )

    dirs: list[Path] = []
    seen: set[str] = set()
    for part in raw_parts:
        path = Path(part).expanduser()
        key = str(path)
        if key not in seen:
            seen.add(key)
            dirs.append(path)
    return dirs or [Path(DEFAULT_KNOWLEDGE_DIR).expanduser()]


def standard_guideline_dirs() -> dict[str, str]:
    return {
        "ADA": "/app/data/ada 或 /app/data/adaguidelines",
        "AACE": "/app/data/aace 或 /app/data/aaceguidelines",
        "KDIGO": "/app/data/kdigo 或 /app/data/kdigoguidelines",
        "Shared": "/app/data 或 /app/data/guidelines",
    }


def extra_knowledge_paths() -> list[Path]:
    raw = os.getenv("LINE_KNOWLEDGE_EXTRA_PATHS")
    if raw is None:
        raw = DEFAULT_EXTRA_KNOWLEDGE_PATHS
    if raw.strip().lower() in {"", "0", "false", "no", "off"}:
        return []
    return [Path(part.strip()).expanduser() for part in re.split(r"[,;\n]+", raw) if part.strip()]


def keyword_paths() -> list[Path]:
    raw = os.getenv("LINE_KEYWORD_PATHS", "")
    paths = [DEFAULT_KEYWORD_DIR]
    if raw.strip().lower() not in {"", "0", "false", "no", "off"}:
        paths.extend(Path(part.strip()).expanduser() for part in re.split(r"[,;\n]+", raw) if part.strip())
    return paths


def keyword_files() -> list[Path]:
    files: list[Path] = []
    for path in keyword_paths():
        if path.exists() and path.is_dir():
            files.extend(sorted(item for item in path.glob("*.json") if item.is_file()))
        elif path.exists() and path.is_file() and path.suffix.lower() == ".json":
            files.append(path)
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in files:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            deduped.append(path)
    return deduped


def load_keyword_entries() -> list[KeywordEntry]:
    files = keyword_files()
    cache_key = tuple(str(path.resolve()) for path in files)
    global _keyword_cache
    if _keyword_cache and _keyword_cache[0] == cache_key:
        return _keyword_cache[1]
    with _keyword_lock:
        if _keyword_cache and _keyword_cache[0] == cache_key:
            return _keyword_cache[1]
        entries: list[KeywordEntry] = []
        for path in files:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                print(f"keyword module load failed: {path}: {type(exc).__name__}: {exc}")
                continue
            module_name = str(payload.get("name") or path.stem)
            for item in payload.get("entries", []):
                if not isinstance(item, dict):
                    continue
                triggers = tuple(str(value).strip() for value in item.get("triggers", []) if str(value).strip())
                expansions = tuple(str(value).strip() for value in item.get("expansions", []) if str(value).strip())
                variant_queries = tuple(
                    str(value).strip() for value in item.get("variant_queries", []) if str(value).strip()
                )
                if triggers and (expansions or variant_queries):
                    entries.append(
                        KeywordEntry(
                            module=module_name,
                            entry_id=str(item.get("id") or ""),
                            triggers=triggers,
                            expansions=expansions,
                            variant_queries=variant_queries,
                        )
                    )
        _keyword_cache = (cache_key, entries)
        return entries


def matched_keyword_entries(query: str) -> list[KeywordEntry]:
    matches: list[KeywordEntry] = []
    for entry in load_keyword_entries():
        if any(keyword_trigger_matches(query, trigger) for trigger in entry.triggers):
            matches.append(entry)
    return matches


def keyword_trigger_matches(query: str, trigger: str) -> bool:
    if not trigger:
        return False
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+-]*", trigger):
        return bool(re.search(rf"(?<![A-Za-z0-9]){re.escape(trigger)}(?![A-Za-z0-9])", query, flags=re.I))
    return trigger in query or trigger.lower() in query.lower()


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


def skippable_guideline_line(line: str) -> bool:
    if "Downloaded" in line:
        return True
    # Keep quoted recommendations such as "> **7.15** ..."; skip only italic
    # copyright/citation footers that commonly begin with "> *...".
    return bool(re.match(r"^>\s+\*(?!\*)", line))


def guideline_source_label(source_name: str, text: str = "") -> str:
    lower = f"{source_name}\n{text[:5000]}".lower()
    if "kdigo" in lower:
        if "2026" in lower and ("public review" in lower or "draft" in lower):
            return "KDIGO 2026 Diabetes and CKD Guideline Update (Public Review Draft)"
        if "2024" in lower and ("ckd" in lower or "chronic kidney disease" in lower):
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
    parent_text: str,
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
            metadata = structured_metadata(source_label, title, section, "table_row", chunk_text, parent_text)
            chunks.append(
                KnowledgeChunk(
                    source,
                    source_label,
                    title,
                    section,
                    "table_row",
                    chunk_text,
                    parent_text,
                    metadata,
                    chunk_tokens(source_label, title, section, "table_row", chunk_text, metadata),
                )
            )
    return chunks


def section_summary_chunk(
    source: str,
    source_label: str,
    title: str,
    section: str,
    lines: list[str],
    parent_text: str,
) -> KnowledgeChunk | None:
    summary_lines = [line for line in lines if line and not line.startswith("|")][:8]
    if not summary_lines:
        return None
    summary_text = "\n".join(
        [
            f"Section map: {title}",
            f"Section: {section}",
            "Key opening context:",
            *summary_lines,
        ]
    )
    metadata = structured_metadata(source_label, title, section, "section_summary", summary_text, parent_text)
    return KnowledgeChunk(
        source,
        source_label,
        title,
        section,
        "section_summary",
        summary_text,
        parent_text,
        metadata,
        chunk_tokens(source_label, title, section, "section_summary", summary_text, metadata),
    )


def recommendation_chunks_from_lines(
    source: str,
    source_label: str,
    title: str,
    section: str,
    lines: list[str],
    parent_text: str,
) -> list[KnowledgeChunk]:
    chunks: list[KnowledgeChunk] = []
    for index, line in enumerate(lines):
        match = RECOMMENDATION_START_RE.match(line)
        if not match:
            continue
        recommendation_id = match.group("ada") or match.group("other") or ""
        rec_lines = [line]
        for follow in lines[index + 1 : index + 3]:
            if RECOMMENDATION_START_RE.match(follow):
                break
            if looks_like_recommendation_continuation(follow):
                rec_lines.append(follow)
        chunk_text = "\n".join(rec_lines)
        if recommendation_id:
            chunk_text = f"Recommendation {recommendation_id}: {chunk_text}"
        metadata = structured_metadata(source_label, title, section, "recommendation", chunk_text, parent_text)
        chunks.append(
            KnowledgeChunk(
                source,
                source_label,
                title,
                section,
                "recommendation",
                chunk_text,
                parent_text,
                metadata,
                chunk_tokens(source_label, title, section, "recommendation", chunk_text, metadata),
            )
        )
    return chunks


def looks_like_recommendation_continuation(line: str) -> bool:
    if not line or line.startswith("|") or "<tr" in line.lower():
        return False
    if re.match(r"^#{1,6}\s+", line):
        return False
    if re.match(r"^\s*>", line):
        return True
    return len(line) < 360 and bool(
        re.search(r"\b(consider|recommend|should|may|screen|monitor|treat|assess|refer|prescribe|avoid)\b", line, flags=re.I)
    )


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


def search_whole_section_context(query: str, seed_hits: list[KnowledgeHit]) -> list[KnowledgeHit]:
    kb = load_knowledge_base()
    if not kb or not seed_hits:
        return []

    max_sections = int(os.getenv("LINE_WHOLE_SECTION_CONTEXT_MAX_SECTIONS", "2"))
    max_chars = int(os.getenv("LINE_WHOLE_SECTION_CONTEXT_CHARS", "9000"))
    query_tokens = list(expand_query_tokens(query))
    results: list[KnowledgeHit] = []
    seen: set[tuple[str, str]] = set()

    for hit in seed_hits:
        if len(results) >= max_sections:
            break
        key = (hit.source, hit.section)
        if key in seen:
            continue
        seen.add(key)
        chunk = best_section_context_chunk(kb, hit)
        if not chunk:
            continue
        section_text = chunk.parent_text or chunk.text
        if not section_text.strip():
            continue
        metadata = tuple(dedupe_terms([*chunk.metadata, "whole_section_context"]))
        results.append(
            KnowledgeHit(
                source=chunk.source,
                source_label=chunk.source_label,
                title=chunk.title,
                section=chunk.section,
                chunk_type="whole_section",
                excerpt=best_excerpt(section_text, query_tokens, max_chars),
                parent_excerpt="",
                metadata=metadata,
                score=hit.score + 12.0,
            )
        )
    return results


def best_section_context_chunk(kb: KnowledgeBase, hit: KnowledgeHit) -> KnowledgeChunk | None:
    candidates = [
        chunk
        for chunk in kb.chunks
        if chunk.source == hit.source and chunk.section == hit.section and (chunk.parent_text or chunk.text)
    ]
    if not candidates:
        return None
    text_chunks = [chunk for chunk in candidates if chunk.chunk_type == "text"]
    candidates = text_chunks or candidates
    return max(candidates, key=lambda chunk: len(chunk.parent_text or chunk.text))


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
        metadata_line = ", ".join(hit.metadata[:18])
        lines.extend(
            [
                f"\n[{index}] {public_metadata(hit.title)}",
                f"來源指南：{hit.source_label}",
                f"章節：{public_metadata(hit.section)}",
                f"片段類型：{hit.chunk_type}",
                f"結構化標籤：{metadata_line or '無'}",
                f"片段：{hit.excerpt}",
            ]
        )
        if hit.parent_excerpt and hit.parent_excerpt != hit.excerpt:
            lines.append(f"父層章節上下文：{hit.parent_excerpt}")
    return "\n".join(lines)


def knowledge_candidates_prompt(hits: list[KnowledgeHit]) -> str:
    if not hits:
        return "\n\n候選指南片段：無。"
    lines = [
        "\n\n候選指南片段：以下為初步召回的候選片段，請只用來做 rerank/coverage，不可用模型內建知識補充。",
    ]
    for index, hit in enumerate(hits, start=1):
        metadata_line = ", ".join(hit.metadata[:18])
        lines.extend(
            [
                f"\n[{index}] {public_metadata(hit.title)}",
                f"來源指南：{hit.source_label}",
                f"章節：{public_metadata(hit.section)}",
                f"片段類型：{hit.chunk_type}",
                f"結構化標籤：{metadata_line or '無'}",
                f"召回分數：{hit.score:.2f}",
                f"片段：{hit.excerpt}",
            ]
        )
        if hit.parent_excerpt and hit.parent_excerpt != hit.excerpt:
            lines.append(f"父層章節上下文：{hit.parent_excerpt}")
    return "\n".join(lines)


def knowledge_status() -> dict[str, object]:
    kb = load_knowledge_base()
    roots = knowledge_dirs()
    extras = extra_knowledge_paths()
    extra_existing = [path for path in extras if path.exists() and path.is_file()]
    dir_file_count = sum(len(list(root.rglob("*.md"))) for root in roots if root.exists() and root.is_dir())
    loaded_files_by_source: dict[str, int] = {}
    loaded_dirs_by_source: dict[str, list[str]] = {}
    chunk_type_counts: dict[str, int] = {}
    ontology_tagged_chunks = 0
    if kb:
        for path in kb.source_files:
            label = guideline_source_label(str(path), path.read_text(encoding="utf-8", errors="ignore")[:5000])
            loaded_files_by_source[label] = loaded_files_by_source.get(label, 0) + 1
            dir_value = str(path.parent)
            loaded_dirs_by_source.setdefault(label, [])
            if dir_value not in loaded_dirs_by_source[label]:
                loaded_dirs_by_source[label].append(dir_value)
        for chunk in kb.chunks:
            chunk_type_counts[chunk.chunk_type] = chunk_type_counts.get(chunk.chunk_type, 0) + 1
            if any(tag.startswith("ontology:") for tag in chunk.metadata):
                ontology_tagged_chunks += 1
    return {
        "enabled": knowledge_enabled(),
        "dir": str(roots[0]) if roots else "",
        "dirs": [str(root) for root in roots],
        "recommended_dirs": standard_guideline_dirs(),
        "extra_paths": [str(path) for path in extras],
        "available": bool(kb),
        "strict": knowledge_strict_enabled(),
        "chunks": len(kb.chunks) if kb else 0,
        "chunk_type_counts": chunk_type_counts,
        "metadata_tagged_chunks": sum(1 for chunk in kb.chunks if chunk.metadata) if kb else 0,
        "inverted_index_enabled": inverted_index_enabled(),
        "inverted_index_terms": len(kb.token_postings) if kb else 0,
        "vector_index_chunks": len(kb.vector_index) if kb else 0,
        "dense_embedding_enabled": dense_embedding_enabled(),
        "dense_embedding_provider": dense_embedding_provider(),
        "dense_embedding_model": dense_embedding_model(),
        "dense_vector_index_chunks": sum(1 for vector in kb.dense_vector_index if vector) if kb else 0,
        "dense_embedding_cache": str(dense_embedding_cache_path()),
        "dense_embedding_error": kb.dense_embedding_error if kb else "",
        "ontology_tagged_chunks": ontology_tagged_chunks,
        "files": len(kb.source_files) if kb else 0,
        "dir_files": dir_file_count,
        "extra_files": len(extra_existing),
        "sources": sorted({chunk.source_label for chunk in kb.chunks}) if kb else [],
        "source_file_counts": loaded_files_by_source,
        "source_dirs": loaded_dirs_by_source,
        "keyword_files": [str(path) for path in keyword_files()],
        "keyword_entries": len(load_keyword_entries()),
    }


def tokenize(text: str) -> Iterable[str]:
    for token in TOKEN_RE.findall(text.lower()):
        token = token.strip()
        if len(token) <= 1 and not token.isdigit() and not re.match(r"[\u4e00-\u9fff]", token):
            continue
        yield token


def hashed_vector(tokens: Iterable[str]) -> dict[int, float]:
    dim = max(64, int(os.getenv("LINE_KNOWLEDGE_VECTOR_DIM", "768")))
    counts: dict[int, float] = {}
    for token in tokens:
        value = token.strip().lower()
        if not value:
            continue
        digest = hashlib.blake2b(value.encode("utf-8", errors="ignore"), digest_size=4).digest()
        bucket = int.from_bytes(digest, "big") % dim
        counts[bucket] = counts.get(bucket, 0.0) + 1.0
    norm = math.sqrt(sum(value * value for value in counts.values()))
    if norm <= 0:
        return {}
    return {key: value / norm for key, value in counts.items()}


def sparse_cosine(left: dict[int, float], right: dict[int, float]) -> float:
    if not left or not right:
        return 0.0
    if len(left) > len(right):
        left, right = right, left
    return sum(value * right.get(key, 0.0) for key, value in left.items())


_dense_query_cache: dict[str, list[float]] = {}


def env_enabled(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() not in {"", "0", "false", "no", "off"}


def dense_embedding_enabled() -> bool:
    return env_enabled("LINE_DENSE_EMBEDDING_ENABLED", "0")


def inverted_index_enabled() -> bool:
    return env_enabled("LINE_KNOWLEDGE_INVERTED_INDEX_ENABLED", "1")


def dense_embedding_provider() -> str:
    return os.getenv("LINE_DENSE_EMBEDDING_PROVIDER", "gemini").strip().lower()


def dense_embedding_model() -> str:
    return os.getenv("LINE_DENSE_EMBEDDING_MODEL", "text-embedding-004").strip()


def dense_embedding_cache_path() -> Path:
    return Path(os.getenv("LINE_DENSE_EMBEDDING_CACHE", "/tmp/line_lifebot_dense_embeddings.jsonl")).expanduser()


def dense_embedding_api_key() -> str:
    if dense_embedding_provider() == "gemini":
        return os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()
    return ""


def dense_embedding_text(chunk: KnowledgeChunk) -> str:
    metadata = " ".join(chunk.metadata[:32])
    value = "\n".join(
        [
            f"Source: {chunk.source_label}",
            f"Title: {chunk.title}",
            f"Section: {chunk.section}",
            f"Type: {chunk.chunk_type}",
            f"Metadata: {metadata}",
            chunk.text,
        ]
    )
    max_chars = int(os.getenv("LINE_DENSE_EMBEDDING_TEXT_CHARS", "1800"))
    return value[:max_chars]


def dense_cache_key(chunk: KnowledgeChunk) -> str:
    digest = hashlib.sha1(dense_embedding_text(chunk).encode("utf-8", errors="ignore")).hexdigest()[:18]
    return "|".join([dense_embedding_provider(), dense_embedding_model(), chunk.source, chunk.section, chunk.chunk_type, digest])


def normalize_dense_vector(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 0:
        return []
    return [value / norm for value in vector]


def dense_cosine(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return sum(a * b for a, b in zip(left, right))


def load_dense_embedding_cache(path: Path) -> dict[str, list[float]]:
    if not path.exists() or not path.is_file():
        return {}
    cache: dict[str, list[float]] = {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = str(item.get("key") or "")
                values = item.get("values")
                if key and isinstance(values, list):
                    vector = normalize_dense_vector([float(value) for value in values])
                    if vector:
                        cache[key] = vector
    except OSError as exc:
        print(f"dense embedding cache read failed: {path}: {type(exc).__name__}: {exc}")
    return cache


def write_dense_embedding_cache(path: Path, cache: dict[str, list[float]]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            for key, vector in cache.items():
                handle.write(json.dumps({"key": key, "values": vector}, ensure_ascii=False) + "\n")
        tmp_path.replace(path)
    except OSError as exc:
        print(f"dense embedding cache write failed: {path}: {type(exc).__name__}: {exc}")


def build_dense_vector_index(chunks: list[KnowledgeChunk]) -> tuple[list[list[float]], str]:
    if not dense_embedding_enabled():
        return [], ""
    api_key = dense_embedding_api_key()
    if not api_key:
        return [], "LINE_DENSE_EMBEDDING_ENABLED=1 but no Gemini API key is configured"

    cache_path = dense_embedding_cache_path()
    cache = load_dense_embedding_cache(cache_path)
    keys = [dense_cache_key(chunk) for chunk in chunks]
    vectors: list[list[float]] = [cache.get(key, []) for key in keys]
    missing = [index for index, vector in enumerate(vectors) if not vector]
    max_chunks = int(os.getenv("LINE_DENSE_EMBEDDING_MAX_CHUNKS", "0"))
    if max_chunks > 0:
        missing = missing[:max_chunks]
    if missing:
        batch_size = max(1, int(os.getenv("LINE_DENSE_EMBEDDING_BATCH_SIZE", "24")))
        try:
            for start in range(0, len(missing), batch_size):
                batch_indexes = missing[start : start + batch_size]
                texts = [dense_embedding_text(chunks[index]) for index in batch_indexes]
                batch_vectors = gemini_embed_texts(api_key, texts)
                for chunk_index, vector in zip(batch_indexes, batch_vectors):
                    if vector:
                        vectors[chunk_index] = vector
                        cache[keys[chunk_index]] = vector
            write_dense_embedding_cache(cache_path, cache)
        except (OSError, urllib.error.URLError, ValueError) as exc:
            return vectors, f"dense embedding build failed: {type(exc).__name__}: {exc}"
    return vectors, ""


def dense_embed_query(query: str) -> list[float]:
    if not dense_embedding_enabled():
        return []
    key = f"{dense_embedding_provider()}|{dense_embedding_model()}|query|{hashlib.sha1(query.encode('utf-8', errors='ignore')).hexdigest()[:18]}"
    if key in _dense_query_cache:
        return _dense_query_cache[key]
    api_key = dense_embedding_api_key()
    if not api_key:
        return []
    try:
        vectors = gemini_embed_texts(api_key, [query])
    except (OSError, urllib.error.URLError, ValueError) as exc:
        print(f"dense query embedding failed: {type(exc).__name__}: {exc}")
        return []
    vector = vectors[0] if vectors else []
    if vector:
        _dense_query_cache[key] = vector
    return vector


def gemini_embed_texts(api_key: str, texts: list[str]) -> list[list[float]]:
    if dense_embedding_provider() != "gemini":
        raise ValueError(f"unsupported dense embedding provider: {dense_embedding_provider()}")
    model = dense_embedding_model()
    model_name = model if model.startswith("models/") else f"models/{model}"
    url_model = model.removeprefix("models/")
    body = {
        "requests": [
            {
                "model": model_name,
                "content": {"parts": [{"text": text}]},
            }
            for text in texts
        ]
    }
    request = urllib.request.Request(
        f"{GEMINI_EMBEDDING_API_BASE}/{url_model}:batchEmbedContents",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "x-goog-api-key": api_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    timeout = int(os.getenv("LINE_DENSE_EMBEDDING_TIMEOUT", "30"))
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    vectors: list[list[float]] = []
    for item in payload.get("embeddings", []):
        values = item.get("values") or []
        vectors.append(normalize_dense_vector([float(value) for value in values]))
    if len(vectors) != len(texts):
        raise ValueError(f"Gemini embedding returned {len(vectors)} vectors for {len(texts)} texts")
    return vectors


def chunk_tokens(
    source_label: str,
    title: str,
    section: str,
    chunk_type: str,
    text: str,
    metadata: tuple[str, ...] = (),
) -> tuple[str, ...]:
    indexed_metadata = f"{source_label} {title} {section} {chunk_type} {' '.join(metadata)}"
    return tuple(tokenize(f"{indexed_metadata}\n{text}"))


ONTOLOGY_PATTERNS: dict[str, tuple[tuple[str, str], ...]] = {
    "disease": (
        ("type_1_diabetes", r"\b(type 1 diabetes|t1d)\b|第一型糖尿病"),
        ("type_2_diabetes", r"\b(type 2 diabetes|t2d)\b|第二型糖尿病"),
        ("prediabetes", r"\bprediabetes\b|糖尿病前期"),
        ("ckd", r"\b(ckd|chronic kidney disease|diabetic kidney disease|dkd|renal impairment)\b|腎"),
        ("ascvd", r"\b(ascvd|atherosclerotic cardiovascular disease|coronary|stroke)\b|心血管"),
        ("heart_failure", r"\b(heart failure|hfref|hfr?ef|hfpef)\b|心衰"),
        ("hypertension", r"\b(hypertension|blood pressure)\b|高血壓|血壓"),
        ("dyslipidemia", r"\b(dyslipidemia|lipid|cholesterol|triglyceride)\b|血脂|膽固醇"),
        ("obesity", r"\b(obesity|overweight|adiposity|weight management)\b|肥胖|過重"),
        ("masld_mash", r"\b(masld|mash|nafld|nash|steatotic liver|fatty liver|steatohepatitis)\b|脂肪肝"),
        ("retinopathy", r"\b(retinopathy|retinal|macular edema|dme|npdr|pdr)\b|視網膜|黃斑"),
        ("neuropathy", r"\b(neuropathy|autonomic neuropathy|peripheral neuropathy)\b|神經病變"),
        ("foot_ulcer_pad", r"\b(foot ulcer|pad|peripheral artery disease|wound|amputation)\b|足部|潰瘍|傷口"),
    ),
    "drug": (
        ("metformin", r"\bmetformin\b"),
        ("sglt2_inhibitor", r"\b(sglt2|sglt-2|empagliflozin|dapagliflozin|canagliflozin)\b"),
        ("glp1_ra", r"\b(glp-?1|semaglutide|liraglutide|dulaglutide)\b"),
        ("dual_gip_glp1_ra", r"\b(tirzepatide|dual gip)\b"),
        ("insulin", r"\binsulin\b|胰島素"),
        ("sulfonylurea", r"\b(sulfonylurea|glyburide|glipizide|glimepiride)\b"),
        ("thiazolidinedione", r"\b(thiazolidinedione|tzd|pioglitazone)\b"),
        ("dpp4_inhibitor", r"\b(dpp-?4|sitagliptin|linagliptin)\b"),
        ("finerenone_ns_mra", r"\b(finerenone|nonsteroidal mra|nsmra)\b"),
        ("acei_arb", r"\b(ace inhibitor|acei|arb|angiotensin receptor blocker)\b"),
        ("statin", r"\bstatin\b"),
        ("anti_vegf", r"\b(anti-?vegf|aflibercept|ranibizumab|bevacizumab)\b"),
        ("glucagon", r"\bglucagon\b"),
        ("glucocorticoid", r"\b(glucocorticoid|steroid|corticosteroid|prednisone|prednisolone|dexamethasone)\b|類固醇"),
    ),
    "test": (
        ("a1c", r"\b(a1c|hba1c)\b"),
        ("cgm", r"\b(cgm|continuous glucose monitoring|time in range|tir)\b|連續血糖"),
        ("bgm_smbg", r"\b(bgm|smbg|blood glucose monitoring)\b|血糖機"),
        ("egfr", r"\begfr\b|腎絲球|過濾率"),
        ("uacr", r"\b(uacr|albuminuria|albumin-to-creatinine)\b|尿蛋白|白蛋白尿"),
        ("ogtt", r"\bogtt|oral glucose tolerance\b"),
        ("lipids", r"\b(ldl|hdl|triglyceride|lipid panel)\b"),
        ("blood_pressure", r"\bblood pressure\b|血壓"),
        ("bmi", r"\bbmi\b|body mass index"),
        ("retinal_exam", r"\b(dilated eye|retinal photography|ophthalmologist|eye examination)\b|眼底"),
        ("monofilament", r"\b(monofilament|protective sensation)\b"),
    ),
    "population": (
        ("older_adults", r"\b(older adults|geriatric|frailty)\b|老人|長者|高齡"),
        ("children_adolescents", r"\b(children|adolescents|youth|pediatric)\b|兒童|青少年"),
        ("pregnancy", r"\b(pregnancy|gestational|preconception|postpartum)\b|懷孕|妊娠|產後"),
        ("hospitalized", r"\b(hospital|inpatient|critical illness)\b|住院"),
        ("perioperative", r"\b(perioperative|surgery|procedure)\b|手術"),
        ("dialysis", r"\b(dialysis|eskd|esrd|kidney failure)\b|洗腎|透析"),
    ),
    "task": (
        ("diagnosis", r"\b(diagnosis|diagnostic|criteria|classification)\b|診斷"),
        ("screening", r"\b(screening|screen)\b|篩檢"),
        ("treatment", r"\b(treatment|therapy|management|pharmacologic|intervention)\b|治療|處理"),
        ("monitoring", r"\b(monitoring|follow-up|surveillance)\b|監測|追蹤"),
        ("safety", r"\b(safety|contraindication|avoid|adverse|risk)\b|安全|禁忌|風險"),
        ("dose_adjustment", r"\b(dose|dosage|adjustment|renal dose)\b|劑量"),
        ("target", r"\b(goal|target)\b|目標"),
        ("staging", r"\b(staging|stage|severity|mild|moderate|severe|classification)\b|分期|分級|嚴重度"),
    ),
}


def ontology_metadata_tags(haystack: str) -> list[str]:
    tags: list[str] = []
    for category, entries in ONTOLOGY_PATTERNS.items():
        for name, pattern in entries:
            if re.search(pattern, haystack, flags=re.I):
                tags.append(f"ontology:{category}:{name}")
    if re.search(r"[<>≤≥=]\s*\d|\b\d+(?:\.\d+)?\s*(?:%|mg/dl|mmol/l|mg/g|ml/min)\b", haystack, flags=re.I):
        tags.append("ontology:value:numeric_cutoff")
    if re.search(r"\begfr\b.{0,40}[<>≤≥=]?\s*\d+|[<>≤≥=]\s*\d+.{0,40}\begfr\b", haystack, flags=re.I):
        tags.append("ontology:value:egfr_cutoff")
    if re.search(r"\b(uacr|albuminuria)\b.{0,40}[<>≤≥=]?\s*\d+|[<>≤≥=]\s*\d+.{0,40}\b(uacr|albuminuria)\b", haystack, flags=re.I):
        tags.append("ontology:value:uacr_cutoff")
    if re.search(r"\b(a1c|hba1c)\b.{0,40}[<>≤≥=]?\s*\d+|[<>≤≥=]\s*\d+.{0,40}\b(a1c|hba1c)\b", haystack, flags=re.I):
        tags.append("ontology:value:a1c_cutoff")
    return tags


def structured_metadata(
    source_label: str,
    title: str,
    section: str,
    chunk_type: str,
    text: str,
    parent_text: str = "",
) -> tuple[str, ...]:
    haystack = f"{source_label} {title} {section} {chunk_type} {text} {parent_text[:1400]}".lower()
    tags: list[str] = []

    if "kdigo" in haystack:
        tags.append("source:kdigo")
    elif "aace" in haystack:
        tags.append("source:aace")
    elif "ada standards" in haystack or re.search(r"\bdc26s\d+\b", haystack):
        tags.append("source:ada")

    year_match = re.search(r"\b(20\d{2})\b", haystack)
    if year_match:
        tags.append(f"guideline_year:{year_match.group(1)}")
    chapter_match = re.search(r"\bdc26s(\d{3})\b", haystack)
    if chapter_match:
        tags.append(f"ada_chapter:s{int(chapter_match.group(1))}")

    tags.append(f"chunk_type:{chunk_type}")
    if chunk_type == "table_row" or re.search(r"\btable\s+\d", haystack):
        tags.append("has_table")
    recommendation_match = re.search(r"\brecommendation\s+(\d[\dA-Za-z.-]*)|\*\*(\d{1,2}\.\d+[a-z]?)\*\*", text, flags=re.I)
    if chunk_type == "recommendation" or re.search(r"\brecommendations?\b|\*\*\d+\.\d+", haystack):
        tags.append("has_recommendation")
    if recommendation_match:
        tags.append(f"recommendation_id:{recommendation_match.group(1) or recommendation_match.group(2)}")
    grade_match = re.search(r"(?:\*\*)?\b([abce])\b(?:\*\*)?\s*$", text.strip(), flags=re.I)
    if grade_match:
        tags.append("has_recommendation_grade")
        tags.append(f"recommendation_grade:{grade_match.group(1).lower()}")
    if re.search(r"[<>≤≥=]\s*\d|\b\d+(?:\.\d+)?\s*(?:%|mg/dl|mmol/l|ml/min|mg/g)\b", haystack):
        tags.append("has_threshold")

    clinical_patterns = {
        "ckd": r"\b(ckd|chronic kidney disease|diabetic kidney disease|dkd|kidney disease|renal|nephropathy)\b|腎",
        "egfr": r"\begfr\b|glomerular filtration|腎絲球|過濾率",
        "uacr": r"\b(uacr|albuminuria|albumin-to-creatinine|proteinuria)\b|尿蛋白|白蛋白尿",
        "sglt2": r"\bsglt2|sglt-2|sodium-glucose cotransporter 2\b",
        "glp1": r"\bglp-?1|glucagon-like peptide|semaglutide|liraglutide|dulaglutide\b",
        "finerenone": r"\bfinerenone|nonsteroidal mra|nsmra|mineralocorticoid receptor antagonist\b",
        "metformin": r"\bmetformin\b",
        "insulin": r"\binsulin\b|胰島素",
        "hypoglycemia": r"\bhypoglycemia\b|低血糖",
        "steroid_hyperglycemia": r"\b(glucocorticoid|steroid-induced hyperglycemia|steroid.*hyperglycemia|corticosteroid|prednisone|prednisolone|dexamethasone|nph insulin)\b|類固醇",
        "ascvd": r"\bascvd|cardiovascular disease|coronary|stroke|peripheral artery\b|心血管",
        "heart_failure": r"\bheart failure|hfr?ef|hfpef\b|心衰",
        "hypertension": r"\bhypertension|blood pressure\b|血壓",
        "lipid": r"\blipid|statin|cholesterol|triglyceride\b|膽固醇",
        "obesity": r"\bobesity|overweight|weight management|adiposity\b|肥胖|體重",
        "masld": r"\bmasld|mash|nafld|nash|steatotic liver|steatohepatitis|fatty liver|cirrhosis|fibrosis\b|脂肪肝|肝硬化|肝纖維",
        "pregnancy": r"\bpregnancy|gestational|gdm|preconception|postpartum\b|懷孕|妊娠",
        "older_adults": r"\bolder adults|geriatric|frailty|cognitive impairment\b|老人|長者|高齡",
        "children": r"\bchildren|adolescents|youth|pediatric\b|兒童|青少年",
        "hospital": r"\bhospital|inpatient|critical illness|perioperative|surgery\b|住院|手術",
        "retinopathy": r"\bretinopathy|retinal|eye examination\b|視網膜|眼",
        "retinopathy_staging": r"\b(nonproliferative diabetic retinopathy|proliferative diabetic retinopathy|npdr|pdr|diabetic macular edema|dme|microaneurysms|neovascularization|severity|staging)\b|分期|分級|嚴重度",
        "retinopathy_treatment": r"\b(anti-vegf|vascular endothelial growth factor|panretinal laser photocoagulation|photocoagulation|vitrectomy|corticosteroid|focal/grid|macular edema treatment|emerging therapies)\b|雷射|注射|治療",
        "neuropathy": r"\bneuropathy|monofilament|foot ulcer|foot care\b|神經|足|腳",
        "technology": r"\bcgm|bgm|smbg|time in range|automated insulin delivery\b|連續血糖|血糖機",
        "technology_indication": r"\b(use of cgm is recommended|recommended at diabetes onset|people with diabetes.*cgm|cgm.*recommended|offered to people with diabetes|on insulin therapy|noninsulin therapies that can cause hypoglycemia|periodic use of personal or professional cgm|individual circumstances preferences needs)\b|適用|適合|哪些病人",
        "diagnosis": r"\bdiagnosis|diagnostic|screening|ogtt|classification|prediabetes\b|診斷|篩檢",
    }
    for tag, pattern in clinical_patterns.items():
        if re.search(pattern, haystack, flags=re.I):
            tags.append(tag)
    tags.extend(ontology_metadata_tags(haystack))

    return tuple(dedupe_terms(tags))


def parent_excerpt_for_chunk(chunk: KnowledgeChunk, query_tokens: list[str]) -> str:
    if not chunk.parent_text:
        return ""
    parent_compact = re.sub(r"\s+", " ", chunk.parent_text).strip()
    text_compact = re.sub(r"\s+", " ", chunk.text).strip()
    if not parent_compact or parent_compact == text_compact:
        return ""
    max_chars = int(os.getenv("LINE_KNOWLEDGE_PARENT_SECTION_CHARS", "1800"))
    return best_excerpt(parent_compact, query_tokens, max_chars)


def chunk_dedup_key(chunk: KnowledgeChunk) -> tuple[str, ...]:
    if chunk.chunk_type in {"table_row", "recommendation"}:
        digest = hashlib.sha1(chunk.text[:500].encode("utf-8", errors="ignore")).hexdigest()[:12]
        return (chunk.source, chunk.section, chunk.chunk_type, digest)
    return (chunk.source, chunk.section, chunk.chunk_type)


def hit_dedup_key(hit: KnowledgeHit) -> tuple[str, ...]:
    if hit.chunk_type in {"table_row", "recommendation"}:
        digest = hashlib.sha1(hit.excerpt[:500].encode("utf-8", errors="ignore")).hexdigest()[:12]
        return (hit.source, hit.section, hit.chunk_type, digest)
    return (hit.source, hit.section, hit.chunk_type)


def query_variants(query: str) -> list[str]:
    return [variant.text for variant in query_variant_specs(query)]


def query_variant_specs(query: str) -> list[QueryVariant]:
    variants: list[QueryVariant] = [QueryVariant("original", query, 1.0)]
    query_lower = query.lower()
    brain_plan = clinical_search_brain_plan(query)

    expansion_terms: list[str] = []
    for key, terms in QUERY_EXPANSIONS.items():
        if key in query:
            expansion_terms.extend(terms)
    keyword_entries = matched_keyword_entries(query)
    for entry in keyword_entries:
        expansion_terms.extend(entry.expansions)
    if expansion_terms:
        variants.append(QueryVariant("synonyms", " ".join([query, *dedupe_terms(expansion_terms)]), 0.9))

    hyperglycemic_crisis_query = any(term in query for term in ("HHNK", "高滲透壓", "高血糖高滲透壓", "高血糖急症", "酮酸", "酮酸中毒")) or any(
        term in query_lower for term in ("hhnk", "hhs", "dka", "ketoacidosis", "hyperosmolar", "hyperglycemic crisis", "hyperglycemic crises")
    )
    if hyperglycemic_crisis_query:
        variants.extend(
            [
                QueryVariant(
                    "hhs_diagnosis",
                    f"{query} ADA section 16 dc26s016 hyperglycemic crises HHS hyperosmolar hyperglycemic state diagnostic criteria Table 16.1 plasma glucose serum osmolality ketones pH bicarbonate",
                    1.02,
                ),
                QueryVariant(
                    "hhs_treatment",
                    f"{query} ADA recommendation 16.16 DKA HHS treatment intravenous fluids insulin electrolytes potassium monitoring transition subcutaneous insulin precipitating cause Figure 16.1",
                    1.02,
                ),
            ]
        )
    steroid_hospital_query = "hospital_steroid_hyperglycemia" in query_concepts(query, query_lower)
    if steroid_hospital_query:
        variants.extend(
            [
                QueryVariant(
                    "steroid_hospital_s16",
                    f"{query} ADA section 16 dc26s016 Diabetes Care in the Hospital glucocorticoid therapy steroid-induced hyperglycemia hospitalized NPH insulin prednisone prednisolone dexamethasone basal insulin prandial correction insulin point-of-care blood glucose monitoring",
                    1.02,
                ),
                QueryVariant(
                    "steroid_hospital_s9",
                    f"{query} ADA section 9 recommendation 9.36 glucocorticoid treatment plan steroid-induced hyperglycemia NPH morning dosing insulin frequent reassessment",
                    0.98,
                ),
            ]
        )
    for index, brain_query in enumerate(brain_plan.get("search_queries", [])[:4], start=1):
        variants.append(QueryVariant(f"clinical_brain_{index}", f"{query} {brain_query}", 0.92))

    for triggers, intent_queries in QUERY_INTENT_VARIANTS:
        if any(trigger in query or trigger in query_lower for trigger in triggers):
            variants.extend(QueryVariant("section_intent", f"{query} {intent_query}", 0.84) for intent_query in intent_queries)
    for entry in keyword_entries:
        variants.extend(
            QueryVariant(f"keyword_{entry.module}_{entry.entry_id}", f"{query} {variant_query}", 0.84)
            for variant_query in entry.variant_queries[:2]
        )

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
    if hyperglycemic_crisis_query:
        variants.extend(
            [
                QueryVariant(
                    "hhs_diagnosis",
                    f"{query} ADA section 16 dc26s016 hyperglycemic crises HHS hyperosmolar hyperglycemic state diagnostic criteria Table 16.1 plasma glucose serum osmolality ketones pH bicarbonate",
                    0.96,
                ),
                QueryVariant(
                    "hhs_treatment",
                    f"{query} ADA recommendation 16.16 DKA HHS treatment intravenous fluids insulin electrolytes potassium monitoring transition subcutaneous insulin precipitating cause Figure 16.1",
                    0.96,
                ),
            ]
        )

    variants.extend(coverage_query_variants(query, query_lower))
    variants.extend(concept_route_variants(query, query_lower))

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
    variant_limit = max(8, int(os.getenv("LINE_KNOWLEDGE_QUERY_VARIANT_LIMIT", "24")))
    return deduped[:variant_limit]


def concept_route_variants(query: str, query_lower: str) -> list[QueryVariant]:
    concepts = query_concepts(query, query_lower)
    variants: list[QueryVariant] = []
    if "retinopathy" in concepts:
        base = (
            f"{query} ADA section 12 dc26s012 diabetic retinopathy retinopathy "
            "Retinopathy Neuropathy and Foot Care ophthalmologist retinal disease"
        )
        if "staging" in concepts:
            base += " staging severity microaneurysms nonproliferative diabetic retinopathy NPDR proliferative diabetic retinopathy PDR diabetic macular edema DME"
        if "treatment" in concepts:
            base += " treatment anti-VEGF intravitreous injection panretinal laser photocoagulation macular focal grid photocoagulation corticosteroid vitrectomy emerging therapies"
        variants.append(QueryVariant("concept_retinopathy", base, 0.94))
    if "neuropathy" in concepts:
        variants.append(
            QueryVariant(
                "concept_neuropathy",
                f"{query} ADA section 12 dc26s012 diabetic neuropathy peripheral neuropathy autonomic neuropathy pain treatment foot care screening",
                0.9,
            )
        )
    if "foot_care" in concepts:
        variants.append(
            QueryVariant(
                "concept_foot_care",
                f"{query} ADA section 12 dc26s012 foot care ulcer monofilament loss of protective sensation PAD wound infection staging treatment",
                0.9,
            )
        )
    if "pad" in concepts:
        variants.extend(
            [
                QueryVariant(
                    "concept_pad_cardiovascular",
                    f"{query} ADA section 10 dc26s010 cardiovascular disease risk management peripheral artery disease PAD lower-extremity arterial disease ASCVD antiplatelet aspirin clopidogrel rivaroxaban statin lipid blood pressure smoking cessation GLP-1 RA semaglutide SGLT2 inhibitor limb outcomes",
                    0.97,
                ),
                QueryVariant(
                    "concept_pad_foot",
                    f"{query} ADA section 12 dc26s012 peripheral artery disease PAD lower extremity pulses claudication rest pain ABI toe pressures vascular assessment revascularization diabetic foot ulcer gangrene amputation semaglutide STRIDE lower-extremity complications",
                    0.94,
                ),
            ]
        )
    if "hyperglycemic_crisis" in concepts:
        variants.extend(
            [
                QueryVariant(
                    "concept_hhs_diagnosis",
                    f"{query} ADA section 16 dc26s016 Diabetes Care in the Hospital hyperglycemic crises HHS hyperosmolar hyperglycemic state diagnostic criteria Table 16.1 plasma glucose osmolality ketones pH bicarbonate DKA",
                    0.98,
                ),
                QueryVariant(
                    "concept_hhs_treatment",
                    f"{query} ADA recommendation 16.16 manage DKA HHS intravenous fluids insulin electrolytes monitoring transition maintenance subcutaneous insulin precipitating cause Figure 16.1",
                    0.98,
                ),
            ]
        )
    if "hospital_steroid_hyperglycemia" in concepts:
        variants.extend(
            [
                QueryVariant(
                    "concept_hospital_steroid_s16",
                    f"{query} ADA section 16 dc26s016 Diabetes Care in the Hospital glucocorticoid therapy hospitalized hyperglycemia NPH insulin prednisone prednisolone dexamethasone basal insulin prandial correction insulin point-of-care blood glucose monitoring",
                    0.98,
                ),
                QueryVariant(
                    "concept_hospital_steroid_s9",
                    f"{query} ADA section 9 recommendation 9.36 glucocorticoid treatment plan steroid-induced hyperglycemia NPH morning dose insulin frequent reassessment",
                    0.92,
                ),
            ]
        )
    return variants


CLINICAL_CONCEPT_PROFILES: dict[str, dict[str, list[str]]] = {
    "pad": {
        "concepts": ["peripheral artery disease", "lower-extremity arterial disease", "ASCVD", "diabetic foot PAD"],
        "target_chapters": [
            "ADA S10 Cardiovascular Disease and Risk Management",
            "ADA S12 Retinopathy, Neuropathy, and Foot Care",
        ],
        "evidence_targets": [
            "PAD and ASCVD secondary prevention",
            "antiplatelet therapy: aspirin, clopidogrel, or P2Y12 alternative when aspirin intolerance is relevant",
            "low-dose rivaroxaban plus aspirin when guideline criteria and bleeding risk allow",
            "lipid-lowering therapy / statin / LDL risk management",
            "blood pressure management",
            "smoking cessation",
            "GLP-1 RA / semaglutide limb outcome evidence such as STRIDE if present in guideline snippets",
            "vascular assessment / ABI / toe pressure / revascularization referral when limb ischemia or ulcer/gangrene is present",
        ],
        "avoid_routes": [
            "do not answer PAD drug therapy from general glucose-lowering medication tables alone",
            "do not treat lower-extremity arterial obstruction as neuropathy-only foot care",
        ],
        "required_facets": ["pad_context", "ascvd_context", "treatment"],
        "search_queries": [
            "ADA section 10 cardiovascular disease risk management peripheral artery disease PAD ASCVD antiplatelet aspirin clopidogrel rivaroxaban statin lipid blood pressure smoking cessation",
            "ADA section 12 foot care peripheral artery disease PAD lower extremity claudication ABI toe pressures vascular assessment revascularization gangrene amputation semaglutide STRIDE limb outcomes",
        ],
    },
    "retinopathy": {
        "concepts": ["diabetic retinopathy", "diabetic macular edema", "NPDR", "PDR"],
        "target_chapters": ["ADA S12 Retinopathy, Neuropathy, and Foot Care"],
        "evidence_targets": ["screening", "staging/severity", "anti-VEGF", "photocoagulation", "vitrectomy", "pregnancy monitoring"],
        "avoid_routes": ["do not answer eye disease from general diagnosis chapters alone"],
        "required_facets": ["retinopathy_context"],
        "search_queries": [
            "ADA section 12 diabetic retinopathy NPDR PDR diabetic macular edema DME anti-VEGF photocoagulation vitrectomy ophthalmologist"
        ],
    },
    "technology": {
        "concepts": ["continuous glucose monitoring", "diabetes technology"],
        "target_chapters": ["ADA S7 Diabetes Technology", "ADA S6 Glycemic Goals and Hypoglycemia"],
        "evidence_targets": ["CGM indications", "insulin therapy", "hypoglycemia risk", "time in range"],
        "avoid_routes": ["do not answer CGM indications only from generic monitoring text"],
        "required_facets": ["technology_indication", "monitoring"],
        "search_queries": [
            "ADA section 7 continuous glucose monitoring CGM recommended diabetes onset insulin therapy noninsulin therapies hypoglycemia time in range"
        ],
    },
    "ckd": {
        "concepts": ["CKD", "diabetic kidney disease", "eGFR", "albuminuria"],
        "target_chapters": ["ADA S11 CKD", "ADA S9 Pharmacologic Approaches", "KDIGO Diabetes and CKD"],
        "evidence_targets": ["SGLT2 inhibitor", "GLP-1 RA", "metformin eGFR limitation", "finerenone", "albuminuria/UACR", "hypoglycemia risk"],
        "avoid_routes": ["do not answer CKD medication selection from glucose efficacy alone"],
        "required_facets": ["kidney_context"],
        "search_queries": [
            "CKD diabetes eGFR albuminuria UACR SGLT2 inhibitor GLP-1 RA metformin finerenone kidney cardiovascular protection KDIGO ADA"
        ],
    },
    "liver": {
        "concepts": ["MASLD", "MASH", "NAFLD", "NASH", "steatotic liver disease"],
        "target_chapters": ["ADA S4 Comprehensive Medical Evaluation", "ADA S8 Obesity", "ADA S9 Pharmacologic Approaches"],
        "evidence_targets": ["weight loss", "GLP-1 RA", "tirzepatide", "pioglitazone", "cirrhosis safety"],
        "avoid_routes": ["do not answer liver disease from obesity text alone unless MASLD/MASH evidence is present"],
        "required_facets": ["liver_context", "treatment"],
        "search_queries": [
            "MASLD MASH NAFLD NASH steatotic liver disease diabetes obesity fibrosis cirrhosis GLP-1 receptor agonist tirzepatide pioglitazone weight loss"
        ],
    },
    "hyperglycemic_crisis": {
        "concepts": ["HHS", "hyperosmolar hyperglycemic state", "hyperglycemic crises", "DKA"],
        "target_chapters": ["ADA S16 Diabetes Care in the Hospital", "ADA S6 Glycemic Goals and Hyperglycemic Crises"],
        "evidence_targets": [
            "diagnostic criteria for DKA and HHS",
            "plasma glucose, ketones, pH or bicarbonate, and serum osmolality",
            "intravenous fluids, insulin, and electrolytes",
            "monitoring during treatment",
            "bridged transition to subcutaneous insulin",
            "identify and treat precipitating cause",
        ],
        "avoid_routes": ["do not answer HHNK/HHS from GDM or outpatient diagnosis criteria"],
        "required_facets": ["diagnosis", "treatment"],
        "search_queries": [
            "ADA section 16 dc26s016 hyperglycemic crises HHS hyperosmolar hyperglycemic state DKA diagnostic criteria Table 16.1 intravenous fluids insulin electrolytes Figure 16.1",
            "hyperglycemic hyperosmolar state HHS diagnosis treatment fluids insulin electrolytes osmolality potassium transition subcutaneous insulin",
        ],
    },
    "hospital_steroid_hyperglycemia": {
        "concepts": ["glucocorticoid-associated hyperglycemia", "steroid-induced hyperglycemia", "inpatient hyperglycemia"],
        "target_chapters": ["ADA S16 Diabetes Care in the Hospital", "ADA S9 Pharmacologic Approaches"],
        "evidence_targets": [
            "glucocorticoid type and duration",
            "NPH insulin with prednisone or prednisolone",
            "long-acting basal insulin for dexamethasone, long-acting glucocorticoids, multiple daily doses, or continuous glucocorticoid use",
            "prandial and correction insulin increases for higher-dose glucocorticoids",
            "daily adjustment based on glycemia and glucocorticoid treatment plan",
            "point-of-care blood glucose monitoring",
            "hypoglycemia risk when glucocorticoid dose is reduced",
        ],
        "avoid_routes": [
            "do not answer steroid-induced inpatient hyperglycemia from hypoglycemia treatment text alone",
            "do not answer from generic outpatient glucose-lowering medication tables alone",
        ],
        "required_facets": ["hospital_context", "steroid_context", "treatment"],
        "search_queries": [
            "ADA section 16 dc26s016 glucocorticoid therapy hospitalized hyperglycemia NPH insulin prednisone prednisolone dexamethasone basal insulin prandial correction insulin point-of-care blood glucose monitoring",
            "ADA section 9 recommendation 9.36 glucocorticoid treatment plan steroid-induced hyperglycemia NPH morning dose insulin frequent reassessment",
        ],
    },
}


def clinical_search_brain_plan(query: str) -> dict[str, list[str]]:
    lower = query.lower()
    concepts = query_concepts(query, lower)
    if any(term in query for term in ("連續血糖", "連續血糖監測", "新科技", "血糖機")) or any(
        term in lower for term in ("cgm", "continuous glucose", "diabetes technology")
    ):
        concepts.add("technology")
    if any(term in query for term in ("腎", "腎絲球", "尿蛋白", "白蛋白尿")) or any(
        term in lower for term in ("ckd", "kidney", "renal", "egfr", "uacr", "albuminuria")
    ):
        concepts.add("ckd")
    if any(term in query for term in ("脂肪肝", "脂肪性肝炎", "代謝性脂肪肝", "肝硬化", "肝纖維")) or any(
        term in lower for term in ("masld", "mash", "nafld", "nash", "steatotic liver", "steatohepatitis", "cirrhosis")
    ):
        concepts.add("liver")
    if any(term in query for term in ("HHNK", "高滲透壓", "高血糖高滲透壓", "高血糖急症", "酮酸", "酮酸中毒")) or any(
        term in lower for term in ("hhnk", "hhs", "dka", "ketoacidosis", "hyperosmolar", "hyperglycemic crisis", "hyperglycemic crises")
    ):
        concepts.add("hyperglycemic_crisis")
    if "hospital_steroid_hyperglycemia" in query_concepts(query, lower):
        concepts.add("hospital_steroid_hyperglycemia")

    plan: dict[str, list[str]] = {
        "concepts": [],
        "target_chapters": [],
        "evidence_targets": [],
        "avoid_routes": [],
        "required_facets": [],
        "search_queries": [],
    }
    for concept in sorted(concepts):
        profile = CLINICAL_CONCEPT_PROFILES.get(concept)
        if not profile:
            continue
        for key in plan:
            plan[key].extend(profile.get(key, []))
    return {key: dedupe_terms(values) for key, values in plan.items() if values}


def query_concepts(query: str, query_lower: str | None = None) -> set[str]:
    lower = query_lower if query_lower is not None else query.lower()
    concepts: set[str] = set()
    if any(term in query for term in ("視網膜", "眼底", "黃斑")) or any(
        term in lower for term in ("retinopathy", "retinal", "macular edema", "dme", "npdr", "pdr")
    ):
        concepts.add("retinopathy")
    if any(term in query for term in ("神經病變", "神經痛", "麻", "刺痛")) or any(
        term in lower for term in ("neuropathy", "peripheral neuropathy", "autonomic neuropathy")
    ):
        concepts.add("neuropathy")
    if any(term in query for term in ("足部", "腳", "傷口", "潰瘍")) or any(
        term in lower for term in ("foot", "ulcer", "monofilament", "pad", "wound")
    ):
        concepts.add("foot_care")
    if any(term in query for term in ("下肢", "動脈阻塞", "血管阻塞", "血管塞", "塞住", "循環不好", "週邊動脈", "周邊動脈", "跛行", "缺血", "壞疽")) or any(
        term in lower
        for term in (
            "peripheral artery disease",
            "peripheral arterial disease",
            "pad",
            "lower extremity",
            "lower-extremity",
            "lower limb",
            "claudication",
            "limb ischemia",
            "gangrene",
            "amputation",
            "abi",
            "toe pressure",
            "revascularization",
        )
    ):
        concepts.update({"pad", "ascvd"})
    if any(term in query for term in ("分期", "分級", "嚴重度", "第幾期", "程度")) or any(
        term in lower for term in ("staging", "stage", "severity", "classification", "mild", "moderate", "severe")
    ):
        concepts.add("staging")
    if any(term in query for term in ("治療", "處理", "怎麼辦", "用藥", "手術", "雷射", "注射")) or any(
        term in lower for term in ("treatment", "therapy", "intervention", "anti-vegf", "photocoagulation", "vitrectomy")
    ):
        concepts.add("treatment")
    if any(term in query for term in ("HHNK", "高滲透壓", "高血糖高滲透壓", "高血糖急症", "酮酸", "酮酸中毒")) or any(
        term in lower for term in ("hhnk", "hhs", "dka", "ketoacidosis", "hyperosmolar", "hyperglycemic crisis", "hyperglycemic crises")
    ):
        concepts.add("hyperglycemic_crisis")
    steroid_context = any(term in query for term in ("類固醇",)) or any(
        term in lower for term in ("glucocorticoid", "steroid", "corticosteroid", "prednisone", "prednisolone", "dexamethasone")
    )
    hospital_context = any(term in query for term in ("住院", "病房", "院內", "住院中")) or any(
        term in lower for term in ("hospital", "inpatient", "hospitalized", "noncritical illness", "critical illness")
    )
    hyperglycemia_context = any(term in query for term in ("高血糖", "血糖高", "血糖升高")) or any(
        term in lower for term in ("hyperglycemia", "glycemic management", "blood glucose")
    )
    if steroid_context and (hospital_context or hyperglycemia_context):
        concepts.add("hospital_steroid_hyperglycemia")
    return concepts


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
    steroid_hospital_query = "hospital_steroid_hyperglycemia" in query_concepts(query, query_lower)

    if steroid_hospital_query:
        variants.extend(
            [
                QueryVariant(
                    "coverage_steroid_hospital_s16",
                    f"{query} ADA section 16 dc26s016 glucocorticoid therapy hospitalized hyperglycemia NPH insulin prednisone prednisolone dexamethasone basal insulin prandial correction insulin point-of-care blood glucose monitoring",
                    0.94,
                ),
                QueryVariant(
                    "coverage_steroid_hospital_s9",
                    f"{query} ADA recommendation 9.36 glucocorticoid treatment plan steroid-induced hyperglycemia insulin NPH frequent reassessment",
                    0.9,
                ),
            ]
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
    liver_query = any(term in query for term in ("脂肪肝", "脂肪性肝炎", "代謝性脂肪肝", "肝硬化", "肝纖維")) or any(
        term in query_lower for term in ("masld", "mash", "nafld", "nash", "steatotic liver", "steatohepatitis", "cirrhosis")
    )
    if liver_query:
        variants.extend(
            [
                QueryVariant(
                    "coverage_liver_disease",
                    f"{query} MASLD metabolic dysfunction-associated steatotic liver disease NAFLD diabetes obesity weight loss lifestyle",
                    0.86,
                ),
                QueryVariant(
                    "coverage_liver_treatment",
                    f"{query} MASH metabolic dysfunction-associated steatohepatitis NASH GLP-1 receptor agonist pioglitazone tirzepatide fibrosis cirrhosis",
                    0.84,
                ),
            ]
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

    ranked_hits = sorted(hits, key=lambda hit: hit.score, reverse=True)
    preferred_source = preferred_source_from_query(query)
    if preferred_source:
        sorted_hits = sorted(
            ranked_hits,
            key=lambda hit: (preferred_source not in hit.source_label.lower(), -hit.score),
        )[: max(limit * 3, limit)]
    else:
        sorted_hits = source_balanced_hits(ranked_hits, max(limit * 3, limit))
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
            if preferred_source and preferred_source not in hit.source_label.lower():
                redundancy_penalty += 0.85
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


def preferred_source_from_query(query: str) -> str:
    lower = query.lower()
    if "kdigo" in lower:
        return "kdigo"
    if "aace" in lower:
        return "aace"
    if re.search(r"\bada\b|american diabetes association|dc26s", lower):
        return "ada"
    if "retinopathy" in query_concepts(query, lower):
        return "ada"
    return ""


def source_balanced_hits(hits: list[KnowledgeHit], limit: int) -> list[KnowledgeHit]:
    if len(hits) <= limit:
        return hits

    quota = max(1, int(os.getenv("LINE_KNOWLEDGE_SOURCE_MIN_CANDIDATES", "2")))
    selected: list[KnowledgeHit] = []
    selected_keys: set[tuple[str, ...]] = set()
    by_source: dict[str, list[KnowledgeHit]] = {}
    for hit in hits:
        by_source.setdefault(hit.source_label, []).append(hit)

    for source in sorted(by_source, key=lambda key: by_source[key][0].score, reverse=True):
        for hit in by_source[source][:quota]:
            key = hit_dedup_key(hit)
            if key not in selected_keys:
                selected.append(hit)
                selected_keys.add(key)
            if len(selected) >= limit:
                return sorted(selected, key=lambda item: item.score, reverse=True)

    for hit in hits:
        key = hit_dedup_key(hit)
        if key in selected_keys:
            continue
        selected.append(hit)
        selected_keys.add(key)
        if len(selected) >= limit:
            break

    return sorted(selected, key=lambda item: item.score, reverse=True)


def required_facets(query: str) -> set[str]:
    lower = query.lower()
    concepts = query_concepts(query, lower)
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
    if (
        any(term in query for term in ("連續血糖", "連續血糖監測", "新科技", "科技", "血糖機"))
        or any(term in lower for term in ("cgm", "continuous glucose", "diabetes technology"))
    ) and (
        any(term in query for term in ("適用", "適合", "哪些病人", "哪種病人", "誰可以", "使用對象"))
        or any(term in lower for term in ("indication", "recommended", "offered", "eligible", "who should"))
    ):
        facets.update({"monitoring", "technology_indication"})
    if any(term in query for term in ("藥", "用藥", "胰島素")) or any(
        term in lower for term in ("medication", "pharmacologic", "sglt", "glp", "insulin", "metformin", "glucocorticoid", "steroid")
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
    if "pad" in concepts:
        facets.update({"pad_context", "ascvd_context"})
    if "retinopathy" in concepts:
        facets.add("retinopathy_context")
    if "staging" in concepts:
        facets.add("staging")
    if any(term in query for term in ("多久", "幾次", "頻率", "一次", "每年")) or any(
        term in lower for term in ("frequency", "annually", "months", "yearly", "every")
    ):
        facets.add("frequency")
    if any(term in query for term in ("懷孕", "妊娠", "孕")) or any(term in lower for term in ("pregnancy", "gestational", "gdm")):
        facets.add("pregnancy")
    if any(term in query for term in ("脂肪肝", "脂肪性肝炎", "代謝性脂肪肝", "肝硬化", "肝纖維")) or any(
        term in lower for term in ("masld", "mash", "nafld", "nash", "steatotic liver", "steatohepatitis", "cirrhosis")
    ):
        facets.update({"liver_context", "treatment"})
    if "hospital_steroid_hyperglycemia" in concepts:
        facets.update({"hospital_context", "steroid_context", "treatment"})
    if "低血糖" in query or "hypoglycemia" in lower:
        facets.update({"hypoglycemia", "treatment"})
    if any(term in query for term in ("處理", "治療", "怎麼辦")) or any(term in lower for term in ("treatment", "management")):
        facets.add("treatment")
    return facets


def hit_facets(hit: KnowledgeHit) -> set[str]:
    haystack = (
        f"{hit.source} {hit.source_label} {hit.title} {hit.section} {hit.chunk_type} "
        f"{' '.join(hit.metadata)} {hit.excerpt} {hit.parent_excerpt[:900]}"
    ).lower()
    facets: set[str] = set()
    if re.search(r"\b(ckd|kidney|renal|egfr|albuminuria|uacr|dialysis|eskd|esrd)\b", haystack):
        facets.add("kidney_context")
    if re.search(r"\b(glycemic goal|glycemic target|glucose target|a1c goal|individualized goal|treatment goals)\b", haystack):
        facets.add("glycemic_target")
    if re.search(r"\b(a1c.*less reliable|less reliable.*a1c|glycated albumin|fructosamine|red blood cell turnover)\b", haystack):
        facets.add("a1c_reliability")
    if re.search(r"\b(cgm|bgm|smbg|glucose monitoring|time in range|tir|time below range|time above range)\b", haystack):
        facets.add("monitoring")
    if re.search(
        r"\b(use of cgm is recommended|recommended at diabetes onset|offered to people with diabetes|on insulin therapy|noninsulin therapies that can cause hypoglycemia|any diabetes treatment where cgm helps|periodic use of personal or professional cgm|individual circumstances preferences needs)\b",
        haystack,
    ):
        facets.add("technology_indication")
    if re.search(r"\b(sglt2|glp-1|insulin|metformin|finerenone|glucagon|pharmacologic|medication|dose|dosage)\b", haystack):
        facets.add("medication")
    if re.search(r"\b(mg/dl|mmol/l|ml/min|%|threshold|criteria|fasting|1 h|2 h|3 h|≥|<=|<|>)\b", haystack):
        facets.add("threshold")
    if re.search(r"\b(diagnosis|diagnostic|screening|ogtt|classification|criteria)\b", haystack):
        facets.add("diagnosis")
    if re.search(r"\b(retinopathy|retinal|macular edema|dme|npdr|pdr|ophthalmologist|anti-vegf|photocoagulation|vitrectomy)\b", haystack):
        facets.add("retinopathy_context")
    if re.search(r"\b(staging|stage|severity|classification|mild|moderate|severe|nonproliferative|proliferative|npdr|pdr|microaneurysms|neovascularization)\b", haystack):
        facets.add("staging")
    if re.search(r"\b(foot|neuropathy|monofilament|ulcer|protective sensation|peripheral artery|pad|lops|podiatrist)\b", haystack):
        facets.add("foot_care")
    if re.search(
        r"\b(peripheral artery disease|peripheral arterial disease|pad|lower-extremity|lower extremity|lower limb|claudication|limb ischemia|gangrene|amputation|abi|toe pressure|revascularization)\b",
        haystack,
    ):
        facets.add("pad_context")
    if re.search(r"\b(ascvd|cardiovascular disease|antiplatelet|aspirin|clopidogrel|rivaroxaban|statin|lipid|blood pressure|smoking cessation)\b", haystack):
        facets.add("ascvd_context")
    if re.search(r"\b(annually|every \d|months?|yearly|frequency|examination frequency|at least yearly)\b", haystack):
        facets.add("frequency")
    if re.search(r"\b(pregnancy|gestational|gdm|preconception|postpartum)\b", haystack):
        facets.add("pregnancy")
    if re.search(r"\b(hospital|inpatient|hospitalized|diabetes care in the hospital|critical illness|noncritical illness|dc26s016)\b|住院", haystack):
        facets.add("hospital_context")
    if re.search(r"\b(glucocorticoid|steroid-induced hyperglycemia|steroid.*hyperglycemia|corticosteroid|prednisone|prednisolone|dexamethasone|nph insulin)\b|類固醇", haystack):
        facets.add("steroid_context")
    if re.search(r"\b(masld|mash|nafld|nash|steatotic liver|steatohepatitis|fatty liver|cirrhosis|fibrosis|hepatic)\b", haystack):
        facets.add("liver_context")
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
    for entry in matched_keyword_entries(query):
        expanded.extend(entry.expansions)
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
    kidney_query = any(term in query for term in ("腎", "腎絲球", "腎病變", "腎衰竭", "尿蛋白", "白蛋白尿")) or any(
        term in query_lower for term in ("ckd", "kidney", "renal", "egfr", "uacr", "albuminuria", "proteinuria")
    )
    kidney_medication_query = kidney_query and (
        any(term in query for term in ("藥", "用藥", "降血糖", "合併")) or any(
            term in query_lower
            for term in ("medication", "pharmacologic", "sglt", "glp", "metformin", "finerenone", "insulin")
        )
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
    hyperglycemic_crisis_query = any(term in query for term in ("HHNK", "高滲透壓", "高血糖高滲透壓", "高血糖急症", "酮酸", "酮酸中毒")) or any(
        term in query_lower for term in ("hhnk", "hhs", "dka", "ketoacidosis", "hyperosmolar", "hyperglycemic crisis", "hyperglycemic crises")
    )
    steroid_hospital_query = "hospital_steroid_hyperglycemia" in query_concepts(query, query_lower)
    liver_query = any(term in query for term in ("肝", "脂肪肝", "脂肪性肝炎", "代謝性脂肪肝", "肝硬化", "肝纖維")) or any(
        term in query_lower for term in ("masld", "mash", "nafld", "nash", "steatotic liver", "steatohepatitis", "cirrhosis")
    )
    concepts = query_concepts(query, query_lower)
    retinopathy_query = "retinopathy" in concepts
    pad_query = "pad" in concepts
    staging_query = "staging" in concepts
    treatment_query = "treatment" in concepts
    technology_indication_query = (
        any(term in query for term in ("連續血糖", "連續血糖監測", "新科技", "科技", "血糖機"))
        or any(term in query_lower for term in ("cgm", "continuous glucose", "diabetes technology"))
    ) and (
        any(term in query for term in ("適用", "適合", "哪些病人", "哪種病人", "誰可以", "使用對象"))
        or any(term in query_lower for term in ("indication", "recommended", "offered", "eligible", "who should"))
    )
    vaccination_query = any(term in query for term in ("疫苗", "流感", "肺炎鏈球菌", "新冠", "帶狀皰疹")) or any(
        term in query_lower for term in ("vaccine", "vaccination", "immunization", "influenza", "pneumococcal", "covid", "hepatitis")
    )

    if chunk.chunk_type == "table_row":
        adjustment *= 1.25
    if chunk.chunk_type == "recommendation":
        adjustment *= 1.45
    if chunk.chunk_type == "section_summary":
        adjustment *= 1.08
    if re.search(r"\b(reference|references|acknowledg|appendix)\b", haystack):
        adjustment *= 0.35
    if not vaccination_query and re.search(
        r"\b(vaccin|immunization|influenza|pneumococcal|covid|hepatitis b|respiratory syncytial virus|rsv)\b",
        haystack,
    ):
        adjustment *= 0.18
    if hyperglycemic_crisis_query and re.search(r"\b(gdm|gestational diabetes|pregnancy)\b", haystack):
        adjustment *= 0.12
    if hyperglycemic_crisis_query and ("dc26s016" in haystack or "diabetes care in the hospital" in haystack):
        adjustment *= 5.0
    if hyperglycemic_crisis_query and ("dc26s006" in haystack or "hyperglycemic crises" in haystack):
        adjustment *= 2.4
    if hyperglycemic_crisis_query and re.search(
        r"\b(dka|diabetic ketoacidosis|hhs|hyperosmolar|hyperglycemic crises|intravenous fluids|insulin|electrolytes|potassium|osmolality|ketones|bicarbonate|anion gap|acidosis|subcutaneous insulin|precipitating cause)\b",
        haystack,
    ):
        adjustment *= 4.2
    if hyperglycemic_crisis_query and not steroid_hospital_query and re.search(
        r"\b(glucocorticoid|steroid-induced hyperglycemia|corticosteroid|prednisone|prednisolone|dexamethasone|nph insulin)\b",
        haystack,
    ):
        adjustment *= 0.25
    if steroid_hospital_query and ("dc26s016" in haystack or "diabetes care in the hospital" in haystack):
        adjustment *= 5.0
    if steroid_hospital_query and ("dc26s009" in haystack or "pharmacologic approaches to glycemic treatment" in haystack):
        adjustment *= 2.2
    if steroid_hospital_query and re.search(
        r"\b(glucocorticoid|steroid-induced hyperglycemia|steroid.*hyperglycemia|corticosteroid|prednisone|prednisolone|dexamethasone|nph insulin|basal insulin|prandial|correction insulin|point-of-care|poc blood glucose|frequent reassessment)\b",
        haystack,
    ):
        adjustment *= 4.2
    if steroid_hospital_query and "dc26s006" in haystack and not re.search(
        r"\b(glucocorticoid|steroid-induced hyperglycemia|corticosteroid|prednisone|prednisolone|dexamethasone|nph insulin)\b",
        haystack,
    ):
        adjustment *= 0.18
    if re.search(r"\b(recommendation|recommendations|treatment|therapy|selection|screening|diagnosis|pharmacologic|management|interventions)\b", haystack):
        adjustment *= 1.18
    if re.search(r"\b(egfr|albuminuria|uacr|mg/g|ml/min|contraindicat|avoid|dose|dosage|adjust|threshold|initiat|discontinu)\b", haystack):
        adjustment *= 1.18
    if "kdigo" in query_lower:
        adjustment *= 2.6 if "kdigo" in haystack else 0.58
    if "aace" in query_lower:
        adjustment *= 2.4 if "aace" in haystack else 0.65
    if re.search(r"\bada\b|american diabetes association|dc26s", query_lower):
        adjustment *= 2.2 if ("ada standards" in haystack or re.search(r"\bdc26s\d+\b", haystack)) else 0.72
    if kidney_query and "kdigo" in haystack:
        adjustment *= float(os.getenv("LINE_KNOWLEDGE_KDIGO_CKD_BOOST", "1.85"))
    if kidney_medication_query and "kdigo" in haystack:
        adjustment *= float(os.getenv("LINE_KNOWLEDGE_KDIGO_CKD_MEDICATION_BOOST", "1.35"))
    if kidney_medication_query and "aace" in haystack:
        adjustment *= float(os.getenv("LINE_KNOWLEDGE_AACE_MEDICATION_BOOST", "1.25"))
    if liver_query and re.search(r"\b(masld|mash|nafld|nash|steatotic liver|steatohepatitis|fatty liver|cirrhosis|fibrosis|hepatic)\b", haystack):
        adjustment *= 2.4
    if liver_query and re.search(r"\b(glp-1|pioglitazone|tirzepatide|weight loss|obesity|lifestyle)\b", haystack):
        adjustment *= 1.45
    if technology_indication_query and ("dc26s007" in haystack or "diabetes technology" in haystack):
        adjustment *= 3.2
    if technology_indication_query and re.search(
        r"\b(7\.15|use of cgm is recommended|recommended at diabetes onset|on insulin therapy|noninsulin therapies that can cause hypoglycemia|any diabetes treatment where cgm helps|periodic use of personal or professional cgm)\b",
        haystack,
    ):
        adjustment *= 2.6
    if technology_indication_query and re.search(r"\b(cgm metrics|table 6\.2|time in range|tar|tbr|tir)\b", haystack):
        adjustment *= 0.55
    if pad_query and ("dc26s010" in haystack or "cardiovascular disease and risk management" in haystack):
        adjustment *= 5.0
    if pad_query and ("dc26s012" in haystack or "foot care" in haystack):
        adjustment *= 3.2
    if pad_query and re.search(
        r"\b(peripheral artery disease|peripheral arterial disease|pad|lower-extremity|lower extremity|claudication|limb ischemia|gangrene|amputation|abi|toe pressure|revascularization)\b",
        haystack,
    ):
        adjustment *= 3.4
    if pad_query and treatment_query and re.search(
        r"\b(antiplatelet|aspirin|clopidogrel|rivaroxaban|statin|lipid-lowering|blood pressure|hypertension|smoking cessation|semaglutide|glp-1|stride|major adverse limb|limb outcomes)\b",
        haystack,
    ):
        adjustment *= 3.0
    if pad_query and ("dc26s009" in haystack or "pharmacologic approaches to glycemic treatment" in haystack) and not re.search(
        r"\b(peripheral artery disease|pad|lower-extremity|limb|amputation|semaglutide|glp-1|ascvd)\b",
        haystack,
    ):
        adjustment *= 0.18
    if retinopathy_query and ("dc26s012" in haystack or "retinopathy, neuropathy, and foot care" in haystack):
        adjustment *= 5.2
    elif retinopathy_query and ("retinopathy" in haystack or "macular edema" in haystack):
        adjustment *= 1.25
    elif retinopathy_query:
        adjustment *= 0.25
    if retinopathy_query and staging_query and re.search(
        r"\b(microaneurysms|nonproliferative|proliferative|npdr|pdr|diabetic macular edema|dme|neovascularization|severity|staging)\b",
        haystack,
    ):
        adjustment *= 2.2
    if retinopathy_query and treatment_query and re.search(
        r"\b(anti-vegf|vascular endothelial growth factor|panretinal laser|photocoagulation|vitrectomy|corticosteroid|focal/grid|emerging therapies|aflibercept|ranibizumab)\b",
        haystack,
    ):
        adjustment *= 2.4
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
