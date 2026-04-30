from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
from pathlib import Path
import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, Header, HTTPException, Request

try:
    from knowledge import (
        KnowledgeHit,
        knowledge_candidates_prompt,
        knowledge_no_answer_text,
        knowledge_prompt_from_hits,
        knowledge_status,
        hit_facets,
        query_variant_specs,
        required_facets,
        search_knowledge_candidates,
        search_whole_section_context,
    )
except ModuleNotFoundError:
    KnowledgeHit = Any

    def knowledge_no_answer_text() -> str:
        return (
            "目前糖尿病指南知識庫尚未正確載入，"
            "為了避免提供不準確的資訊，我先不回答這個問題。"
        )

    def search_knowledge_candidates(query: str) -> list[Any]:
        return []

    def search_whole_section_context(query: str, seed_hits: list[Any]) -> list[Any]:
        return []

    def query_variant_specs(query: str) -> list[Any]:
        return []

    def knowledge_candidates_prompt(hits: list[Any]) -> str:
        return "\n\n候選指南片段：目前部署環境沒有載入 knowledge.py。"

    def knowledge_prompt_from_hits(hits: list[Any]) -> str:
        return (
            "\n\n背景知識檢索：目前部署環境沒有載入 knowledge.py，"
            "請不要使用模型內建知識回答。"
        )

    def knowledge_status() -> dict[str, object]:
        return {
            "enabled": False,
            "available": False,
            "error": "knowledge.py not found in deployment",
        }

    def required_facets(query: str) -> set[str]:
        return set()

    def hit_facets(hit: Any) -> set[str]:
        return set()


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
DEEPSEEK_API_BASE = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com").rstrip("/")
APP_VERSION = os.getenv("APP_VERSION", "2026-05-01-dense-ontology-v21")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
DEEPSEEK_REASONING_EFFORT = os.getenv("DEEPSEEK_REASONING_EFFORT", "high")
DEEPSEEK_THINKING_ENABLED = os.getenv("DEEPSEEK_THINKING_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "20"))
LINE_QUERY_PLANNING_ENABLED = os.getenv("LINE_QUERY_PLANNING_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_EVIDENCE_REVIEW_ENABLED = os.getenv("LINE_EVIDENCE_REVIEW_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_LLM_RERANK_ENABLED = os.getenv("LINE_LLM_RERANK_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_LLM_RERANK_TOP_K = int(os.getenv("LINE_LLM_RERANK_TOP_K", "5"))
LINE_RECURSIVE_COVERAGE_ENABLED = os.getenv("LINE_RECURSIVE_COVERAGE_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_RECURSIVE_COVERAGE_MAX_QUERIES = int(os.getenv("LINE_RECURSIVE_COVERAGE_MAX_QUERIES", "4"))
LINE_RECURSIVE_COVERAGE_MAX_HITS = int(os.getenv("LINE_RECURSIVE_COVERAGE_MAX_HITS", "4"))
LINE_LONG_CONTEXT_VERIFICATION_ENABLED = os.getenv("LINE_LONG_CONTEXT_VERIFICATION_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_WHOLE_SECTION_CONTEXT_ENABLED = os.getenv("LINE_WHOLE_SECTION_CONTEXT_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_DEBUG_SEARCH_ENABLED = os.getenv("LINE_DEBUG_SEARCH_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LINE_DEBUG_SEARCH_MAX_HITS = int(os.getenv("LINE_DEBUG_SEARCH_MAX_HITS", "12"))
LINE_RETRIEVAL_QUERY_MAX_CHARS = int(os.getenv("LINE_RETRIEVAL_QUERY_MAX_CHARS", "1400"))
LINE_TIMEOUT = int(os.getenv("LINE_TIMEOUT", "12"))
LINE_MEMORY_ENABLED = os.getenv("LINE_MEMORY_ENABLED", "1").strip() != "0"
LINE_MEMORY_DB = os.getenv("LINE_MEMORY_DB", "/tmp/line_lifebot_memory.sqlite3")
LINE_CONTEXT_ENABLED = os.getenv("LINE_CONTEXT_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
LINE_CONTEXT_MAX_MESSAGES = int(os.getenv("LINE_CONTEXT_MAX_MESSAGES", "8"))
LINE_CONTEXT_TTL_SECONDS = int(os.getenv("LINE_CONTEXT_TTL_SECONDS", "43200"))
LINE_SESSION_SCOPE = os.getenv("LINE_SESSION_SCOPE", "user").strip().lower()

app = FastAPI(title="LifeBot Fast LINE QA")

_memory_ready = False
_memory_lock = threading.Lock()
_session_locks: dict[str, threading.Lock] = {}
_session_locks_guard = threading.Lock()


SYSTEM_PROMPT = """你是 LifeBot 糖尿病衛教 LINE 機器人，請用繁體中文回答病友問題。

回答規則：
- 口吻溫和、清楚、像衛教師在 LINE 上簡短回覆。
- 不要使用 Markdown 格式，不要使用井字號、星號或程式碼區塊。
- 不要提供個人化診斷、處方、劑量調整、停藥建議，或替代醫師判斷。
- 回答以 2 到 4 個短段落為主，適合手機閱讀。
- 只能根據「背景知識檢索」提供的已載入臨床指南片段回答。
- 不要使用模型內建知識、一般醫學常識、未載入指南、新聞或推測補完。
- 若指南片段沒有直接依據，請明確說目前指南知識庫資料不足，並停止回答。
- 回答中請自然標示依據來源，例如 ADA 2026、KDIGO、AACE 或片段中出現的指南名稱；不要編造未出現在片段中的來源。
- 若問題涉及低血糖、高血糖急症、胸痛、意識不清、酮酸中毒疑慮、懷孕、兒童、腎功能、嚴重感染或傷口惡化，請提醒盡快聯絡醫療團隊或就醫。
- 回覆結尾不要提出追問；即使資訊不足，也請用一般提醒收束，不要問「請問您目前...？」。
- 不要編造最新研究、新聞或來源；若病友要求最新醫學期刊或新聞，請說明需要啟用搜尋流程，並先給一般衛教原則。
- 若有提供「最近對話脈絡」，請用它理解代名詞、接續問題與前後文；但不要把短期對話內容當作永久病歷或已驗證診斷。
"""


def memory_database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip() or os.getenv("POSTGRES_URL", "").strip()


def memory_backend() -> str:
    url = memory_database_url()
    if urlparse(url).scheme in {"postgres", "postgresql"}:
        return "postgres"
    return "sqlite"


@contextmanager
def memory_connection():
    if memory_backend() == "postgres":
        import psycopg

        with psycopg.connect(memory_database_url()) as conn:
            yield conn
        return

    path = Path(LINE_MEMORY_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_memory_db() -> None:
    global _memory_ready
    if not (LINE_MEMORY_ENABLED or LINE_CONTEXT_ENABLED) or _memory_ready:
        return
    with _memory_lock:
        if _memory_ready:
            return
        with memory_connection() as conn:
            if memory_backend() == "postgres":
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS line_user_memory (
                        line_user_id TEXT PRIMARY KEY,
                        display_name TEXT,
                        profile_summary TEXT,
                        consent_memory BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS line_conversation_context (
                        line_user_id TEXT PRIMARY KEY,
                        turns_json TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS line_user_memory (
                        line_user_id TEXT PRIMARY KEY,
                        display_name TEXT,
                        profile_summary TEXT,
                        consent_memory INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS line_conversation_context (
                        line_user_id TEXT PRIMARY KEY,
                        turns_json TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
        _memory_ready = True


def fetch_user_memory(line_user_id: str) -> dict[str, Any] | None:
    if not LINE_MEMORY_ENABLED or not line_user_id:
        return None
    ensure_memory_db()
    with memory_connection() as conn:
        row = conn.execute(
            "SELECT line_user_id, display_name, profile_summary, consent_memory, updated_at "
            "FROM line_user_memory WHERE line_user_id = %s"
            if memory_backend() == "postgres"
            else "SELECT line_user_id, display_name, profile_summary, consent_memory, updated_at "
            "FROM line_user_memory WHERE line_user_id = ?",
            (line_user_id,),
        ).fetchone()
    if not row:
        return None
    if hasattr(row, "keys"):
        return dict(row)
    keys = ["line_user_id", "display_name", "profile_summary", "consent_memory", "updated_at"]
    return dict(zip(keys, row))


def delete_user_memory(line_user_id: str) -> None:
    if not LINE_MEMORY_ENABLED or not line_user_id:
        return
    ensure_memory_db()
    with memory_connection() as conn:
        conn.execute(
            "DELETE FROM line_user_memory WHERE line_user_id = %s"
            if memory_backend() == "postgres"
            else "DELETE FROM line_user_memory WHERE line_user_id = ?",
            (line_user_id,),
        )


def fetch_conversation_turns(line_user_id: str) -> list[dict[str, Any]]:
    if not LINE_CONTEXT_ENABLED or not line_user_id:
        return []
    ensure_memory_db()
    with memory_connection() as conn:
        row = conn.execute(
            "SELECT turns_json FROM line_conversation_context WHERE line_user_id = %s"
            if memory_backend() == "postgres"
            else "SELECT turns_json FROM line_conversation_context WHERE line_user_id = ?",
            (line_user_id,),
        ).fetchone()
    if not row:
        return []
    raw_json = row[0] if not hasattr(row, "keys") else row["turns_json"]
    try:
        turns = json.loads(raw_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(turns, list):
        return []

    cutoff = current_epoch_seconds() - LINE_CONTEXT_TTL_SECONDS
    fresh_turns = [
        turn
        for turn in turns
        if isinstance(turn, dict) and int(turn.get("ts") or 0) >= cutoff and turn.get("role") in {"user", "assistant"}
    ]
    return fresh_turns[-LINE_CONTEXT_MAX_MESSAGES:]


def save_conversation_turn(line_user_id: str, user_text: str, assistant_text: str) -> None:
    if not LINE_CONTEXT_ENABLED or not line_user_id:
        return
    ensure_memory_db()
    now = current_epoch_seconds()
    turns = fetch_conversation_turns(line_user_id)
    turns.extend(
        [
            {"role": "user", "text": clean_context_text(user_text), "ts": now},
            {"role": "assistant", "text": clean_context_text(assistant_text), "ts": now},
        ]
    )
    turns = [turn for turn in turns if turn.get("text")][-LINE_CONTEXT_MAX_MESSAGES:]
    turns_json = json.dumps(turns, ensure_ascii=False)
    with memory_connection() as conn:
        if memory_backend() == "postgres":
            conn.execute(
                """
                INSERT INTO line_conversation_context (line_user_id, turns_json, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (line_user_id) DO UPDATE SET
                    turns_json = EXCLUDED.turns_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (line_user_id, turns_json),
            )
        else:
            conn.execute(
                """
                INSERT INTO line_conversation_context (line_user_id, turns_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(line_user_id) DO UPDATE SET
                    turns_json = excluded.turns_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (line_user_id, turns_json),
            )


def delete_conversation_context(line_user_id: str) -> None:
    if not LINE_CONTEXT_ENABLED or not line_user_id:
        return
    ensure_memory_db()
    with memory_connection() as conn:
        conn.execute(
            "DELETE FROM line_conversation_context WHERE line_user_id = %s"
            if memory_backend() == "postgres"
            else "DELETE FROM line_conversation_context WHERE line_user_id = ?",
            (line_user_id,),
        )


def conversation_prompt(line_user_id: str) -> str:
    turns = fetch_conversation_turns(line_user_id)
    if not turns:
        return ""
    labels = {"user": "使用者", "assistant": "LifeBot"}
    lines = [
        "\n\n最近對話脈絡：",
        "以下是同一個 LINE 對話中尚未過期的最近訊息，只用來理解前後文、代名詞與接續問題。",
        "若最近脈絡和本次問題衝突，請以本次問題為主；不要主動揭露這段脈絡存在。",
    ]
    for turn in turns:
        text = clean_context_text(str(turn.get("text") or ""))
        if text:
            lines.append(f"- {labels.get(str(turn.get('role')), '訊息')}：{text}")
    return "\n".join(lines)


def current_epoch_seconds() -> int:
    import time

    return int(time.time())


def clean_context_text(value: str) -> str:
    value = re.sub(r"\s+", " ", value).strip()
    return value[:700]


def clean_memory_text(value: str) -> str:
    value = re.split(r"\b(?:and|with|but|because|who|that|having|taking|using|on)\b", value, maxsplit=1, flags=re.I)[0]
    value = re.split(r"(?:，|。|、|；|;|,|！|!|？|\?|目前|正在|有|患有|罹患|得了|想問|請問)", value, maxsplit=1)[0]
    value = re.sub(r"\s+", " ", value).strip(" ，。,.！!？?")
    return value[:20]


def save_user_name(line_user_id: str, display_name: str) -> None:
    if not LINE_MEMORY_ENABLED or not line_user_id:
        return
    ensure_memory_db()
    display_name = clean_memory_text(display_name)
    if not display_name:
        return
    with memory_connection() as conn:
        if memory_backend() == "postgres":
            conn.execute(
                """
                INSERT INTO line_user_memory (line_user_id, display_name, profile_summary, consent_memory, updated_at)
                VALUES (%s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (line_user_id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    profile_summary = NULL,
                    consent_memory = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (line_user_id, display_name, None),
            )
        else:
            conn.execute(
                """
                INSERT INTO line_user_memory (line_user_id, display_name, profile_summary, consent_memory, updated_at)
                VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                ON CONFLICT(line_user_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    profile_summary = NULL,
                    consent_memory = 1,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (line_user_id, display_name, None),
            )


def memory_prompt(line_user_id: str) -> str:
    memory = fetch_user_memory(line_user_id)
    if not memory or not memory.get("consent_memory"):
        return ""
    parts = []
    if memory.get("display_name"):
        parts.append(f"稱呼：{memory['display_name']}")
    if not parts:
        return ""
    return "\n\n使用者已明確同意保存的稱呼：\n" + "\n".join(f"- {part}" for part in parts) + (
        "\n回答時可以自然用這個稱呼；不要主動揭露 LINE userId，也不要假裝知道未保存的個資。"
    )


def extract_display_name(text: str) -> str | None:
    patterns = [
        r"(?:請|幫我)?記住(?:我)?(?:叫|名字是|姓名是)\s*([^\s，。,.！!？?]{1,20})",
        r"(?:我叫|我的名字是|我的姓名是)\s*([^\s，。,.！!？?]{1,20})",
        r"我是\s*([^\s，。,.！!？?]{1,20})",
        r"(?:以後|下次)(?:請)?(?:叫我|稱呼我)\s*([^\s，。,.！!？?]{1,20})",
        r"你可以叫我\s*([^\s，。,.！!？?]{1,20})",
        r"(?i)\bmy name is\s+([A-Za-z][A-Za-z .'-]{0,38})",
        r"(?i)\bcall me\s+([A-Za-z][A-Za-z .'-]{0,38})",
        r"(?i)\byou can call me\s+([A-Za-z][A-Za-z .'-]{0,38})",
        r"(?i)\bi am\s+([A-Za-z][A-Za-z .'-]{0,38})",
        r"(?i)\bi'm\s+([A-Za-z][A-Za-z .'-]{0,38})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            name = clean_memory_text(match.group(1))
            if is_plausible_display_name(name):
                return name
    return None


def is_plausible_display_name(value: str) -> bool:
    if not value:
        return False
    lower = value.lower().strip()
    rejected = {
        "diabetic",
        "a diabetic",
        "pregnant",
        "sick",
        "hungry",
        "low",
        "high",
        "type 1",
        "type 2",
        "type one",
        "type two",
        "糖尿病",
        "糖尿病患者",
        "病人",
        "患者",
        "孕婦",
        "懷孕",
        "低血糖",
        "高血糖",
        "第一型糖尿病",
        "第二型糖尿病",
    }
    if lower in rejected or lower.startswith(("having ", "taking ", "using ", "on ")):
        return False
    if any(term in value for term in ("糖尿病", "血糖", "懷孕", "症狀", "用藥", "藥物", "胰島素")):
        return False
    return True


def memory_command_response(line_user_id: str, user_text: str) -> str | None:
    if is_context_reset_command(user_text):
        delete_conversation_context(line_user_id)
        return "好的，這段對話脈絡已清除。接下來我會把下一則訊息當作新的問題開始。"

    display_name = extract_display_name(user_text)
    if not LINE_MEMORY_ENABLED:
        if display_name:
            return "目前名字記憶功能尚未啟用，所以我暫時無法保存你的稱呼。"
        return None

    if re.search(r"(忘記|刪除|清除).*(我的)?(名字|稱呼|資料|記憶)|不要再記住我", user_text):
        delete_user_memory(line_user_id)
        delete_conversation_context(line_user_id)
        return "我已經刪除目前為你保存的稱呼。之後如果你希望我再記住名字，可以再告訴我。"

    if re.search(r"(你記得我什麼|我有哪些資料被記住|查詢.*(名字|稱呼|資料|記憶)|看.*我的記憶|你記得我的名字嗎)", user_text):
        memory = fetch_user_memory(line_user_id)
        if not memory or not memory.get("consent_memory") or not memory.get("display_name"):
            return "目前我還沒有記住你的名字。你可以說：我叫小明。"
        return f"我目前記得你的稱呼是：{memory['display_name']}。"

    if not display_name:
        return None

    save_user_name(line_user_id, display_name)
    return f"好的，我記住了。之後我會用「{display_name}」來稱呼你。你也可以隨時說「忘記我的名字」來刪除。"


def is_context_reset_command(user_text: str) -> bool:
    return bool(re.search(r"(忘記|刪除|清除).*(這段|剛剛|對話|上下文|脈絡|context)|重新開始|從頭開始", user_text, re.I))


def verify_line_signature(body: bytes, signature: str) -> None:
    secret = os.getenv("LINE_CHANNEL_SECRET", "").strip()
    if not secret:
        raise HTTPException(status_code=500, detail="LINE_CHANNEL_SECRET is not configured")
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    if not hmac.compare_digest(expected, signature or ""):
        raise HTTPException(status_code=401, detail="invalid LINE signature")


def line_send(endpoint: str, payload: dict[str, Any]) -> tuple[bool, str]:
    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
    if not token:
        return False, "LINE_CHANNEL_ACCESS_TOKEN is not configured"
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=LINE_TIMEOUT) as response:
            return response.status < 300, f"LINE HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, f"LINE HTTP {exc.code}: {body[:300]}"
    except urllib.error.URLError as exc:
        return False, f"LINE request failed: {exc}"


def line_reply_text(reply_token: str, text: str) -> tuple[bool, str]:
    return line_send(
        "https://api.line.me/v2/bot/message/reply",
        {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900]}]},
    )


def line_push_text(to: str, text: str) -> tuple[bool, str]:
    return line_send(
        "https://api.line.me/v2/bot/message/push",
        {"to": to, "messages": [{"type": "text", "text": text[:4900]}]},
    )


def source_target(event: dict[str, Any]) -> str:
    source = event.get("source", {})
    return source.get("groupId") or source.get("roomId") or source.get("userId") or ""


def source_session_key(event: dict[str, Any]) -> str:
    source = event.get("source", {})
    user_id = source.get("userId") or ""
    group_id = source.get("groupId") or ""
    room_id = source.get("roomId") or ""
    chat_id = group_id or room_id or user_id

    if LINE_SESSION_SCOPE == "chat":
        return f"chat:{chat_id}" if chat_id else ""
    if LINE_SESSION_SCOPE == "chat_user":
        if chat_id and user_id and chat_id != user_id:
            return f"chat_user:{chat_id}:{user_id}"
        return user_id or chat_id

    return user_id or chat_id


def session_lock(session_key: str) -> threading.Lock:
    with _session_locks_guard:
        lock = _session_locks.get(session_key)
        if lock is None:
            lock = threading.Lock()
            _session_locks[session_key] = lock
        return lock


def answer_for_session(session_key: str, user_text: str) -> str:
    with session_lock(session_key):
        memory_answer = memory_command_response(session_key, user_text)
        answer = memory_answer or llm_answer(user_text, session_key)
        if not is_context_reset_command(user_text):
            save_conversation_turn(session_key, user_text, answer)
        return answer


def extract_gemini_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for candidate in payload.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            if part.get("text"):
                parts.append(str(part["text"]))
    return "\n".join(parts).strip()


def active_model() -> str:
    if LLM_PROVIDER == "deepseek":
        return DEEPSEEK_MODEL
    return GEMINI_MODEL


def active_api_key() -> str:
    if LLM_PROVIDER == "deepseek":
        return os.getenv("DEEPSEEK_API_KEY", "").strip()
    return os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()


def llm_configured() -> bool:
    return bool(active_api_key())


def call_gemini(
    api_key: str,
    system_text: str,
    user_text: str,
    max_output_tokens: int = 650,
    temperature: float = 0.4,
    timeout: int | None = None,
) -> str:
    body = {
        "system_instruction": {"parts": [{"text": system_text}]},
        "contents": [{"role": "user", "parts": [{"text": user_text}]}],
        "generationConfig": {
            "maxOutputTokens": max_output_tokens,
            "temperature": temperature,
        },
    }
    target_url = f"{GEMINI_API_BASE}/{GEMINI_MODEL}:generateContent"
    request = urllib.request.Request(
        target_url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "x-goog-api-key": api_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout or GEMINI_TIMEOUT) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    return extract_gemini_text(payload)


def extract_deepseek_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for choice in payload.get("choices", []):
        message = choice.get("message") or {}
        content = message.get("content")
        if content:
            parts.append(str(content))
    return "\n".join(parts).strip()


def call_deepseek(
    api_key: str,
    system_text: str,
    user_text: str,
    max_output_tokens: int = 650,
    temperature: float = 0.4,
    timeout: int | None = None,
) -> str:
    body: dict[str, Any] = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
        "max_tokens": max_output_tokens,
        "temperature": temperature,
        "stream": False,
    }
    if DEEPSEEK_THINKING_ENABLED:
        body["thinking"] = {"type": "enabled"}
        if DEEPSEEK_REASONING_EFFORT:
            body["reasoning_effort"] = DEEPSEEK_REASONING_EFFORT
    else:
        body["thinking"] = {"type": "disabled"}

    request = urllib.request.Request(
        f"{DEEPSEEK_API_BASE}/chat/completions",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout or GEMINI_TIMEOUT) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    return extract_deepseek_text(payload)


def call_llm(
    api_key: str,
    system_text: str,
    user_text: str,
    max_output_tokens: int = 650,
    temperature: float = 0.4,
    timeout: int | None = None,
) -> str:
    if LLM_PROVIDER == "deepseek":
        return call_deepseek(api_key, system_text, user_text, max_output_tokens, temperature, timeout)
    return call_gemini(api_key, system_text, user_text, max_output_tokens, temperature, timeout)


def extract_json_object(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, flags=re.S)
    if not match:
        return {}
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def clinical_intent_text(clinical_intent: dict[str, Any] | None) -> str:
    if not clinical_intent:
        return ""
    parts = [
        str(clinical_intent.get("clinical_intent") or "").strip(),
        str(clinical_intent.get("question_type") or "").strip(),
        str(clinical_intent.get("answer_strategy") or "").strip(),
        *json_list(clinical_intent.get("patient_context")),
        *json_list(clinical_intent.get("must_retrieve")),
        *json_list(clinical_intent.get("required_facets")),
    ]
    return " ".join(part for part in parts if part).strip()


def clinical_intent_prompt(clinical_intent: dict[str, Any] | None) -> str:
    if not clinical_intent:
        return ""
    return (
        "\n\n臨床問題理解：\n"
        "以下 JSON 是本輪回答前對使用者問題的臨床意圖拆解，用來定義要檢索與整理哪些證據；"
        "它不是醫療知識來源，最終回答仍只能根據指南片段。\n"
        f"{json.dumps(clinical_intent, ensure_ascii=False)}"
    )


def fallback_clinical_intent(user_text: str) -> dict[str, Any]:
    if comparative_threshold_question(user_text):
        return {
            "clinical_intent": "advanced_ckd_medication_selection",
            "question_type": "medication_threshold_comparison",
            "patient_context": ["diabetes", "advanced CKD", "low eGFR", user_text],
            "must_retrieve": [
                "CKD glucose-lowering therapy",
                "SGLT2 inhibitor eGFR initiation threshold",
                "metformin eGFR contraindication or dose limitation",
                "GLP-1 receptor agonist use in CKD",
                "finerenone or nonsteroidal MRA eGFR threshold",
                "hypoglycemia risk in advanced CKD",
                "insulin safety or dose adjustment in kidney impairment",
            ],
            "required_facets": ["kidney_context", "medication", "threshold"],
            "answer_strategy": (
                "compare retrieved eGFR thresholds with the user's eGFR value; state what is not suitable "
                "to initiate, what may be considered from the retrieved guideline snippets, and what requires clinician individualization"
            ),
            "do_not_answer_with": [
                "requiring an exact sentence for the exact eGFR number",
                "personalized dose recommendation",
                "model general medical knowledge",
            ],
        }
    facets = sorted(required_facets(user_text))
    return {
        "clinical_intent": "diabetes_guideline_question",
        "question_type": "guideline_grounded_answer",
        "patient_context": [user_text],
        "must_retrieve": [],
        "required_facets": facets,
        "answer_strategy": "answer only if retrieved guideline snippets cover the user's core question",
        "do_not_answer_with": ["model general medical knowledge", "unsupported inference"],
    }


def build_clinical_intent(api_key: str, user_text: str, recent_context: str) -> dict[str, Any]:
    fallback = fallback_clinical_intent(user_text)
    if not LINE_QUERY_PLANNING_ENABLED or not api_key:
        return fallback

    system_text = (
        "你是糖尿病指南問答的 clinical intent parser，不是回答者。"
        "你的任務是先理解使用者真正要問的臨床問題，定義需要檢索哪些指南證據。"
        "不要提供醫療建議，不要回答問題，不要使用模型內建醫學知識下結論。"
        "請輸出 JSON，欄位固定為："
        "clinical_intent, question_type, patient_context, must_retrieve, required_facets, answer_strategy, do_not_answer_with。"
        "required_facets 只能使用這些值：kidney_context, medication, threshold, glycemic_target, a1c_reliability, monitoring, technology_indication, diagnosis, retinopathy_context, staging, pregnancy, hypoglycemia, treatment, foot_care, frequency, liver_context。"
        "若問題是特定 eGFR 數值下的用藥/合併用藥，question_type 必須是 medication_threshold_comparison，"
        "must_retrieve 要包含 SGLT2 eGFR threshold、metformin eGFR limitation、GLP-1 RA in CKD、finerenone/nsMRA eGFR threshold、advanced CKD hypoglycemia/insulin safety。"
        "answer_strategy 要明確說明：用檢索到的 eGFR 門檻與使用者 eGFR 數值比較，不需要文件逐字出現 exact eGFR 數字。"
    )
    prompt = (
        f"本次問題：{user_text}\n\n"
        f"{recent_context or '最近對話脈絡：無'}\n\n"
        "請先輸出臨床意圖 JSON。"
    )
    try:
        raw = call_llm(
            api_key,
            system_text,
            prompt,
            max_output_tokens=520,
            temperature=0.05,
            timeout=min(GEMINI_TIMEOUT, 10),
        )
    except Exception as exc:
        print(f"{LLM_PROVIDER} clinical intent failed: {type(exc).__name__}: {exc}")
        return fallback

    data = extract_json_object(raw)
    if not data:
        return fallback
    merged = {**fallback, **data}
    for key in ("patient_context", "must_retrieve", "required_facets", "do_not_answer_with"):
        merged[key] = json_list(merged.get(key))
    return merged


def build_retrieval_query(
    api_key: str,
    user_text: str,
    recent_context: str,
    clinical_intent: dict[str, Any] | None = None,
) -> str:
    if not LINE_QUERY_PLANNING_ENABLED or not api_key:
        return " ".join(part for part in [user_text, clinical_intent_text(clinical_intent)] if part).strip() or user_text

    system_text = (
        "你不是回答者，也不要提供醫療建議。"
        "你的唯一任務是把 LINE 病友問題轉成已載入臨床指南文件檢索查詢。"
        "請根據本次問題與最近對話脈絡，補上可能出現在這些指南文件中的英文術語、縮寫、同義詞與章節詞。"
        "你會收到 clinical intent JSON；請優先根據 must_retrieve、required_facets、answer_strategy 產生多面向檢索詞。"
        "若問題提到洗腎/透析/腎衰竭與血糖控制目標，請加入 dialysis、kidney failure、glycemic goals、A1C goal、A1C reliability、CGM、BGM、glycated albumin、fructosamine。"
        "若 question_type 是 medication_threshold_comparison，請加入 CKD glucose-lowering therapy、SGLT2 inhibitor eGFR threshold、metformin eGFR、GLP-1 RA CKD、finerenone nonsteroidal MRA eGFR、hypoglycemia risk advanced CKD、insulin kidney impairment。"
        "若問題提到脂肪肝、脂肪性肝炎、MASLD、MASH、NAFLD、NASH、肝硬化或肝纖維化，請加入 MASLD、MASH、NAFLD、NASH、steatotic liver disease、steatohepatitis、fibrosis、cirrhosis、GLP-1 receptor agonist、pioglitazone、tirzepatide、weight loss。"
        "不要新增使用者沒有問到的病情、診斷、用藥劑量或結論。"
        "只輸出 JSON，格式為：{\"search_query\":\"...\",\"keywords\":[\"...\"]}。"
    )
    prompt = (
        f"本次問題：{user_text}\n\n"
        f"{recent_context or '最近對話脈絡：無'}\n\n"
        f"{clinical_intent_prompt(clinical_intent) or '臨床問題理解：無'}\n\n"
        "請產生適合全文檢索糖尿病指南 Markdown 的查詢。"
    )
    try:
        raw = call_llm(
            api_key,
            system_text,
            prompt,
            max_output_tokens=260,
            temperature=0.1,
            timeout=min(GEMINI_TIMEOUT, 10),
        )
    except Exception as exc:
        print(f"{LLM_PROVIDER} query planning failed: {type(exc).__name__}: {exc}")
        return user_text

    data = extract_json_object(raw)
    search_query = str(data.get("search_query") or "").strip()
    keywords = data.get("keywords") if isinstance(data.get("keywords"), list) else []
    keyword_text = " ".join(str(keyword).strip() for keyword in keywords if str(keyword).strip())
    combined = " ".join(part for part in [user_text, clinical_intent_text(clinical_intent), search_query, keyword_text] if part).strip()
    return combined[:LINE_RETRIEVAL_QUERY_MAX_CHARS] or user_text


def local_evidence_coverage(
    user_text: str,
    hits: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    required = set(required_facets(user_text))
    required.update(required_facets(clinical_intent_text(clinical_intent)))
    required.update(
        facet
        for facet in json_list((clinical_intent or {}).get("required_facets"))
        if facet
        in {
            "kidney_context",
            "medication",
            "threshold",
            "glycemic_target",
            "a1c_reliability",
            "monitoring",
            "technology_indication",
            "diagnosis",
            "retinopathy_context",
            "staging",
            "pregnancy",
            "hypoglycemia",
            "treatment",
            "foot_care",
            "frequency",
            "liver_context",
        }
    )
    if not required:
        return bool(hits), ""

    covered: set[str] = set()
    for hit in hits:
        covered.update(hit_facets(hit))

    missing = required - covered
    if not missing:
        return True, ""

    return False, "本地檢索仍缺少必要面向：" + ", ".join(sorted(missing))


def hit_identity(hit: KnowledgeHit) -> tuple[str, str, str, str]:
    return (
        str(getattr(hit, "source", "")),
        str(getattr(hit, "section", "")),
        str(getattr(hit, "chunk_type", "")),
        hashlib.sha1(str(getattr(hit, "excerpt", "")).encode("utf-8", errors="ignore")).hexdigest()[:12],
    )


def recursive_coverage_queries(
    user_text: str,
    hits: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
) -> list[str]:
    if not LINE_RECURSIVE_COVERAGE_ENABLED:
        return []

    required = set(required_facets(user_text))
    required.update(required_facets(clinical_intent_text(clinical_intent)))
    required.update(json_list((clinical_intent or {}).get("required_facets")))

    covered: set[str] = set()
    for hit in hits:
        covered.update(hit_facets(hit))
    missing = required - covered

    lower = f"{user_text} {clinical_intent_text(clinical_intent)}".lower()
    queries: list[str] = []
    kidney = any(term in user_text for term in ("腎", "腎絲球", "尿蛋白", "白蛋白尿")) or any(
        term in lower for term in ("ckd", "kidney", "renal", "egfr", "uacr", "albuminuria")
    )
    medication = any(term in user_text for term in ("藥", "用藥", "降血糖", "合併")) or any(
        term in lower for term in ("medication", "pharmacologic", "sglt", "glp", "metformin", "insulin", "finerenone")
    )
    liver = any(term in user_text for term in ("脂肪肝", "脂肪性肝炎", "代謝性脂肪肝", "肝硬化", "肝纖維")) or any(
        term in lower for term in ("masld", "mash", "nafld", "nash", "steatotic liver", "steatohepatitis", "cirrhosis")
    )

    facet_queries = {
        "kidney_context": f"{user_text} CKD chronic kidney disease eGFR albuminuria UACR KDIGO ADA AACE",
        "medication": f"{user_text} pharmacologic therapy medication selection SGLT2 GLP-1 metformin insulin finerenone",
        "threshold": f"{user_text} eGFR threshold cutoff contraindication initiate discontinue dose adjustment",
        "glycemic_target": f"{user_text} glycemic goals A1C goal individualized target hypoglycemia risk",
        "a1c_reliability": f"{user_text} A1C less reliable advanced CKD dialysis glycated albumin fructosamine CGM BGM",
        "monitoring": f"{user_text} monitoring CGM BGM SMBG time in range follow-up",
        "technology_indication": f"{user_text} ADA section 7 diabetes technology use of CGM recommended diabetes onset children adolescents adults insulin therapy noninsulin therapies hypoglycemia any diabetes treatment where CGM helps management",
        "diagnosis": f"{user_text} diagnosis screening diagnostic criteria A1C fasting plasma glucose OGTT",
        "retinopathy_context": f"{user_text} ADA section 12 diabetic retinopathy retinal disease ophthalmologist macular edema DME NPDR PDR",
        "staging": f"{user_text} staging severity classification mild moderate severe nonproliferative proliferative NPDR PDR diabetic macular edema DME",
        "pregnancy": f"{user_text} pregnancy gestational diabetes preconception postpartum insulin glycemic goals",
        "hypoglycemia": f"{user_text} hypoglycemia level 1 level 2 level 3 treatment glucagon severe hypoglycemia",
        "treatment": f"{user_text} treatment management recommendation therapy intervention",
        "foot_care": f"{user_text} foot care neuropathy monofilament ulcer peripheral artery disease screening",
        "frequency": f"{user_text} screening frequency annually every year follow-up interval",
        "liver_context": f"{user_text} MASLD MASH NAFLD NASH steatotic liver disease diabetes obesity fibrosis cirrhosis",
    }
    for facet in sorted(missing):
        query = facet_queries.get(facet)
        if query:
            queries.append(query)

    if kidney and medication:
        queries.extend(
            [
                f"{user_text} SGLT2 inhibitor eGFR initiation threshold continuation DKA acute illness perioperative hold KDIGO",
                f"{user_text} metformin eGFR renal function contraindication contrast acute illness perioperative older adults",
                f"{user_text} GLP-1 receptor agonist CKD ASCVD weight hypoglycemia kidney cardiovascular benefit",
                f"{user_text} finerenone nonsteroidal MRA UACR albuminuria eGFR potassium hyperkalemia",
            ]
        )
    if liver:
        queries.extend(
            [
                f"{user_text} MASLD NAFLD metabolic dysfunction-associated steatotic liver disease diabetes obesity weight loss",
                f"{user_text} MASH NASH steatohepatitis GLP-1 receptor agonist pioglitazone tirzepatide cirrhosis fibrosis",
            ]
        )

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        compact = re.sub(r"\s+", " ", query).strip()
        key = compact.lower()
        if compact and key not in seen:
            seen.add(key)
            deduped.append(compact)
        if len(deduped) >= LINE_RECURSIVE_COVERAGE_MAX_QUERIES:
            break
    return deduped


def append_recursive_coverage_hits(
    user_text: str,
    selected_hits: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
) -> tuple[list[KnowledgeHit], str]:
    queries = recursive_coverage_queries(user_text, selected_hits, clinical_intent)
    if not queries:
        return selected_hits, ""

    merged = list(selected_hits)
    seen = {hit_identity(hit) for hit in merged}
    added = 0
    for query in queries:
        for hit in search_knowledge_candidates(query):
            key = hit_identity(hit)
            if key in seen:
                continue
            seen.add(key)
            merged.append(hit)
            added += 1
            if added >= LINE_RECURSIVE_COVERAGE_MAX_HITS:
                note = f"recursive coverage retrieval 補入 {added} 個候選片段。"
                return merged, note
    if added:
        return merged, f"recursive coverage retrieval 補入 {added} 個候選片段。"
    return selected_hits, "recursive coverage retrieval 已執行，但沒有找到新的非重複片段。"


def broad_section_context_needed(
    user_text: str,
    hits: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
) -> bool:
    if not LINE_WHOLE_SECTION_CONTEXT_ENABLED:
        return False
    lower = f"{user_text} {clinical_intent_text(clinical_intent)}".lower()
    facets = set(required_facets(user_text))
    facets.update(required_facets(clinical_intent_text(clinical_intent)))
    facets.update(json_list((clinical_intent or {}).get("required_facets")))
    if "technology_indication" in facets:
        return True
    broad_terms = ("哪些", "哪種", "誰可以", "適用", "適合", "使用對象", "建議", "怎麼選", "治療建議")
    if any(term in user_text for term in broad_terms):
        return True
    if any(term in lower for term in ("who should", "indication", "recommended population", "management recommendation")):
        return True
    return any("whole_section_context" in getattr(hit, "metadata", ()) for hit in hits)


def append_whole_section_context_hits(
    user_text: str,
    selected_hits: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
) -> tuple[list[KnowledgeHit], str]:
    if not selected_hits or not broad_section_context_needed(user_text, selected_hits, clinical_intent):
        return selected_hits, ""
    whole_hits = search_whole_section_context(user_text, selected_hits)
    if not whole_hits:
        return selected_hits, "whole-section context 已觸發，但沒有找到可加入的完整 section。"
    seen = {hit_identity(hit) for hit in selected_hits}
    merged = list(selected_hits)
    added = 0
    for hit in whole_hits:
        key = hit_identity(hit)
        if key in seen:
            continue
        seen.add(key)
        merged.append(hit)
        added += 1
    if not added:
        return selected_hits, ""
    return merged, f"whole-section context 補入 {added} 個完整相關章節。"


def comparative_threshold_question(user_text: str) -> bool:
    lower = user_text.lower()
    has_numeric_threshold = bool(re.search(r"\b\d+(?:\.\d+)?\b", user_text))
    has_kidney = any(term in user_text for term in ("腎", "腎功能", "腎絲球")) or any(
        term in lower for term in ("egfr", "ckd", "kidney", "renal")
    )
    has_medication = any(term in user_text for term in ("藥", "用藥", "合併")) or any(
        term in lower for term in ("medication", "pharmacologic", "sglt", "glp", "metformin", "finerenone", "mra")
    )
    return has_numeric_threshold and has_kidney and has_medication


def select_guideline_hits(
    api_key: str,
    user_text: str,
    candidates: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
) -> tuple[list[KnowledgeHit], bool, str]:
    if not candidates:
        return [], False, "沒有候選片段。"
    if not LINE_LLM_RERANK_ENABLED or not api_key:
        return candidates[:LINE_LLM_RERANK_TOP_K], True, "LLM reranker disabled; using local ranking."

    system_text = (
        "你是醫療指南檢索 reranker，不是回答者。"
        "你只能根據候選指南片段判斷哪些片段最能回答使用者問題。"
        "不要提供醫療建議，不要使用模型內建知識，不要補充候選片段以外的內容。"
        "你會收到 clinical intent JSON；請根據 required_facets 與 answer_strategy 判斷片段是否足夠，而不是只看候選片段是否逐字命中使用者原句。"
        "請特別檢查問題中的所有核心概念是否都有片段支持，例如藥物類別、疾病階段、eGFR 門檻、禁忌或安全限制。"
        "優先選擇 recommendation、treatment、selection、screening、diagnosis、table_row、含 eGFR/threshold/contraindication/avoid/dose 的片段。"
        "若 CKD/eGFR/albuminuria/UACR/finerenone/腎臟問題有 KDIGO 候選，請優先檢查並保留 KDIGO，因為腎臟病分期、eGFR、albuminuria 與腎臟保護治療通常以 KDIGO 較完整；再搭配 ADA/AACE 的糖尿病用藥與整體照護片段。若片段不足以完整回答，answerable 必須是 false。"
        "若使用者問洗腎/透析時血糖控制目標，但候選片段顯示沒有單一固定數字、需個別化、A1C 在 advanced CKD 較不可靠，並提供 CGM/BGM 或替代指標片段，這種情況可判定 answerable=true，用來回答「指南沒有固定單一目標，應個別化」。"
        "若使用者問特定 eGFR 數值下的用藥或合併用藥，而候選片段提供 eGFR 起始/使用門檻（例如 ≥20、≥25、≥30）或 CKD glucose-lowering therapy 建議，這可以回答「哪些藥物在此數值下不符合起始條件、哪些需依指南條件評估」，answerable 可為 true；不要因為片段沒有逐字寫出該 exact eGFR 數字就判 false。"
        "只輸出 JSON，格式：{\"selected_ids\":[1,2,3],\"answerable\":true,\"coverage_gaps\":[\"...\"]}。"
    )
    prompt = (
        f"使用者問題：{user_text}\n\n"
        f"{clinical_intent_prompt(clinical_intent) or '臨床問題理解：無'}\n\n"
        f"{knowledge_candidates_prompt(candidates)}\n\n"
        f"請選出最多 {LINE_LLM_RERANK_TOP_K} 個最能回答問題的候選 id，並判斷 evidence coverage 是否足夠。"
    )
    try:
        raw = call_llm(
            api_key,
            system_text,
            prompt,
            max_output_tokens=420,
            temperature=0.05,
            timeout=min(GEMINI_TIMEOUT, 12),
        )
    except Exception as exc:
        print(f"{LLM_PROVIDER} rerank failed: {type(exc).__name__}: {exc}")
        return candidates[:LINE_LLM_RERANK_TOP_K], True, "Reranker failed; using local ranking."

    data = extract_json_object(raw)
    selected_ids = data.get("selected_ids")
    if not isinstance(selected_ids, list):
        selected_ids = []

    selected: list[KnowledgeHit] = []
    seen: set[int] = set()
    for raw_id in selected_ids:
        try:
            candidate_index = int(raw_id) - 1
        except (TypeError, ValueError):
            continue
        if 0 <= candidate_index < len(candidates) and candidate_index not in seen:
            seen.add(candidate_index)
            selected.append(candidates[candidate_index])
        if len(selected) >= LINE_LLM_RERANK_TOP_K:
            break

    if not selected:
        selected = candidates[:LINE_LLM_RERANK_TOP_K]

    answerable_value = data.get("answerable")
    if isinstance(answerable_value, bool):
        answerable = answerable_value
    elif answerable_value is None:
        answerable = True
    else:
        answerable = str(answerable_value).strip().lower() not in {"false", "no", "0"}
    gaps = data.get("coverage_gaps")
    if isinstance(gaps, list):
        coverage_gaps = "；".join(str(gap) for gap in gaps if str(gap).strip())
    else:
        coverage_gaps = str(gaps or "").strip()
    local_answerable, local_gap = local_evidence_coverage(user_text, selected, clinical_intent)
    if not local_answerable:
        all_candidates_answerable, all_candidates_gap = local_evidence_coverage(user_text, candidates, clinical_intent)
        if all_candidates_answerable:
            selected = candidates[:LINE_LLM_RERANK_TOP_K]
            local_answerable = True
            coverage_gaps = (coverage_gaps + "；" if coverage_gaps else "") + "LLM reranker 選片漏掉部分必要面向，已改用本地 coverage-aware 候選。"
        else:
            coverage_gaps = (coverage_gaps + "；" if coverage_gaps else "") + (local_gap or all_candidates_gap)
    if comparative_threshold_question(user_text) and local_answerable and not answerable:
        answerable = True
        coverage_gaps = (
            coverage_gaps + "；" if coverage_gaps else ""
        ) + "候選片段提供 eGFR 門檻，可用來回答此數值下的用藥限制與可評估方向。"
    if not local_answerable:
        answerable = False
    return selected, answerable, coverage_gaps


def build_evidence_review(
    api_key: str,
    user_text: str,
    knowledge_text: str,
    clinical_intent: dict[str, Any] | None = None,
) -> str:
    if not LINE_EVIDENCE_REVIEW_ENABLED or not api_key:
        return ""

    system_text = (
        "你是糖尿病指南證據整理助手，不是最終回答者。"
        "只能根據提供的指南片段整理，不可使用模型內建知識、未載入指南、新聞或推測補完。"
        "你會收到 clinical intent JSON；請用 answer_strategy 組織證據，但不可把 clinical intent 當成醫療證據。"
        "請用繁體中文輸出精簡整理："
        "1. 可直接回答使用者問題的指南重點；"
        "2. 片段中明確的門檻、限制、藥物例外或安全提醒；"
        "3. 使用者問題中的每個核心概念是否都有片段支持；"
        "4. 若指南沒有給單一固定數字，但有說明應個別化或 A1C 不可靠，也要明確整理成可回答重點；"
        "5. 若使用者問特定 eGFR 數值下的用藥，而片段提供藥物的 eGFR 起始/使用門檻，請用比較方式整理哪些不符合門檻、哪些可依指南條件評估；"
        "6. 片段不足或不能回答的地方。"
        "最後一行必須寫 ANSWERABLE: yes 或 ANSWERABLE: no。"
    )
    prompt = (
        f"使用者問題：{user_text}\n\n"
        f"{clinical_intent_prompt(clinical_intent) or '臨床問題理解：無'}\n\n"
        f"{knowledge_text}\n\n"
        "請先整理證據，不要寫給病友看的最終答案。"
    )
    try:
        return call_llm(
            api_key,
            system_text,
            prompt,
            max_output_tokens=520,
            temperature=0.1,
            timeout=min(GEMINI_TIMEOUT, 12),
        ).strip()
    except Exception as exc:
        print(f"{LLM_PROVIDER} evidence review failed: {type(exc).__name__}: {exc}")
        return ""


def build_long_context_verification(
    api_key: str,
    user_text: str,
    hits: list[KnowledgeHit],
    clinical_intent: dict[str, Any] | None = None,
    evidence_review: str = "",
) -> str:
    if not LINE_LONG_CONTEXT_VERIFICATION_ENABLED or not api_key or not hits:
        return ""

    system_text = (
        "你是臨床指南長上下文驗證器，不是最終回答者。"
        "你只能根據提供的指南片段、父層章節上下文與結構化標籤檢查 evidence coverage。"
        "不要使用模型內建知識、不要補充未提供的指南內容。"
        "請檢查：1. 是否有回答使用者核心問題的直接依據；"
        "2. 是否需要補足其他章節、表格、footnote 或特殊族群；"
        "3. recommendation、rationale、table 或 safety warning 是否互相矛盾。"
        "輸出繁體中文，最多 6 行。最後一行必須是 VERIFIED: yes 或 VERIFIED: no。"
    )
    prompt = (
        f"使用者問題：{user_text}\n\n"
        f"{clinical_intent_prompt(clinical_intent) or '臨床問題理解：無'}\n\n"
        f"{knowledge_prompt_from_hits(hits)}\n\n"
        f"初步證據整理：\n{evidence_review or '無'}\n\n"
        "請做長上下文二次確認。"
    )
    try:
        return call_llm(
            api_key,
            system_text,
            prompt,
            max_output_tokens=420,
            temperature=0.05,
            timeout=min(GEMINI_TIMEOUT, 12),
        ).strip()
    except Exception as exc:
        print(f"{LLM_PROVIDER} long-context verification failed: {type(exc).__name__}: {exc}")
        return ""


def long_context_verification_prompt(verification: str) -> str:
    if not verification:
        return ""
    return (
        "\n\n長上下文二次確認：\n"
        "以下驗證只根據本輪片段與父層章節上下文；若它指出缺口，最終回答不可補完缺口。\n"
        f"{verification}"
    )


def long_context_says_unverified(verification: str) -> bool:
    return bool(re.search(r"VERIFIED\s*:\s*no\b", verification, flags=re.I))


def evidence_review_prompt(review: str) -> str:
    if not review:
        return ""
    return (
        "\n\n檢索後證據整理：\n"
        "以下整理只來自本輪指南片段，用來幫助最終回答完整且不漏重點；"
        "若它和原始指南片段衝突，必須以原始片段為準。\n"
        f"{review}"
    )


def rerank_coverage_prompt(coverage_gaps: str) -> str:
    if not coverage_gaps:
        return ""
    return (
        "\n\nReranker coverage check：\n"
        "以下是檢索 reranker 對候選片段覆蓋度的判斷。若提到缺口，最終回答不可補完缺口，只能說明指南片段不足。\n"
        f"{coverage_gaps}"
    )


def evidence_review_says_unanswerable(review: str) -> bool:
    return bool(re.search(r"ANSWERABLE\s*:\s*no\b", review, flags=re.I))


def remove_trailing_question(text: str) -> str:
    paragraphs = [part.strip() for part in re.split(r"\n{2,}", text.strip()) if part.strip()]
    if not paragraphs:
        return text.strip()

    last = paragraphs[-1]
    if not re.search(r"[？?]\s*$", last):
        return text.strip()

    question_starters = (
        "請問",
        "想請問",
        "可否",
        "是否",
        "方便",
        "能否",
        "您目前",
        "你目前",
        "如果方便",
        "若方便",
        "May I",
        "Could you",
        "Can you",
        "Would you",
        "Do you",
        "Are you",
        "What",
        "When",
        "Where",
        "Which",
        "How",
    )

    sentence_parts = re.split(r"(?<=[。.!！])\s*", last)
    trailing_sentence = sentence_parts[-1].strip() if sentence_parts else last
    if trailing_sentence.startswith(question_starters) or len(trailing_sentence) <= 120:
        if len(sentence_parts) > 1:
            sentence_parts.pop()
            paragraphs[-1] = "".join(sentence_parts).strip()
            return "\n\n".join(part for part in paragraphs if part).strip()
        paragraphs.pop()
        return "\n\n".join(paragraphs).strip()
    return text.strip()


def serialize_debug_hit(hit: KnowledgeHit, index: int) -> dict[str, Any]:
    facets = sorted(hit_facets(hit))
    return {
        "id": index,
        "source": getattr(hit, "source", ""),
        "source_label": getattr(hit, "source_label", ""),
        "title": getattr(hit, "title", ""),
        "section": getattr(hit, "section", ""),
        "chunk_type": getattr(hit, "chunk_type", ""),
        "score": round(float(getattr(hit, "score", 0.0)), 3),
        "metadata": list(getattr(hit, "metadata", ())[:24]),
        "facets": facets,
        "has_parent_excerpt": bool(getattr(hit, "parent_excerpt", "")),
        "excerpt": re.sub(r"\s+", " ", str(getattr(hit, "excerpt", ""))).strip()[:420],
        "parent_excerpt": re.sub(r"\s+", " ", str(getattr(hit, "parent_excerpt", ""))).strip()[:420],
    }


def debug_search_trace(user_text: str, use_llm: bool = False) -> dict[str, Any]:
    api_key = active_api_key() if use_llm else ""
    recent_context = ""
    clinical_intent = build_clinical_intent(api_key, user_text, recent_context) if use_llm else fallback_clinical_intent(user_text)
    retrieval_query = build_retrieval_query(api_key, user_text, recent_context, clinical_intent) if use_llm else " ".join(
        part for part in [user_text, clinical_intent_text(clinical_intent)] if part
    ).strip()
    variants = query_variant_specs(retrieval_query)
    candidates = search_knowledge_candidates(retrieval_query)
    selected_hits, answerable, coverage_gaps = select_guideline_hits(api_key, user_text, candidates, clinical_intent)
    selected_hits, recursive_note = append_recursive_coverage_hits(user_text, selected_hits, clinical_intent)
    selected_hits, whole_section_note = append_whole_section_context_hits(user_text, selected_hits, clinical_intent)
    local_answerable, local_gap = local_evidence_coverage(user_text, selected_hits, clinical_intent)
    covered_facets: set[str] = set()
    for hit in selected_hits:
        covered_facets.update(hit_facets(hit))
    required = set(required_facets(user_text))
    required.update(required_facets(clinical_intent_text(clinical_intent)))
    required.update(json_list((clinical_intent or {}).get("required_facets")))
    return {
        "query": user_text,
        "use_llm": use_llm,
        "retrieval_query": retrieval_query,
        "clinical_intent": clinical_intent,
        "query_variants": [
            {
                "label": getattr(variant, "label", ""),
                "weight": getattr(variant, "weight", 0.0),
                "text": getattr(variant, "text", ""),
            }
            for variant in variants
        ],
        "required_facets": sorted(required),
        "covered_facets": sorted(covered_facets),
        "missing_facets": sorted(required - covered_facets),
        "candidate_count": len(candidates),
        "candidates": [
            serialize_debug_hit(hit, index)
            for index, hit in enumerate(candidates[:LINE_DEBUG_SEARCH_MAX_HITS], start=1)
        ],
        "selected_count": len(selected_hits),
        "selected_hits": [
            serialize_debug_hit(hit, index)
            for index, hit in enumerate(selected_hits[: max(LINE_DEBUG_SEARCH_MAX_HITS, LINE_LLM_RERANK_TOP_K)], start=1)
        ],
        "rerank_answerable": answerable,
        "local_answerable": local_answerable,
        "coverage_gaps": coverage_gaps,
        "local_gap": local_gap,
        "recursive_note": recursive_note,
        "whole_section_note": whole_section_note,
        "knowledge": knowledge_status(),
    }


def llm_answer(user_text: str, line_user_id: str = "") -> str:
    api_key = active_api_key()
    if not api_key:
        return f"目前快速問答服務尚未設定 {LLM_PROVIDER} API key。若你有血糖不舒服、低血糖症狀或血糖持續很高，請先聯絡醫療團隊。"

    recent_context = conversation_prompt(line_user_id)
    clinical_intent = build_clinical_intent(api_key, user_text, recent_context)
    retrieval_query = build_retrieval_query(api_key, user_text, recent_context, clinical_intent)
    candidates = search_knowledge_candidates(retrieval_query)
    if not candidates:
        return knowledge_no_answer_text()

    selected_hits, rerank_answerable, coverage_gaps = select_guideline_hits(api_key, user_text, candidates, clinical_intent)
    selected_hits, recursive_note = append_recursive_coverage_hits(user_text, selected_hits, clinical_intent)
    if recursive_note:
        coverage_gaps = (coverage_gaps + "；" if coverage_gaps else "") + recursive_note
    if selected_hits:
        recursive_answerable, recursive_gap = local_evidence_coverage(user_text, selected_hits, clinical_intent)
        if recursive_answerable:
            rerank_answerable = True
        elif recursive_gap:
            coverage_gaps = (coverage_gaps + "；" if coverage_gaps else "") + recursive_gap
        selected_hits, whole_section_note = append_whole_section_context_hits(user_text, selected_hits, clinical_intent)
        if whole_section_note:
            coverage_gaps = (coverage_gaps + "；" if coverage_gaps else "") + whole_section_note
        whole_answerable, whole_gap = local_evidence_coverage(user_text, selected_hits, clinical_intent)
        if whole_answerable:
            rerank_answerable = True
        elif whole_gap:
            coverage_gaps = (coverage_gaps + "；" if coverage_gaps else "") + whole_gap
    if not selected_hits or not rerank_answerable:
        return knowledge_no_answer_text()

    knowledge_text = knowledge_prompt_from_hits(selected_hits)
    evidence_review = build_evidence_review(api_key, user_text, knowledge_text, clinical_intent)
    if evidence_review_says_unanswerable(evidence_review):
        locally_answerable, _local_gap = local_evidence_coverage(user_text, selected_hits, clinical_intent)
        if not (comparative_threshold_question(user_text) and locally_answerable):
            return knowledge_no_answer_text()
        evidence_review = (
            evidence_review
            + "\n\n本地 coverage override：此題可用指南片段中的 eGFR 門檻做比較式回答；"
            "不要補充片段外資訊，也不要給個人化劑量。"
        )
    long_context_verification = build_long_context_verification(
        api_key,
        user_text,
        selected_hits,
        clinical_intent,
        evidence_review,
    )
    if long_context_says_unverified(long_context_verification):
        locally_answerable, _local_gap = local_evidence_coverage(user_text, selected_hits, clinical_intent)
        if not (comparative_threshold_question(user_text) and locally_answerable):
            return knowledge_no_answer_text()
    system_text = (
        SYSTEM_PROMPT
        + memory_prompt(line_user_id)
        + recent_context
        + clinical_intent_prompt(clinical_intent)
        + knowledge_text
        + rerank_coverage_prompt(coverage_gaps)
        + evidence_review_prompt(evidence_review)
        + long_context_verification_prompt(long_context_verification)
    )
    try:
        answer = call_llm(
            api_key,
            system_text,
            f"病友問題：{user_text}\n\n請先完整檢視本輪指南片段與證據整理，再用繁體中文回答。不要提出追問。",
            max_output_tokens=820,
            temperature=0.35,
            timeout=GEMINI_TIMEOUT,
        )
        if answer:
            return remove_trailing_question(answer)[:4900]
        return "目前系統暫時沒有產生完整回覆。若你有明顯不舒服或血糖異常，請先聯絡醫療團隊。"
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:200]
        print(f"{LLM_PROVIDER} HTTP {exc.code}: {detail}")
    except Exception as exc:
        print(f"{LLM_PROVIDER} request failed: {type(exc).__name__}: {exc}")
    return "目前快速問答暫時無法回覆。若你有低血糖症狀、血糖持續很高、胸痛、意識不清或明顯不舒服，請先聯絡醫療團隊或就醫。"


async def handle_text_event(event: dict[str, Any]) -> None:
    message = event.get("message", {})
    user_text = (message.get("text") or "").strip()
    target = source_target(event)
    session_key = source_session_key(event)
    reply_token = (event.get("replyToken") or "").strip()
    if not user_text or not target or not session_key:
        return

    loop = asyncio.get_running_loop()
    answer = await loop.run_in_executor(None, answer_for_session, session_key, user_text)
    if reply_token:
        ok, status = line_reply_text(reply_token, answer)
        print(f"LINE fast QA reply status: {status}")
        if ok:
            return
    ok, status = line_push_text(target, answer)
    print(f"LINE fast QA fallback push status: {status}")
    if not ok:
        print(f"Failed to push LINE fast QA answer for target={target[:8]}...")


@app.get("/")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "line-lifebot-qa",
        "app_version": APP_VERSION,
        "llm_provider": LLM_PROVIDER,
        "model": active_model(),
        "llm_configured": llm_configured(),
        "gemini_configured": bool(os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()),
        "deepseek_configured": bool(os.getenv("DEEPSEEK_API_KEY", "").strip()),
        "deepseek_thinking_enabled": DEEPSEEK_THINKING_ENABLED if LLM_PROVIDER == "deepseek" else False,
        "line_configured": bool(os.getenv("LINE_CHANNEL_SECRET", "").strip() and os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()),
        "memory_enabled": LINE_MEMORY_ENABLED,
        "memory_backend": memory_backend(),
        "features": {
            "english_name_memory": True,
            "chinese_name_memory": True,
            "trailing_question_removal": True,
            "short_term_context": LINE_CONTEXT_ENABLED,
            "session_scoped_context": True,
            "guideline_strict_grounding": True,
            "guideline_query_planning": LINE_QUERY_PLANNING_ENABLED,
            "clinical_intent_planning": LINE_QUERY_PLANNING_ENABLED,
            "guideline_evidence_review": LINE_EVIDENCE_REVIEW_ENABLED,
            "all_mounted_guideline_sources": True,
            "ada_only_sources": False,
            "multi_guideline_sources": True,
            "source_aware_reranking": True,
            "source_balanced_retrieval": True,
            "section_aware_retrieval": True,
            "chapter_section_index": True,
            "recommendation_aware_retrieval": True,
            "table_aware_retrieval": True,
            "multi_query_retrieval": True,
            "intent_query_variants": True,
            "clinical_concept_routing": True,
            "metadata_indexing": True,
            "automatic_ontology_extraction": True,
            "dense_embedding_index": True,
            "coverage_aware_retrieval": True,
            "mmr_style_diversity": True,
            "local_coverage_answerability": True,
            "comparative_threshold_answering": True,
            "threshold_review_override": True,
            "deepseek_provider": True,
            "llm_reranker": LINE_LLM_RERANK_ENABLED,
            "coverage_answerability_check": True,
            "parent_child_table_context": True,
            "parent_child_section_retrieval": True,
            "structured_metadata_extraction": True,
            "recursive_coverage_retrieval": LINE_RECURSIVE_COVERAGE_ENABLED,
            "whole_section_context": LINE_WHOLE_SECTION_CONTEXT_ENABLED,
            "local_hashed_vector_index": True,
            "long_context_verification": LINE_LONG_CONTEXT_VERIFICATION_ENABLED,
            "debug_search_endpoint": LINE_DEBUG_SEARCH_ENABLED,
            "guideline_strict_grounding_current": True,
            "guideline_query_planning_current": LINE_QUERY_PLANNING_ENABLED,
            "guideline_evidence_review_current": LINE_EVIDENCE_REVIEW_ENABLED,
        },
        "context_enabled": LINE_CONTEXT_ENABLED,
        "context_max_messages": LINE_CONTEXT_MAX_MESSAGES,
        "context_ttl_seconds": LINE_CONTEXT_TTL_SECONDS,
        "session_scope": LINE_SESSION_SCOPE,
        "knowledge": knowledge_status(),
    }


@app.get("/debug/search")
def debug_search(q: str = "", llm: bool = False, x_debug_token: str = Header(default="")) -> dict[str, Any]:
    if not LINE_DEBUG_SEARCH_ENABLED:
        raise HTTPException(status_code=404, detail="debug search disabled")
    expected_token = os.getenv("LINE_DEBUG_TOKEN", "").strip()
    if expected_token and not hmac.compare_digest(x_debug_token, expected_token):
        raise HTTPException(status_code=403, detail="invalid debug token")
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="missing q")
    return debug_search_trace(query, use_llm=llm)


@app.post("/line/webhook")
async def line_webhook(request: Request, x_line_signature: str = Header(default="")) -> dict[str, bool]:
    body = await request.body()
    verify_line_signature(body, x_line_signature)
    payload = json.loads(body.decode("utf-8"))
    for event in payload.get("events", []):
        if event.get("type") == "message" and event.get("message", {}).get("type") == "text":
            asyncio.create_task(handle_text_event(event))
    return {"ok": True}
