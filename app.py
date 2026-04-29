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
        search_knowledge_candidates,
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


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
APP_VERSION = os.getenv("APP_VERSION", "2026-04-30-coverage-retrieval-v8")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
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
- 只能根據「背景知識檢索」提供的糖尿病指南片段回答，來源可能包含 ADA、AACE、KDIGO 等已載入檔案。
- 不要使用模型內建知識、一般醫學常識、未載入指南、新聞或推測補完。
- 若指南片段沒有直接依據，請明確說目前指南知識庫資料不足，並停止回答。
- 回答中請自然標示依據來源，例如 ADA、AACE 或 KDIGO；不要編造未出現在片段中的來源，也不要使用 draft 或 public review draft 字樣。
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
        answer = memory_answer or gemini_answer(user_text, session_key)
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


def extract_json_object(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, flags=re.S)
    if not match:
        return {}
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def build_retrieval_query(api_key: str, user_text: str, recent_context: str) -> str:
    if not LINE_QUERY_PLANNING_ENABLED or not api_key:
        return user_text

    system_text = (
        "你不是回答者，也不要提供醫療建議。"
        "你的唯一任務是把 LINE 病友問題轉成糖尿病指南文件檢索查詢，來源可能包含 ADA、AACE、KDIGO。"
        "請根據本次問題與最近對話脈絡，補上可能出現在這些指南文件中的英文術語、縮寫、同義詞與章節詞。"
        "若問題提到洗腎/透析/腎衰竭與血糖控制目標，請加入 dialysis、kidney failure、glycemic goals、A1C goal、A1C reliability、CGM、BGM、glycated albumin、fructosamine。"
        "不要新增使用者沒有問到的病情、診斷、用藥劑量或結論。"
        "只輸出 JSON，格式為：{\"search_query\":\"...\",\"keywords\":[\"...\"]}。"
    )
    prompt = (
        f"本次問題：{user_text}\n\n"
        f"{recent_context or '最近對話脈絡：無'}\n\n"
        "請產生適合全文檢索糖尿病指南 Markdown 的查詢。"
    )
    try:
        raw = call_gemini(
            api_key,
            system_text,
            prompt,
            max_output_tokens=260,
            temperature=0.1,
            timeout=min(GEMINI_TIMEOUT, 10),
        )
    except Exception as exc:
        print(f"Gemini query planning failed: {type(exc).__name__}: {exc}")
        return user_text

    data = extract_json_object(raw)
    search_query = str(data.get("search_query") or "").strip()
    keywords = data.get("keywords") if isinstance(data.get("keywords"), list) else []
    keyword_text = " ".join(str(keyword).strip() for keyword in keywords if str(keyword).strip())
    combined = " ".join(part for part in [user_text, search_query, keyword_text] if part).strip()
    return combined[:LINE_RETRIEVAL_QUERY_MAX_CHARS] or user_text


def select_guideline_hits(api_key: str, user_text: str, candidates: list[KnowledgeHit]) -> tuple[list[KnowledgeHit], bool, str]:
    if not candidates:
        return [], False, "沒有候選片段。"
    if not LINE_LLM_RERANK_ENABLED or not api_key:
        return candidates[:LINE_LLM_RERANK_TOP_K], True, "LLM reranker disabled; using local ranking."

    system_text = (
        "你是醫療指南檢索 reranker，不是回答者。"
        "你只能根據候選指南片段判斷哪些片段最能回答使用者問題。"
        "不要提供醫療建議，不要使用模型內建知識，不要補充候選片段以外的內容。"
        "請特別檢查問題中的所有核心概念是否都有片段支持，例如藥物類別、疾病階段、eGFR 門檻、禁忌或安全限制。"
        "優先選擇 recommendation、treatment、selection、screening、diagnosis、table_row、含 eGFR/threshold/contraindication/avoid/dose 的片段。"
        "若 CKD/eGFR/腎臟問題同時有 KDIGO 與 ADA 相關候選，請優先保留它們；若片段不足以完整回答，answerable 必須是 false。"
        "若使用者問洗腎/透析時血糖控制目標，但候選片段顯示沒有單一固定數字、需個別化、A1C 在 advanced CKD 較不可靠，並提供 CGM/BGM 或替代指標片段，這種情況可判定 answerable=true，用來回答「指南沒有固定單一目標，應個別化」。"
        "只輸出 JSON，格式：{\"selected_ids\":[1,2,3],\"answerable\":true,\"coverage_gaps\":[\"...\"]}。"
    )
    prompt = (
        f"使用者問題：{user_text}\n\n"
        f"{knowledge_candidates_prompt(candidates)}\n\n"
        f"請選出最多 {LINE_LLM_RERANK_TOP_K} 個最能回答問題的候選 id，並判斷 evidence coverage 是否足夠。"
    )
    try:
        raw = call_gemini(
            api_key,
            system_text,
            prompt,
            max_output_tokens=420,
            temperature=0.05,
            timeout=min(GEMINI_TIMEOUT, 12),
        )
    except Exception as exc:
        print(f"Gemini rerank failed: {type(exc).__name__}: {exc}")
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
    return selected, answerable, coverage_gaps


def build_evidence_review(api_key: str, user_text: str, knowledge_text: str) -> str:
    if not LINE_EVIDENCE_REVIEW_ENABLED or not api_key:
        return ""

    system_text = (
        "你是糖尿病指南證據整理助手，不是最終回答者。"
        "只能根據提供的指南片段整理，不可使用模型內建知識、未載入指南、新聞或推測補完。"
        "請用繁體中文輸出精簡整理："
        "1. 可直接回答使用者問題的指南重點；"
        "2. 片段中明確的門檻、限制、藥物例外或安全提醒；"
        "3. 使用者問題中的每個核心概念是否都有片段支持；"
        "4. 若指南沒有給單一固定數字，但有說明應個別化或 A1C 不可靠，也要明確整理成可回答重點；"
        "5. 片段不足或不能回答的地方。"
        "最後一行必須寫 ANSWERABLE: yes 或 ANSWERABLE: no。"
    )
    prompt = f"使用者問題：{user_text}\n\n{knowledge_text}\n\n請先整理證據，不要寫給病友看的最終答案。"
    try:
        return call_gemini(
            api_key,
            system_text,
            prompt,
            max_output_tokens=520,
            temperature=0.1,
            timeout=min(GEMINI_TIMEOUT, 12),
        ).strip()
    except Exception as exc:
        print(f"Gemini evidence review failed: {type(exc).__name__}: {exc}")
        return ""


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


def gemini_answer(user_text: str, line_user_id: str = "") -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        return "目前快速問答服務尚未設定 Gemini API key。若你有血糖不舒服、低血糖症狀或血糖持續很高，請先聯絡醫療團隊。"

    recent_context = conversation_prompt(line_user_id)
    retrieval_query = build_retrieval_query(api_key, user_text, recent_context)
    candidates = search_knowledge_candidates(retrieval_query)
    if not candidates:
        return knowledge_no_answer_text()

    selected_hits, rerank_answerable, coverage_gaps = select_guideline_hits(api_key, user_text, candidates)
    if not selected_hits or not rerank_answerable:
        return knowledge_no_answer_text()

    knowledge_text = knowledge_prompt_from_hits(selected_hits)
    evidence_review = build_evidence_review(api_key, user_text, knowledge_text)
    if evidence_review_says_unanswerable(evidence_review):
        return knowledge_no_answer_text()
    system_text = (
        SYSTEM_PROMPT
        + memory_prompt(line_user_id)
        + recent_context
        + knowledge_text
        + rerank_coverage_prompt(coverage_gaps)
        + evidence_review_prompt(evidence_review)
    )
    try:
        answer = call_gemini(
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
        print(f"Gemini HTTP {exc.code}: {detail}")
    except Exception as exc:
        print(f"Gemini request failed: {type(exc).__name__}: {exc}")
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
        "model": GEMINI_MODEL,
        "gemini_configured": bool(os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()),
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
            "guideline_evidence_review": LINE_EVIDENCE_REVIEW_ENABLED,
            "multi_guideline_sources": True,
            "source_aware_reranking": True,
            "section_aware_retrieval": True,
            "table_aware_retrieval": True,
            "multi_query_retrieval": True,
            "intent_query_variants": True,
            "metadata_indexing": True,
            "coverage_aware_retrieval": True,
            "mmr_style_diversity": True,
            "llm_reranker": LINE_LLM_RERANK_ENABLED,
            "coverage_answerability_check": True,
            "ada_strict_grounding": True,
            "ada_query_planning": LINE_QUERY_PLANNING_ENABLED,
            "ada_evidence_review": LINE_EVIDENCE_REVIEW_ENABLED,
        },
        "context_enabled": LINE_CONTEXT_ENABLED,
        "context_max_messages": LINE_CONTEXT_MAX_MESSAGES,
        "context_ttl_seconds": LINE_CONTEXT_TTL_SECONDS,
        "session_scope": LINE_SESSION_SCOPE,
        "knowledge": knowledge_status(),
    }


@app.post("/line/webhook")
async def line_webhook(request: Request, x_line_signature: str = Header(default="")) -> dict[str, bool]:
    body = await request.body()
    verify_line_signature(body, x_line_signature)
    payload = json.loads(body.decode("utf-8"))
    for event in payload.get("events", []):
        if event.get("type") == "message" and event.get("message", {}).get("type") == "text":
            asyncio.create_task(handle_text_event(event))
    return {"ok": True}
