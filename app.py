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
    from knowledge import knowledge_prompt, knowledge_status
except ModuleNotFoundError:
    def knowledge_prompt(query: str) -> str:
        return (
            "\n\n背景知識檢索：目前部署環境沒有載入 knowledge.py，"
            "請只給一般糖尿病衛教原則，不要假裝查到 ADA guideline 片段。"
        )

    def knowledge_status() -> dict[str, object]:
        return {
            "enabled": False,
            "available": False,
            "error": "knowledge.py not found in deployment",
        }


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
APP_VERSION = os.getenv("APP_VERSION", "2026-04-30-session-context-v2")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "20"))
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
- 優先提供糖尿病自我照護、飲食、運動、用藥安全、血糖監測、併發症預防的衛教。
- 若有提供「背景知識檢索」片段，優先依據該片段回答；不要逐字大段引用，也不要補出片段沒有支持的醫學細節。
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

    body = {
        "system_instruction": {
            "parts": [
                {
                    "text": SYSTEM_PROMPT
                    + memory_prompt(line_user_id)
                    + conversation_prompt(line_user_id)
                    + knowledge_prompt(user_text)
                }
            ]
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": f"病友問題：{user_text}"}],
            }
        ],
        "generationConfig": {
            "maxOutputTokens": 650,
            "temperature": 0.4,
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
    try:
        with urllib.request.urlopen(request, timeout=GEMINI_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
        answer = extract_gemini_text(payload)
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
