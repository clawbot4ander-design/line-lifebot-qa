from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import urllib.error
import urllib.request
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request


GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "20"))
LINE_TIMEOUT = int(os.getenv("LINE_TIMEOUT", "12"))

app = FastAPI(title="LifeBot Fast LINE QA")


SYSTEM_PROMPT = """你是 LifeBot 糖尿病衛教 LINE 機器人，請用繁體中文回答病友問題。

回答規則：
- 口吻溫和、清楚、像衛教師在 LINE 上簡短回覆。
- 不要使用 Markdown 格式，不要使用井字號、星號或程式碼區塊。
- 不要提供個人化診斷、處方、劑量調整、停藥建議，或替代醫師判斷。
- 回答以 2 到 4 個短段落為主，適合手機閱讀。
- 優先提供糖尿病自我照護、飲食、運動、用藥安全、血糖監測、併發症預防的衛教。
- 若問題涉及低血糖、高血糖急症、胸痛、意識不清、酮酸中毒疑慮、懷孕、兒童、腎功能、嚴重感染或傷口惡化，請提醒盡快聯絡醫療團隊或就醫。
- 若資訊不足，最後用一句話請病友補充，例如目前血糖、用藥、症狀、發生時間或飯前飯後。
- 不要編造最新研究、新聞或來源；若病友要求最新醫學期刊或新聞，請說明需要啟用搜尋流程，並先給一般衛教原則。
"""


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
    return source.get("userId") or source.get("groupId") or source.get("roomId") or ""


def extract_gemini_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for candidate in payload.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            if part.get("text"):
                parts.append(str(part["text"]))
    return "\n".join(parts).strip()


def gemini_answer(user_text: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        return "目前快速問答服務尚未設定 Gemini API key。若你有血糖不舒服、低血糖症狀或血糖持續很高，請先聯絡醫療團隊。"

    body = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
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
            return answer[:4900]
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
    reply_token = (event.get("replyToken") or "").strip()
    if not user_text or not target:
        return

    loop = asyncio.get_running_loop()
    answer = await loop.run_in_executor(None, gemini_answer, user_text)
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
        "model": GEMINI_MODEL,
        "gemini_configured": bool(os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_API_KEY", "").strip()),
        "line_configured": bool(os.getenv("LINE_CHANNEL_SECRET", "").strip() and os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()),
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
