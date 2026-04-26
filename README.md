# LifeBot Fast LINE QA

Fast Zeabur webhook for ordinary diabetes patient-education questions.

This service answers LINE text messages directly with Gemini API and does not start
Hermes Agent. Keep Hermes Agent for scheduled diabetes news, academic search,
Obsidian/Google Drive archiving, image generation, and audio generation.

## Endpoints

- `GET /` health check
- `POST /line/webhook` LINE Messaging API webhook

## Zeabur Environment Variables

```bash
LINE_CHANNEL_SECRET=...
LINE_CHANNEL_ACCESS_TOKEN=...
GEMINI_API_KEY=...
GEMINI_MODEL=gemini-3.1-flash-lite-preview
GEMINI_TIMEOUT=20
LINE_TIMEOUT=12
```

`GOOGLE_API_KEY` is also accepted as a fallback. If Google AI Studio changes the
preview model name, set `GEMINI_MODEL` to the new model name in Zeabur without
changing the code.

## LINE Webhook URL

After Zeabur deploys this service, set the LINE webhook to:

```text
https://<your-zeabur-domain>/line/webhook
```

The previous local Hermes bridge can stay available for testing, but the fast
QA production webhook should point to this Zeabur service.

## Local Test

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 127.0.0.1 --port 8790
```

Open:

```text
http://127.0.0.1:8790/
```
