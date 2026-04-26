# LifeBot Fast LINE QA

Fast Zeabur webhook for ordinary diabetes patient-education questions.

This service answers LINE text messages directly with OpenAI and does not start
Hermes Agent. Keep Hermes Agent for scheduled diabetes news, academic search,
Obsidian/Google Drive archiving, image generation, and audio generation.

## Endpoints

- `GET /` health check
- `POST /line/webhook` LINE Messaging API webhook

## Zeabur Environment Variables

```bash
LINE_CHANNEL_SECRET=...
LINE_CHANNEL_ACCESS_TOKEN=...
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4.1-mini
OPENAI_TIMEOUT=20
LINE_TIMEOUT=12
```

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
