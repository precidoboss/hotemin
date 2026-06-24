# Blaze Companion Bot

Persistent Node.js backend for the Blaze Companion loyalty tracker.

## Deploy to Render

1. Push this folder to a GitHub repo
2. Go to render.com → New → Web Service → connect repo
3. Build: `npm install` | Start: `npm start`
4. Add env vars:
   - `BLAZE_TOKEN` — your Blaze user access token
   - `BLAZE_CHANNEL` — your channel UUID
5. Deploy — bot stays alive 24/7

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | UptimeRobot pings this |
| GET | /api/state | Full loyalty state for dashboard |
| POST | /api/connect | Connect bot (send token + channelId) |
| POST | /api/chat | Send a chat message |
| POST | /api/reset | Reset all loyalty data |

## Keep-Alive (Render free tier sleeps after 15min)

Add your Render URL to UptimeRobot (free):
- Monitor type: HTTP(s)
- URL: https://your-bot.onrender.com/health
- Interval: every 5 minutes
