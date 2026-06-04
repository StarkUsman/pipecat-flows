# Pipecat Voice AI — Multi-Agent Platform

A pipecat-based voice AI platform that supports both **single-agent** local development and a **dynamic multi-agent runtime** where agents are created, configured, and destroyed via REST API at runtime.

---

## Architecture Overview

```
manager.py  (Admin REST API — port 8080)
  ├── POST   /agents              → create + start a new agent
  ├── GET    /agents              → list all agents
  ├── GET    /agents/{id}         → get single agent details
  ├── PUT    /agents/{id}/flow    → hot-update an agent's flow code
  ├── PUT    /agents/{id}/activate    → start a stopped agent
  ├── PUT    /agents/{id}/deactivate  → stop without losing config
  └── DELETE /agents/{id}        → permanently remove an agent

bot.py  (per-agent subprocess — WebRTC on port 7860, 7861, ...)
  └── Loads flow from FLOW_PATH env var (per-agent flow.py)

flow.py  (default standalone flow — used when running bot.py directly)

agents/
  registry.json          ← persisted agent registry (auto-generated)
  {agent-id}/
    flow.py              ← per-agent flow code (written by manager)
    config.json          ← per-agent metadata
```

---

## Prerequisites

- Python 3.10 or later
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager
- API keys from:
  - [Deepgram](https://console.deepgram.com/signup) — Speech-to-Text (and fallback TTS)
  - [OpenAI](https://auth.openai.com/create-account) — LLM inference
  - [Cartesia](https://play.cartesia.ai/sign-up) — Text-to-Speech (optional, falls back to Deepgram TTS)

Install dependencies:

```bash
uv sync
```

---

## Mode 1 — Single Agent (standalone)

Run a single bot directly using the default `flow.py`:

```bash
cp env.example .env   # add your API keys
uv run bot.py
```

Open `http://localhost:7860` in your browser and click **Connect**.

To update the flow at runtime without restarting, POST new Python code to the sidecar API:

```bash
curl -X POST http://localhost:8080/deployNewFlow \
  -H "Content-Type: application/json" \
  -d '{"code": "<your flow.py contents>"}'
```

> First run may take ~20 seconds while Pipecat downloads models.

---

## Mode 2 — Multi-Agent Manager

The manager runs as a standalone process and dynamically spawns isolated `bot.py` subprocesses, each with its own flow file, API keys, and WebRTC port.

### Start the manager

```bash
python manager.py
```

The manager listens on port **8080** by default. Agents are assigned WebRTC ports starting at **7860**.

Environment variable overrides:

| Variable | Default | Purpose |
|---|---|---|
| `MANAGER_PORT` | `8080` | Admin API port |
| `AGENT_BASE_PORT` | `7860` | First agent WebRTC port |
| `FLOW_API_BASE_PORT` | `18000` | First per-agent sidecar port |

---

## REST API Reference

### Create an agent

```
POST /agents
```

```json
{
  "name": "interview-bot",
  "flow_code": "<contents of a valid flow.py>",
  "config": {
    "OPENAI_API_KEY": "sk-...",
    "OPENAI_MODEL": "gpt-4o",
    "OPENAI_BASE_URL": "https://api.openai.com/v1",
    "DEEPGRAM_API_KEY": "...",
    "CARTESIA_API_KEY": "...",
    "TTS_PROVIDER": "cartesia"
  }
}
```

Response `201`:

```json
{
  "id": "abc123",
  "name": "interview-bot",
  "port": 7860,
  "status": "running",
  "client_url": "http://localhost:7860/client",
  "created_at": "2024-01-01T00:00:00+00:00"
}
```

---

### List all agents

```
GET /agents
```

Response `200`:

```json
[
  { "id": "abc123", "name": "interview-bot", "port": 7860, "status": "running", "created_at": "..." },
  { "id": "def456", "name": "support-bot",   "port": 7861, "status": "inactive", "created_at": "..." }
]
```

---

### Get a single agent

```
GET /agents/{id}
```

Response `200`:

```json
{
  "id": "abc123",
  "name": "interview-bot",
  "port": 7860,
  "flow_api_port": 18000,
  "status": "running",
  "client_url": "http://localhost:7860/client",
  "created_at": "..."
}
```

---

### Update an agent's flow (hot reload)

```
PUT /agents/{id}/flow
```

```json
{ "flow_code": "<new flow.py contents>" }
```

The new flow takes effect on the next client connection — no restart required.

Response `200`:

```json
{ "status": "ok", "message": "Flow updated — takes effect on next client connection" }
```

---

### Deactivate an agent (stop, keep config)

```
PUT /agents/{id}/deactivate
```

Stops the subprocess. Config, flow file, and port reservation are preserved.

Response `200`:

```json
{ "status": "ok", "message": "Agent stopped" }
```

---

### Activate an agent (restart)

```
PUT /agents/{id}/activate
```

Re-spawns the subprocess using the saved flow and config on the same port.

Response `200`:

```json
{ "status": "ok", "port": 7860, "client_url": "http://localhost:7860/client" }
```

---

### Delete an agent

```
DELETE /agents/{id}
```

Terminates the subprocess (if running), frees the port, removes the agent directory, and removes the registry entry.

Response `200`:

```json
{ "status": "ok" }
```

---

## Port Scheme

| Port range | Purpose |
|---|---|
| 8080 | Manager admin API |
| 7860, 7861, 7862, ... | Agent WebRTC ports (one per agent) |
| 18000, 18001, 18002, ... | Per-agent flow sidecar API ports |

---

## Persistence

The registry is saved to `agents/registry.json` after every state change using an atomic write (write to `.tmp`, then rename). On manager startup, all agents with `status: "running"` are automatically re-spawned. Agents with `status: "inactive"` remain inactive until explicitly activated.

---

## Config Keys Reference

| Key | Required | Purpose |
|---|---|---|
| `OPENAI_API_KEY` | Yes | LLM inference |
| `OPENAI_MODEL` | No | LLM model name (default: provider default) |
| `OPENAI_BASE_URL` | No | Custom LLM endpoint |
| `DEEPGRAM_API_KEY` | Yes | STT and fallback TTS |
| `CARTESIA_API_KEY` | No | Cartesia TTS (falls back to Deepgram if missing) |
| `TTS_PROVIDER` | No | `"cartesia"` or `"deepgram"` (default: `"deepgram"`) |
| `DEEPGRAM_TTS_VOICE` | No | Deepgram voice ID |
| `CARTESIA_TTS_VOICE` | No | Cartesia voice ID |

---

## Backwards Compatibility

Running `bot.py` directly (without the manager) still works exactly as before. If `FLOW_PATH` is not set, the bot loads `flow.py` from its own directory. The manager is an optional layer on top.

---

## Security Note

The `/agents` API accepts arbitrary Python code executed in a subprocess. In production, protect this endpoint with an API key header, IP allowlist, or auth middleware. Do not expose port 8080 publicly without authentication.

---

## Deploy to Production (Pipecat Cloud)

For single-agent cloud deployment, use the Pipecat Cloud CLI:

```bash
uv run pcc auth login
```

Update `pcc-deploy.toml` with your Docker Hub username:

```ini
agent_name = "quickstart"
image = "YOUR_DOCKERHUB_USERNAME/quickstart:0.1"
secret_set = "quickstart-secrets"

[scaling]
    min_agents = 1
```

Upload secrets and deploy:

```bash
uv run pcc secrets set quickstart-secrets --file .env
uv run pcc docker build-push
uv run pcc deploy
```

See [Pipecat Cloud docs](https://docs.pipecat.ai/) for multi-agent cloud deployment options.

---

## Troubleshooting

- **Browser permissions**: Allow microphone access when prompted
- **Port already in use**: Another process is on 7860 or 8080 — set `AGENT_BASE_PORT` or `MANAGER_PORT` env vars
- **Connection issues**: Try a different browser or check VPN/firewall settings
- **Audio issues**: Verify microphone and speakers are working and not muted
- **Agent stuck as running after crash**: Call `PUT /agents/{id}/deactivate` then `PUT /agents/{id}/activate` to reset it
