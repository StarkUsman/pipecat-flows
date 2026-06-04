# Multi-Agent Pipecat — Implementation Plan & Reference

## Problem

The original project ran a single static pipecat agent (port 7860, `flow.py`). The goal is to support **dynamic runtime agent creation**: an admin frontend POSTs a config + flow code, which spins up a new isolated agent on demand. Agents must be creatable, activatable, deactivatable, and deletable at runtime without knowing the number of agents upfront.

---

## Architecture

```
manager.py  (port 8080 — admin API)
  │
  ├── POST   /agents                 → create + start new agent
  ├── GET    /agents                 → list all agents
  ├── GET    /agents/{id}            → get agent details
  ├── PUT    /agents/{id}/flow       → hot-update flow (no restart needed)
  ├── PUT    /agents/{id}/activate   → start a stopped agent
  ├── PUT    /agents/{id}/deactivate → stop without losing config/flow
  ├── DELETE /agents/{id}            → stop + permanently remove
  │
  ├── Subprocess: bot.py  --port 7860  FLOW_PATH=agents/abc/flow.py  OPENAI_API_KEY=...
  ├── Subprocess: bot.py  --port 7861  FLOW_PATH=agents/def/flow.py  OPENAI_API_KEY=...
  └── ...

agents/
  registry.json       ← auto-managed; survives manager restarts
  {agent-id}/
    flow.py           ← written at create/update time
    config.json       ← name + env var snapshot
```

---

## Files Changed / Created

| File | Change |
|------|--------|
| `bot.py` | Added `FLOW_PATH` env var support + dynamic module loading via `importlib.util` |
| `manager.py` | **New** — dynamic agent registry, subprocess lifecycle, admin REST API |
| `agents/` | **Auto-created** at runtime by manager |

---

## How It Works

### bot.py changes (minimal)

`FLOW_PATH` env var now controls which flow file the bot loads.  
If not set, defaults to the original `flow.py` — fully backwards-compatible.

```python
_FLOW_PATH = os.environ.get(
    "FLOW_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow.py"),
)

def _load_flow_module(path: str):
    spec = importlib.util.spec_from_file_location("flow_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

flow_module = _load_flow_module(_FLOW_PATH)
```

On each client connect the module is re-read from disk, so `PUT /agents/{id}/flow` takes effect immediately for new connections without restarting the subprocess.

### manager.py

- Maintains an in-memory registry persisted to `agents/registry.json`
- Allocates ports sequentially from `AGENT_BASE_PORT` (default 7860)
- Spawns each agent as `python bot.py --port PORT` with agent-specific env vars
- On restart, re-spawns all agents that were `"running"` when the manager stopped

---

## Running

### Start the manager (replaces running bot.py directly)

```bash
cd pipecat-flows
python manager.py
# Manager API on :8080, agents will spawn on :7860, :7861, ...
```

### Environment variables for the manager

| Variable | Default | Purpose |
|----------|---------|---------|
| `MANAGER_PORT` | 8080 | Manager admin API port |
| `AGENT_BASE_PORT` | 7860 | First WebRTC port for agents |
| `FLOW_API_BASE_PORT` | 18000 | First sidecar flow-API port for agents |

---

## Admin API Reference

### Create agent

```
POST /agents
Content-Type: application/json

{
  "name": "interview-bot",
  "flow_code": "...",          // Python flow code (string)
  "config": {
    "OPENAI_API_KEY": "sk-...",
    "OPENAI_MODEL": "gpt-4o",
    "OPENAI_BASE_URL": "",     // optional
    "DEEPGRAM_API_KEY": "...",
    "TTS_PROVIDER": "deepgram",
    "DEEPGRAM_TTS_VOICE": "aura-2-helena-en"
  }
}

→ 201
{
  "id": "a1b2c3d4",
  "name": "interview-bot",
  "port": 7860,
  "status": "running",
  "client_url": "http://host:7860/client",
  "created_at": "2026-06-04T12:00:00+00:00"
}
```

### List agents

```
GET /agents

→ 200
[
  { "id": "a1b2c3d4", "name": "interview-bot", "port": 7860, "status": "running", "created_at": "..." },
  { "id": "e5f6a7b8", "name": "sales-bot",     "port": 7861, "status": "inactive", "created_at": "..." }
]
```

### Get agent

```
GET /agents/{id}

→ 200
{
  "id": "a1b2c3d4",
  "name": "interview-bot",
  "port": 7860,
  "flow_api_port": 18000,
  "status": "running",
  "client_url": "http://host:7860/client",
  "created_at": "..."
}
```

### Update flow (no restart required)

```
PUT /agents/{id}/flow
Content-Type: application/json

{ "flow_code": "..." }

→ 200  { "status": "ok", "message": "Flow updated — takes effect on next client connection" }
```

### Activate (start stopped agent)

```
PUT /agents/{id}/activate

→ 200  { "status": "ok", "port": 7860, "client_url": "http://host:7860/client" }
```

### Deactivate (stop, keep config/flow)

```
PUT /agents/{id}/deactivate

→ 200  { "status": "ok", "message": "Agent stopped" }
```

### Delete agent permanently

```
DELETE /agents/{id}

→ 200  { "status": "ok" }
```

---

## Port Scheme

| Resource | Port range |
|----------|-----------|
| Manager admin API | `MANAGER_PORT` (default 8080) |
| Agent WebRTC / client UI | `AGENT_BASE_PORT + N` (7860, 7861, ...) |
| Agent sidecar flow-API | `FLOW_API_BASE_PORT + N` (18000, 18001, ...) |

Clients connect to the agent's `client_url` (e.g., `http://host:7860/client`) for the WebRTC session.

---

## Config keys each agent supports

Pass these in the `config` object when creating an agent:

| Key | Required | Description |
|-----|----------|-------------|
| `OPENAI_API_KEY` | yes | OpenAI API key |
| `OPENAI_MODEL` | no | Model override (e.g., `gpt-4o`) |
| `OPENAI_BASE_URL` | no | Custom OpenAI-compatible endpoint |
| `DEEPGRAM_API_KEY` | yes | Deepgram STT/TTS key |
| `TTS_PROVIDER` | no | `deepgram` (default) or `cartesia` |
| `DEEPGRAM_TTS_VOICE` | no | Deepgram voice name |
| `CARTESIA_API_KEY` | no | Cartesia TTS key (if TTS_PROVIDER=cartesia) |
| `CARTESIA_TTS_VOICE` | no | Cartesia voice UUID |

---

## Data persistence

`agents/registry.json` is written atomically (write-to-temp, then rename) on every state change. On manager restart, agents with `status: "running"` are automatically re-spawned. Agents with `status: "inactive"` remain inactive until explicitly activated.

---

## Security note

`POST /agents` and `PUT /agents/{id}/flow` accept arbitrary Python code that gets executed as a subprocess. In production, protect the manager API with an API key header, IP allowlist, or auth middleware before exposing it publicly.

---

## Backwards compatibility

Running `python bot.py` directly (without the manager) still works exactly as before — `FLOW_PATH` defaults to `flow.py` in the same directory.
