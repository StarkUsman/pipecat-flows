"""
Dynamic multi-agent manager for pipecat-flows.

Run with: python manager.py

Agents are created, activated, deactivated, and deleted at runtime via REST API.
Each agent runs as an isolated subprocess with its own flow file, API keys, and
WebRTC port.

Two agent flavours share this manager (same process/port and port pool), each
with its own registry, Postgres tables, bot script, and route family:
  - regular STT→LLM→TTS agents:  bot.py,     /agents,     /providers
  - speech-to-speech agents:     sts_bot.py,  /STS/agents, /STS/providers

Environment variables:
  MANAGER_PORT       — Admin API port (default: 8080)
  AGENT_BASE_PORT    — First WebRTC port for agents (default: 7860)
  FLOW_API_BASE_PORT — First sidecar flow-API port for agents (default: 18000)

API (mirrored under both /agents and /STS/agents):
  POST   {prefix}                 — create + start a new agent
  GET    {prefix}                 — list all agents
  GET    {prefix}/stats           — aggregate stats across all agents
  GET    {prefix}/{id}            — get single agent details
  GET    {prefix}/{id}/stats      — per-agent stats + recent sessions
  GET    {prefix}/{id}/flow       — get an agent's current flow code
  PUT    {prefix}/{id}/flow       — hot-update an agent's flow code
  PUT    {prefix}/{id}/config     — hot-update an agent's config (full replace)
  PUT    {prefix}/{id}/activate   — start a stopped agent
  PUT    {prefix}/{id}/deactivate — stop a running agent (keep config)
  DELETE {prefix}/{id}            — permanently remove an agent
  GET    /providers | /STS/providers — provider catalog for that flavour
"""

import asyncio
import json
import os
import shutil
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Callable

from aiohttp import web
from loguru import logger

import db
import services


def _json_dumps(obj) -> str:
    """json.dumps that tolerates datetime/Decimal values returned from Postgres."""
    return json.dumps(obj, default=str)

BASE_DIR = Path(__file__).parent
AGENTS_DIR = BASE_DIR / "agents"
REGISTRY_FILE = AGENTS_DIR / "registry.json"

MANAGER_PORT = int(os.environ.get("MANAGER_PORT", "8080"))
AGENT_BASE_PORT = int(os.environ.get("AGENT_BASE_PORT", "7860"))
FLOW_API_BASE_PORT = int(os.environ.get("FLOW_API_BASE_PORT", "18000"))

_TYPING_NAMES = {"Any", "Optional", "Union", "List", "Dict", "Tuple", "Set", "Type", "Callable"}
_TYPING_HEADER = "from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union\n"


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class AgentRecord:
    id: str
    name: str
    port: int
    flow_api_port: int
    flow_path: str
    config: dict
    status: str        # "running" | "inactive"
    created_at: str


@dataclass
class AgentKind:
    """Bundles everything that differs between the regular and speech-to-speech
    agent flavours so the route handlers can stay generic. The process/port pools
    are shared (ids are globally-unique uuids); only the registry, DB table, bot
    script, provider validation, catalog, and route prefix differ."""

    name: str                       # "agent" | "s2s" (log/label only)
    bot_script: str                 # bot.py | sts_bot.py
    agents_table: str               # db.AGENTS_TABLE | db.STS_AGENTS_TABLE
    registry_file: Path
    route_prefix: str               # "/agents" | "/STS/agents"
    provider_selectors: dict        # modality -> env selector key, for validation
    provider_catalog: object        # PROVIDER_CATALOG | S2S_PROVIDER_CATALOG
    get_stats_all: Callable         # db.get_stats_all | db.get_sts_stats_all
    get_stats_for_agent: Callable   # db.get_stats_for_agent | db.get_sts_stats_for_agent
    registry: dict = field(default_factory=dict)


_processes: dict[str, asyncio.subprocess.Process] = {}
_used_ports: set[int] = set()
_bg_tasks: set[asyncio.Task] = set()


# Regular STT→LLM→TTS agents.
AGENT_KIND = AgentKind(
    name="agent",
    bot_script="bot.py",
    agents_table=db.AGENTS_TABLE,
    registry_file=AGENTS_DIR / "registry.json",
    route_prefix="/agents",
    provider_selectors={"stt": "STT_PROVIDER", "llm": "LLM_PROVIDER", "tts": "TTS_PROVIDER"},
    provider_catalog=services.PROVIDER_CATALOG,
    get_stats_all=db.get_stats_all,
    get_stats_for_agent=db.get_stats_for_agent,
)

# Speech-to-speech (realtime) agents.
STS_KIND = AgentKind(
    name="s2s",
    bot_script="sts_bot.py",
    agents_table=db.STS_AGENTS_TABLE,
    registry_file=AGENTS_DIR / "sts_registry.json",
    route_prefix="/STS/agents",
    provider_selectors={"s2s": "STS_PROVIDER"},
    provider_catalog=services.S2S_PROVIDER_CATALOG,
    get_stats_all=db.get_sts_stats_all,
    get_stats_for_agent=db.get_sts_stats_for_agent,
)

_KINDS = (AGENT_KIND, STS_KIND)


# ── Persistence ───────────────────────────────────────────────────────────────

def _save_registry(kind: AgentKind) -> None:
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    data = {aid: asdict(rec) for aid, rec in kind.registry.items()}
    tmp = kind.registry_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(kind.registry_file)


def _load_registry(kind: AgentKind) -> None:
    if not kind.registry_file.exists():
        return
    data = json.loads(kind.registry_file.read_text())
    for agent_id, d in data.items():
        kind.registry[agent_id] = AgentRecord(**d)
        _used_ports.add(d["port"])


def _write_agent_files(record: AgentRecord, flow_code: str) -> None:
    """Recreate the on-disk flow.py + config.json for an agent from DB data."""
    agent_dir = AGENTS_DIR / record.id
    agent_dir.mkdir(parents=True, exist_ok=True)
    Path(record.flow_path).write_text(flow_code)
    (agent_dir / "config.json").write_text(
        json.dumps({"name": record.name, "config": record.config}, indent=2)
    )


async def _restore_from_db(kind: AgentKind) -> None:
    """Make Postgres authoritative on startup, per agent kind.

    - If the DB table has agents, rebuild the in-memory registry and overwrite the
      local flow.py / config.json / registry file from the DB rows (DB wins).
    - If the table is empty but a local registry exists, seed Postgres from it
      (one-time migration so already-created file-based agents aren't lost).
    """
    rows = await db.load_all(table=kind.agents_table)

    if rows:
        for d in rows:
            record = AgentRecord(
                id=d["id"],
                name=d["name"],
                port=d["port"],
                flow_api_port=d["flow_api_port"],
                # Recompute the path so it's valid for this container's filesystem.
                flow_path=str(AGENTS_DIR / d["id"] / "flow.py"),
                config=d["config"],
                status=d["status"],
                created_at=d["created_at"],
            )
            kind.registry[record.id] = record
            _used_ports.add(record.port)
            _write_agent_files(record, d.get("flow_code") or "")
        _save_registry(kind)
        logger.info(f"Restored {len(rows)} {kind.name} agent(s) from Postgres (DB authoritative)")
        return

    # Empty table → seed from any existing local files.
    _load_registry(kind)
    if kind.registry:
        logger.info(f"Postgres empty — seeding {len(kind.registry)} {kind.name} agent(s) from local files")
        for record in kind.registry.values():
            try:
                flow_code = Path(record.flow_path).read_text()
            except FileNotFoundError:
                flow_code = ""
            await db.upsert_agent(record, flow_code, table=kind.agents_table)


# ── Port management ───────────────────────────────────────────────────────────

def _alloc_ports() -> tuple[int, int]:
    """Return next free (webrtc_port, flow_api_port) pair."""
    port = AGENT_BASE_PORT
    while port in _used_ports:
        port += 1
    offset = port - AGENT_BASE_PORT
    return port, FLOW_API_BASE_PORT + offset


# ── Subprocess management ─────────────────────────────────────────────────────

async def _spawn(record: AgentRecord, kind: AgentKind) -> None:
    env = {
        **os.environ,
        **record.config,
        "FLOW_PATH": record.flow_path,
        "FLOW_API_PORT": str(record.flow_api_port),
        "AGENT_ID": record.id,
        "AGENT_NAME": record.name,
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(BASE_DIR / kind.bot_script),
        "--port", str(record.port),
        env=env,
        cwd=str(BASE_DIR),
    )
    _processes[record.id] = proc
    record.status = "running"
    _save_registry(kind)
    await db.update_status(record.id, record.status, table=kind.agents_table)
    logger.info(f"{kind.name} agent '{record.name}' ({record.id}) started — PID {proc.pid}, port {record.port}")


async def _terminate(record: AgentRecord, kind: AgentKind) -> None:
    proc = _processes.pop(record.id, None)
    if proc and proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
    record.status = "inactive"
    _save_registry(kind)
    await db.update_status(record.id, record.status, table=kind.agents_table)
    logger.info(f"{kind.name} agent '{record.name}' ({record.id}) stopped")


# ── Flow code helpers ─────────────────────────────────────────────────────────

def _validate_providers(config: dict, kind: AgentKind) -> str | None:
    """Return an error message if config names an unknown provider for this kind."""
    for modality, key in kind.provider_selectors.items():
        value = config.get(key)
        if value is None or value == "":
            continue
        provider = str(value).strip().lower()
        known = services.KNOWN_PROVIDERS[modality]
        if provider not in known:
            return f"Unknown {key} '{value}'. Supported: {', '.join(known)}"
    return None


def _prepare_flow_code(code: str) -> str:
    """Auto-inject typing imports and validate syntax. Returns processed code."""
    needs_typing = any(name in code for name in _TYPING_NAMES)
    has_typing_import = "from typing import" in code or "import typing" in code
    if needs_typing and not has_typing_import:
        code = _TYPING_HEADER + code
    compile(code, "flow.py", "exec")   # raises SyntaxError if invalid
    return code


# ── CORS middleware ───────────────────────────────────────────────────────────

def _cors_headers(request: web.Request) -> dict:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": request.headers.get("Access-Control-Request-Headers") or "*",
        "Access-Control-Max-Age": "86400",
    }


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=_cors_headers(request))
    response = await handler(request)
    response.headers.update(_cors_headers(request))
    return response


# ── Route helpers ─────────────────────────────────────────────────────────────

def _agent_host(request: web.Request) -> str:
    return request.host.split(":")[0]


def _live_status(record: AgentRecord, kind: AgentKind) -> str:
    proc = _processes.get(record.id)
    if record.status == "running" and proc and proc.returncode is not None:
        record.status = "inactive"
        _save_registry(kind)
        # Best-effort DB sync; authoritative status is re-derived on the next op/restart.
        task = asyncio.create_task(db.update_status(record.id, "inactive", table=kind.agents_table))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)
    return record.status


# ── Route handlers ────────────────────────────────────────────────────────────
#
# Every handler takes the AgentKind it serves as its last argument; the routes are
# registered with functools.partial(handler, kind=...) so the same code backs both
# the /agents and /STS/agents endpoint families.

async def handle_create_agent(request: web.Request, kind: AgentKind) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    name = (body.get("name") or "").strip()
    flow_code = (body.get("flow_code") or "").strip()
    config = body.get("config") or {}

    if not name:
        return web.json_response({"error": "Missing 'name'"}, status=400)
    if not flow_code:
        return web.json_response({"error": "Missing 'flow_code'"}, status=400)
    if not isinstance(config, dict):
        return web.json_response({"error": "'config' must be an object"}, status=400)

    provider_error = _validate_providers(config, kind)
    if provider_error:
        return web.json_response({"error": provider_error}, status=400)

    try:
        flow_code = _prepare_flow_code(flow_code)
    except SyntaxError as exc:
        return web.json_response({"error": f"Syntax error in flow_code: {exc}"}, status=400)

    agent_id = uuid.uuid4().hex[:8]
    webrtc_port, flow_api_port = _alloc_ports()

    agent_dir = AGENTS_DIR / agent_id
    flow_path = str(agent_dir / "flow.py")

    record = AgentRecord(
        id=agent_id,
        name=name,
        port=webrtc_port,
        flow_api_port=flow_api_port,
        flow_path=flow_path,
        config=config,
        status="inactive",
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    # Persist to Postgres FIRST — if this fails (after retries) the agent is not
    # saved: no files, no port reservation, no registry entry. Errors propagate.
    await db.upsert_agent(record, flow_code, table=kind.agents_table)

    _used_ports.add(webrtc_port)
    agent_dir.mkdir(parents=True, exist_ok=True)
    Path(flow_path).write_text(flow_code)
    (agent_dir / "config.json").write_text(
        json.dumps({"name": name, "config": config}, indent=2)
    )
    kind.registry[agent_id] = record

    await _spawn(record, kind)

    host = _agent_host(request)
    return web.json_response(
        {
            "id": agent_id,
            "name": name,
            "port": webrtc_port,
            "status": "running",
            "client_url": f"http://{host}:{webrtc_port}/client",
            "created_at": record.created_at,
        },
        status=201,
    )


async def handle_list_agents(request: web.Request, kind: AgentKind) -> web.Response:
    return web.json_response(
        [
            {
                "id": r.id,
                "name": r.name,
                "port": r.port,
                "status": _live_status(r, kind),
                "created_at": r.created_at,
            }
            for r in kind.registry.values()
        ]
    )


async def handle_get_agent(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)
    host = _agent_host(request)
    return web.json_response(
        {
            "id": record.id,
            "name": record.name,
            "port": record.port,
            "flow_api_port": record.flow_api_port,
            "status": _live_status(record, kind),
            "client_url": f"http://{host}:{record.port}/client",
            "created_at": record.created_at,
        }
    )


async def handle_get_flow(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)
    try:
        flow_code = Path(record.flow_path).read_text()
    except FileNotFoundError:
        flow_code = ""
    return web.json_response({"flow_code": flow_code})


async def handle_get_all_stats(request: web.Request, kind: AgentKind) -> web.Response:
    return web.json_response(await kind.get_stats_all(), dumps=_json_dumps)


async def handle_get_agent_stats(request: web.Request, kind: AgentKind) -> web.Response:
    agent_id = request.match_info["id"]
    stats = await kind.get_stats_for_agent(agent_id)
    if agent_id not in kind.registry and not stats.get("sessions"):
        return web.json_response({"error": "Not found"}, status=404)
    return web.json_response(stats, dumps=_json_dumps)


async def handle_update_flow(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    flow_code = (body.get("flow_code") or "").strip()
    if not flow_code:
        return web.json_response({"error": "Missing 'flow_code'"}, status=400)

    try:
        flow_code = _prepare_flow_code(flow_code)
    except SyntaxError as exc:
        return web.json_response({"error": f"Syntax error in flow_code: {exc}"}, status=400)

    await db.update_flow(record.id, flow_code, record.flow_path, table=kind.agents_table)
    Path(record.flow_path).write_text(flow_code)
    logger.info(f"Flow updated for {kind.name} agent '{record.name}' ({record.id})")
    return web.json_response(
        {"status": "ok", "message": "Flow updated — takes effect on next client connection"}
    )


async def handle_update_config(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    config = body.get("config")
    if not isinstance(config, dict):
        return web.json_response({"error": "'config' must be an object"}, status=400)

    provider_error = _validate_providers(config, kind)
    if provider_error:
        return web.json_response({"error": provider_error}, status=400)

    # Persist to Postgres FIRST — if this fails (after retries) the error
    # propagates and the agent keeps its previous config on disk and in memory.
    await db.update_config(record.id, config, table=kind.agents_table)

    record.config = config
    agent_dir = AGENTS_DIR / record.id
    agent_dir.mkdir(parents=True, exist_ok=True)
    (agent_dir / "config.json").write_text(
        json.dumps({"name": record.name, "config": config}, indent=2)
    )
    _save_registry(kind)
    logger.info(f"Config updated for {kind.name} agent '{record.name}' ({record.id})")
    return web.json_response(
        {"status": "ok", "message": "Config updated — takes effect on next client connection"}
    )


async def handle_activate(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    proc = _processes.get(record.id)
    if record.status == "running" and proc and proc.returncode is None:
        return web.json_response({"error": "Agent is already running"}, status=409)

    await _spawn(record, kind)
    host = _agent_host(request)
    return web.json_response(
        {"status": "ok", "port": record.port, "client_url": f"http://{host}:{record.port}/client"}
    )


async def handle_deactivate(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    if _live_status(record, kind) == "inactive":
        return web.json_response({"error": "Agent is already inactive"}, status=409)

    await _terminate(record, kind)
    return web.json_response({"status": "ok", "message": "Agent stopped"})


async def handle_delete_agent(request: web.Request, kind: AgentKind) -> web.Response:
    record = kind.registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    # Remove from Postgres first; if this fails (after retries) the error
    # propagates and the agent stays intact on disk and in the registry.
    await db.delete_agent(record.id, table=kind.agents_table)

    if record.status == "running":
        await _terminate(record, kind)

    _used_ports.discard(record.port)
    del kind.registry[record.id]

    agent_dir = AGENTS_DIR / record.id
    if agent_dir.exists():
        shutil.rmtree(agent_dir)

    _save_registry(kind)
    logger.info(f"{kind.name} agent '{record.name}' ({record.id}) deleted")
    return web.json_response({"status": "ok"})


async def handle_get_providers(request: web.Request, kind: AgentKind) -> web.Response:
    """Return the provider catalog for this kind (providers, models, voices, key env)."""
    return web.json_response(kind.provider_catalog)


# ── Main ──────────────────────────────────────────────────────────────────────

def _register_kind_routes(app: web.Application, kind: AgentKind, providers_path: str) -> None:
    """Register the full CRUD/stats route family for one agent kind."""
    p = kind.route_prefix
    app.router.add_get(providers_path, partial(handle_get_providers, kind=kind))
    app.router.add_post(p, partial(handle_create_agent, kind=kind))
    app.router.add_get(p, partial(handle_list_agents, kind=kind))
    # Register the static {prefix}/stats route BEFORE {prefix}/{id} so it isn't
    # captured as id="stats".
    app.router.add_get(f"{p}/stats", partial(handle_get_all_stats, kind=kind))
    app.router.add_get(f"{p}/{{id}}/stats", partial(handle_get_agent_stats, kind=kind))
    app.router.add_get(f"{p}/{{id}}", partial(handle_get_agent, kind=kind))
    app.router.add_get(f"{p}/{{id}}/flow", partial(handle_get_flow, kind=kind))
    app.router.add_put(f"{p}/{{id}}/flow", partial(handle_update_flow, kind=kind))
    app.router.add_put(f"{p}/{{id}}/config", partial(handle_update_config, kind=kind))
    app.router.add_put(f"{p}/{{id}}/activate", partial(handle_activate, kind=kind))
    app.router.add_put(f"{p}/{{id}}/deactivate", partial(handle_deactivate, kind=kind))
    app.router.add_delete(f"{p}/{{id}}", partial(handle_delete_agent, kind=kind))


async def main():
    await db.init_db()

    for kind in _KINDS:
        await _restore_from_db(kind)
        # Re-spawn agents that were running before the manager was stopped
        for record in list(kind.registry.values()):
            if record.status == "running":
                logger.info(f"Re-spawning {kind.name} agent '{record.name}' ({record.id}) from registry")
                await _spawn(record, kind)

    app = web.Application(middlewares=[cors_middleware])
    _register_kind_routes(app, AGENT_KIND, providers_path="/providers")
    _register_kind_routes(app, STS_KIND, providers_path="/STS/providers")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", MANAGER_PORT)
    await site.start()

    logger.info(f"Manager API listening on :{MANAGER_PORT}")
    logger.info(f"  regular agents:        /agents, /providers")
    logger.info(f"  speech-to-speech (S2S): /STS/agents, /STS/providers")
    logger.info(f"Agent WebRTC ports start at {AGENT_BASE_PORT}")
    logger.info(f"Agent sidecar API ports start at {FLOW_API_BASE_PORT}")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
