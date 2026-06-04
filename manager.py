"""
Dynamic multi-agent manager for pipecat-flows.

Run with: python manager.py

Agents are created, activated, deactivated, and deleted at runtime via REST API.
Each agent runs as an isolated bot.py subprocess with its own flow file,
API keys, and WebRTC port.

Environment variables:
  MANAGER_PORT       — Admin API port (default: 8080)
  AGENT_BASE_PORT    — First WebRTC port for agents (default: 7860)
  FLOW_API_BASE_PORT — First sidecar flow-API port for agents (default: 18000)

API:
  POST   /agents                 — create + start a new agent
  GET    /agents                 — list all agents
  GET    /agents/{id}            — get single agent details
  PUT    /agents/{id}/flow       — hot-update an agent's flow code
  PUT    /agents/{id}/activate   — start a stopped agent
  PUT    /agents/{id}/deactivate — stop a running agent (keep config)
  DELETE /agents/{id}            — permanently remove an agent
"""

import asyncio
import json
import os
import shutil
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web
from loguru import logger

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


_registry: dict[str, AgentRecord] = {}
_processes: dict[str, asyncio.subprocess.Process] = {}
_used_ports: set[int] = set()


# ── Persistence ───────────────────────────────────────────────────────────────

def _save_registry() -> None:
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    data = {aid: asdict(rec) for aid, rec in _registry.items()}
    tmp = REGISTRY_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(REGISTRY_FILE)


def _load_registry() -> None:
    if not REGISTRY_FILE.exists():
        return
    data = json.loads(REGISTRY_FILE.read_text())
    for agent_id, d in data.items():
        _registry[agent_id] = AgentRecord(**d)
        _used_ports.add(d["port"])


# ── Port management ───────────────────────────────────────────────────────────

def _alloc_ports() -> tuple[int, int]:
    """Return next free (webrtc_port, flow_api_port) pair."""
    port = AGENT_BASE_PORT
    while port in _used_ports:
        port += 1
    offset = port - AGENT_BASE_PORT
    return port, FLOW_API_BASE_PORT + offset


# ── Subprocess management ─────────────────────────────────────────────────────

async def _spawn(record: AgentRecord) -> None:
    env = {
        **os.environ,
        **record.config,
        "FLOW_PATH": record.flow_path,
        "FLOW_API_PORT": str(record.flow_api_port),
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(BASE_DIR / "bot.py"),
        "--port", str(record.port),
        env=env,
        cwd=str(BASE_DIR),
    )
    _processes[record.id] = proc
    record.status = "running"
    _save_registry()
    logger.info(f"Agent '{record.name}' ({record.id}) started — PID {proc.pid}, port {record.port}")


async def _terminate(record: AgentRecord) -> None:
    proc = _processes.pop(record.id, None)
    if proc and proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
    record.status = "inactive"
    _save_registry()
    logger.info(f"Agent '{record.name}' ({record.id}) stopped")


# ── Flow code helpers ─────────────────────────────────────────────────────────

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


def _live_status(record: AgentRecord) -> str:
    proc = _processes.get(record.id)
    if record.status == "running" and proc and proc.returncode is not None:
        record.status = "inactive"
        _save_registry()
    return record.status


# ── Route handlers ────────────────────────────────────────────────────────────

async def handle_create_agent(request: web.Request) -> web.Response:
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

    try:
        flow_code = _prepare_flow_code(flow_code)
    except SyntaxError as exc:
        return web.json_response({"error": f"Syntax error in flow_code: {exc}"}, status=400)

    agent_id = uuid.uuid4().hex[:8]
    webrtc_port, flow_api_port = _alloc_ports()
    _used_ports.add(webrtc_port)

    agent_dir = AGENTS_DIR / agent_id
    agent_dir.mkdir(parents=True, exist_ok=True)
    flow_path = str(agent_dir / "flow.py")
    Path(flow_path).write_text(flow_code)
    (agent_dir / "config.json").write_text(
        json.dumps({"name": name, "config": config}, indent=2)
    )

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
    _registry[agent_id] = record

    await _spawn(record)

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


async def handle_list_agents(request: web.Request) -> web.Response:
    return web.json_response(
        [
            {
                "id": r.id,
                "name": r.name,
                "port": r.port,
                "status": _live_status(r),
                "created_at": r.created_at,
            }
            for r in _registry.values()
        ]
    )


async def handle_get_agent(request: web.Request) -> web.Response:
    record = _registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)
    host = _agent_host(request)
    return web.json_response(
        {
            "id": record.id,
            "name": record.name,
            "port": record.port,
            "flow_api_port": record.flow_api_port,
            "status": _live_status(record),
            "client_url": f"http://{host}:{record.port}/client",
            "created_at": record.created_at,
        }
    )


async def handle_update_flow(request: web.Request) -> web.Response:
    record = _registry.get(request.match_info["id"])
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

    Path(record.flow_path).write_text(flow_code)
    logger.info(f"Flow updated for agent '{record.name}' ({record.id})")
    return web.json_response(
        {"status": "ok", "message": "Flow updated — takes effect on next client connection"}
    )


async def handle_activate(request: web.Request) -> web.Response:
    record = _registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    proc = _processes.get(record.id)
    if record.status == "running" and proc and proc.returncode is None:
        return web.json_response({"error": "Agent is already running"}, status=409)

    await _spawn(record)
    host = _agent_host(request)
    return web.json_response(
        {"status": "ok", "port": record.port, "client_url": f"http://{host}:{record.port}/client"}
    )


async def handle_deactivate(request: web.Request) -> web.Response:
    record = _registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    if _live_status(record) == "inactive":
        return web.json_response({"error": "Agent is already inactive"}, status=409)

    await _terminate(record)
    return web.json_response({"status": "ok", "message": "Agent stopped"})


async def handle_delete_agent(request: web.Request) -> web.Response:
    record = _registry.get(request.match_info["id"])
    if not record:
        return web.json_response({"error": "Not found"}, status=404)

    if record.status == "running":
        await _terminate(record)

    _used_ports.discard(record.port)
    del _registry[record.id]

    agent_dir = AGENTS_DIR / record.id
    if agent_dir.exists():
        shutil.rmtree(agent_dir)

    _save_registry()
    logger.info(f"Agent '{record.name}' ({record.id}) deleted")
    return web.json_response({"status": "ok"})


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    _load_registry()

    # Re-spawn agents that were running before the manager was stopped
    for record in list(_registry.values()):
        if record.status == "running":
            logger.info(f"Re-spawning agent '{record.name}' ({record.id}) from registry")
            await _spawn(record)

    app = web.Application(middlewares=[cors_middleware])
    app.router.add_post("/agents", handle_create_agent)
    app.router.add_get("/agents", handle_list_agents)
    app.router.add_get("/agents/{id}", handle_get_agent)
    app.router.add_put("/agents/{id}/flow", handle_update_flow)
    app.router.add_put("/agents/{id}/activate", handle_activate)
    app.router.add_put("/agents/{id}/deactivate", handle_deactivate)
    app.router.add_delete("/agents/{id}", handle_delete_agent)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", MANAGER_PORT)
    await site.start()

    logger.info(f"Manager API listening on :{MANAGER_PORT}")
    logger.info(f"Agent WebRTC ports start at {AGENT_BASE_PORT}")
    logger.info(f"Agent sidecar API ports start at {FLOW_API_BASE_PORT}")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
