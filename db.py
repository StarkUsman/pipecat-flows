"""
Postgres persistence for the agent manager.

Agents are still written to disk (agents/<id>/flow.py, config.json, registry.json)
so the bot subprocess can read them, but Postgres is the durable source of truth:
in a Docker/pod deployment the filesystem is ephemeral, so the `agents` table lets
the manager rebuild every agent after a crash or reschedule.

Connection params come from PG_* env vars (see env.example). Every write retries up
to PG_MAX_RETRIES times; if it still fails the error is re-raised so the caller can
abort the operation and crash the manager (loud failure → pod restarts and reloads
from the DB).
"""

import asyncio
import json
import os

import asyncpg
from loguru import logger

PG_HOST = os.environ.get("PG_HOST", "84.46.251.98")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "faraz")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "mypass")
PG_DATABASE = os.environ.get("PG_DATABASE", "faraz")
PG_MAX_RETRIES = int(os.environ.get("PG_MAX_RETRIES", "3"))
_RETRY_BACKOFF_SECONDS = 1.0

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS agents (
    id            TEXT PRIMARY KEY,
    name          TEXT        NOT NULL,
    port          INTEGER     NOT NULL,
    flow_api_port INTEGER     NOT NULL,
    flow_path     TEXT        NOT NULL,
    config        JSONB       NOT NULL DEFAULT '{}',
    flow_code     TEXT        NOT NULL DEFAULT '',
    status        TEXT        NOT NULL DEFAULT 'inactive',
    created_at    TEXT        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_pool: asyncpg.Pool | None = None


async def _with_retry(label: str, op):
    """Run an async DB op, retrying up to PG_MAX_RETRIES. Re-raise on final failure."""
    last_exc: Exception | None = None
    for attempt in range(1, PG_MAX_RETRIES + 1):
        try:
            return await op()
        except Exception as exc:  # noqa: BLE001 — any DB/connection error is retryable
            last_exc = exc
            logger.warning(f"DB {label} failed (attempt {attempt}/{PG_MAX_RETRIES}): {exc}")
            if attempt < PG_MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)
    logger.error(f"DB {label} failed after {PG_MAX_RETRIES} attempts — aborting")
    raise last_exc


async def init_db() -> None:
    """Create the connection pool and ensure the agents table exists.

    Raises (crashing the manager) if Postgres is unreachable after the retries.
    """
    global _pool

    async def _connect():
        return await asyncpg.create_pool(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            database=PG_DATABASE,
        )

    _pool = await _with_retry("connect", _connect)
    async with _pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE)
    logger.info(f"Postgres connected ({PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}); agents table ready")


async def upsert_agent(record, flow_code: str) -> None:
    """Insert or update an agent row (all columns)."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agents
                    (id, name, port, flow_api_port, flow_path, config, flow_code, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    port = EXCLUDED.port,
                    flow_api_port = EXCLUDED.flow_api_port,
                    flow_path = EXCLUDED.flow_path,
                    config = EXCLUDED.config,
                    flow_code = EXCLUDED.flow_code,
                    status = EXCLUDED.status,
                    updated_at = now()
                """,
                record.id,
                record.name,
                record.port,
                record.flow_api_port,
                record.flow_path,
                json.dumps(record.config),
                flow_code,
                record.status,
                record.created_at,
            )

    await _with_retry(f"upsert_agent {record.id}", _op)


async def update_status(agent_id: str, status: str) -> None:
    """Update just the status of an agent."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                "UPDATE agents SET status = $2, updated_at = now() WHERE id = $1",
                agent_id,
                status,
            )

    await _with_retry(f"update_status {agent_id}", _op)


async def update_flow(agent_id: str, flow_code: str, flow_path: str) -> None:
    """Update just the flow code / path of an agent."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                "UPDATE agents SET flow_code = $2, flow_path = $3, updated_at = now() WHERE id = $1",
                agent_id,
                flow_code,
                flow_path,
            )

    await _with_retry(f"update_flow {agent_id}", _op)


async def delete_agent(agent_id: str) -> None:
    """Permanently remove an agent row."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute("DELETE FROM agents WHERE id = $1", agent_id)

    await _with_retry(f"delete_agent {agent_id}", _op)


async def load_all() -> list[dict]:
    """Return every agent row as a dict (config decoded to a dict)."""

    async def _op():
        async with _pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM agents")

    rows = await _with_retry("load_all", _op)
    result: list[dict] = []
    for row in rows:
        d = dict(row)
        config = d.get("config")
        d["config"] = json.loads(config) if isinstance(config, str) else (config or {})
        result.append(d)
    return result
