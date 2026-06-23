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

# Table names — the agent CRUD/stats helpers are parameterized by these so the
# speech-to-speech agents get their own tables without duplicating the SQL.
AGENTS_TABLE = "agents"
STS_AGENTS_TABLE = "sts_agents"
STATS_TABLE = "agent_stats"
STS_STATS_TABLE = "sts_agent_stats"


def _create_agents_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    id            TEXT PRIMARY KEY,
    name          TEXT        NOT NULL,
    port          INTEGER     NOT NULL,
    flow_api_port INTEGER     NOT NULL,
    flow_path     TEXT        NOT NULL,
    config        JSONB       NOT NULL DEFAULT '{{}}',
    flow_code     TEXT        NOT NULL DEFAULT '',
    status        TEXT        NOT NULL DEFAULT 'inactive',
    created_at    TEXT        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


_CREATE_TABLE = _create_agents_ddl(AGENTS_TABLE)
# Speech-to-speech agents — identical schema, separate table.
_CREATE_STS_TABLE = _create_agents_ddl(STS_AGENTS_TABLE)

# Per-conversation analytics. agent_id is plain text (no FK) so stats survive
# agent deletion.
_CREATE_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS agent_stats (
    id                BIGSERIAL PRIMARY KEY,
    session_id        TEXT NOT NULL,
    agent_id          TEXT NOT NULL,
    agent_name        TEXT,
    started_at        TIMESTAMPTZ NOT NULL,
    ended_at          TIMESTAMPTZ,
    duration_seconds  DOUBLE PRECISION,
    status            TEXT NOT NULL DEFAULT 'unknown',
    last_node         TEXT,
    turns             INTEGER DEFAULT 0,
    prompt_tokens     BIGINT DEFAULT 0,
    completion_tokens BIGINT DEFAULT 0,
    total_tokens      BIGINT DEFAULT 0,
    tts_characters    BIGINT DEFAULT 0,
    avg_llm_ttfb_ms   DOUBLE PRECISION,
    avg_tts_ttfb_ms   DOUBLE PRECISION,
    error             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_agent_stats_agent_id ON agent_stats(agent_id);
"""

# Speech-to-speech analytics. No STT/TTS stage, so the TTS-specific columns
# (tts_characters / avg_tts_ttfb_ms) are dropped; avg_llm_ttfb_ms captures the
# realtime model's response latency.
_CREATE_STS_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS sts_agent_stats (
    id                BIGSERIAL PRIMARY KEY,
    session_id        TEXT NOT NULL,
    agent_id          TEXT NOT NULL,
    agent_name        TEXT,
    started_at        TIMESTAMPTZ NOT NULL,
    ended_at          TIMESTAMPTZ,
    duration_seconds  DOUBLE PRECISION,
    status            TEXT NOT NULL DEFAULT 'unknown',
    last_node         TEXT,
    turns             INTEGER DEFAULT 0,
    prompt_tokens     BIGINT DEFAULT 0,
    completion_tokens BIGINT DEFAULT 0,
    total_tokens      BIGINT DEFAULT 0,
    avg_llm_ttfb_ms   DOUBLE PRECISION,
    error             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sts_agent_stats_agent_id ON sts_agent_stats(agent_id);
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
    """Create the connection pool and ensure the agents/agent_stats tables exist.

    Idempotent — safe to call more than once per process. Raises (crashing the
    caller) if Postgres is unreachable after the retries.
    """
    global _pool
    if _pool is not None:
        return

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
        await conn.execute(_CREATE_STATS_TABLE)
        await conn.execute(_CREATE_STS_TABLE)
        await conn.execute(_CREATE_STS_STATS_TABLE)
    logger.info(
        f"Postgres connected ({PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}); "
        "agents + agent_stats + sts_agents + sts_agent_stats tables ready"
    )


async def upsert_agent(record, flow_code: str, table: str = AGENTS_TABLE) -> None:
    """Insert or update an agent row (all columns) in ``table``."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {table}
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


async def update_status(agent_id: str, status: str, table: str = AGENTS_TABLE) -> None:
    """Update just the status of an agent."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {table} SET status = $2, updated_at = now() WHERE id = $1",
                agent_id,
                status,
            )

    await _with_retry(f"update_status {agent_id}", _op)


async def update_flow(agent_id: str, flow_code: str, flow_path: str, table: str = AGENTS_TABLE) -> None:
    """Update just the flow code / path of an agent."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {table} SET flow_code = $2, flow_path = $3, updated_at = now() WHERE id = $1",
                agent_id,
                flow_code,
                flow_path,
            )

    await _with_retry(f"update_flow {agent_id}", _op)


async def update_config(agent_id: str, config: dict, table: str = AGENTS_TABLE) -> None:
    """Update just the config (JSONB) of an agent."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {table} SET config = $2, updated_at = now() WHERE id = $1",
                agent_id,
                json.dumps(config),
            )

    await _with_retry(f"update_config {agent_id}", _op)


async def delete_agent(agent_id: str, table: str = AGENTS_TABLE) -> None:
    """Permanently remove an agent row."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {table} WHERE id = $1", agent_id)

    await _with_retry(f"delete_agent {agent_id}", _op)


async def load_all(table: str = AGENTS_TABLE) -> list[dict]:
    """Return every agent row as a dict (config decoded to a dict)."""

    async def _op():
        async with _pool.acquire() as conn:
            return await conn.fetch(f"SELECT * FROM {table}")

    rows = await _with_retry("load_all", _op)
    result: list[dict] = []
    for row in rows:
        d = dict(row)
        config = d.get("config")
        d["config"] = json.loads(config) if isinstance(config, str) else (config or {})
        result.append(d)
    return result


# ── Per-conversation stats ──────────────────────────────────────────────────────

async def insert_stats(row: dict) -> None:
    """Insert one conversation's stats row into agent_stats."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_stats
                    (session_id, agent_id, agent_name, started_at, ended_at,
                     duration_seconds, status, last_node, turns, prompt_tokens,
                     completion_tokens, total_tokens, tts_characters,
                     avg_llm_ttfb_ms, avg_tts_ttfb_ms, error)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                """,
                row.get("session_id"),
                row.get("agent_id"),
                row.get("agent_name"),
                row.get("started_at"),
                row.get("ended_at"),
                row.get("duration_seconds"),
                row.get("status", "unknown"),
                row.get("last_node"),
                row.get("turns", 0),
                row.get("prompt_tokens", 0),
                row.get("completion_tokens", 0),
                row.get("total_tokens", 0),
                row.get("tts_characters", 0),
                row.get("avg_llm_ttfb_ms"),
                row.get("avg_tts_ttfb_ms"),
                row.get("error"),
            )

    await _with_retry(f"insert_stats {row.get('agent_id')}", _op)


# Shared aggregate projection used by both stats read functions.
_STATS_AGG_COLUMNS = """
    agent_id,
    max(agent_name)                                       AS agent_name,
    count(*)                                              AS total_sessions,
    count(*) FILTER (WHERE status = 'completed')          AS completed_sessions,
    count(*) FILTER (WHERE status = 'disconnected')       AS disconnected_sessions,
    count(*) FILTER (WHERE status = 'failed')             AS failed_sessions,
    coalesce(sum(prompt_tokens), 0)::bigint               AS prompt_tokens,
    coalesce(sum(completion_tokens), 0)::bigint           AS completion_tokens,
    coalesce(sum(total_tokens), 0)::bigint                AS total_tokens,
    coalesce(sum(tts_characters), 0)::bigint              AS tts_characters,
    coalesce(sum(turns), 0)::bigint                       AS total_turns,
    avg(duration_seconds)                                 AS avg_duration_seconds,
    avg(avg_llm_ttfb_ms)                                  AS avg_llm_ttfb_ms,
    avg(avg_tts_ttfb_ms)                                  AS avg_tts_ttfb_ms,
    max(ended_at)                                         AS last_session_at
"""


async def get_stats_all() -> list[dict]:
    """Return one aggregate summary row per agent (across all stored sessions)."""

    async def _op():
        async with _pool.acquire() as conn:
            return await conn.fetch(
                f"SELECT {_STATS_AGG_COLUMNS} FROM agent_stats GROUP BY agent_id "
                "ORDER BY total_sessions DESC"
            )

    rows = await _with_retry("get_stats_all", _op)
    return [dict(r) for r in rows]


async def get_stats_for_agent(agent_id: str, limit: int = 50) -> dict:
    """Return an aggregate summary for one agent plus its most-recent sessions."""

    async def _op():
        async with _pool.acquire() as conn:
            summary = await conn.fetchrow(
                f"SELECT {_STATS_AGG_COLUMNS} FROM agent_stats WHERE agent_id = $1 "
                "GROUP BY agent_id",
                agent_id,
            )
            sessions = await conn.fetch(
                "SELECT * FROM agent_stats WHERE agent_id = $1 "
                "ORDER BY started_at DESC LIMIT $2",
                agent_id,
                limit,
            )
            return summary, sessions

    summary, sessions = await _with_retry(f"get_stats_for_agent {agent_id}", _op)
    return {
        "agent_id": agent_id,
        "summary": dict(summary) if summary else None,
        "sessions": [dict(s) for s in sessions],
    }


# ── Speech-to-speech (S2S) per-conversation stats ─────────────────────────────

async def insert_sts_stats(row: dict) -> None:
    """Insert one S2S conversation's stats row into sts_agent_stats."""

    async def _op():
        async with _pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {STS_STATS_TABLE}
                    (session_id, agent_id, agent_name, started_at, ended_at,
                     duration_seconds, status, last_node, turns, prompt_tokens,
                     completion_tokens, total_tokens, avg_llm_ttfb_ms, error)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                """,
                row.get("session_id"),
                row.get("agent_id"),
                row.get("agent_name"),
                row.get("started_at"),
                row.get("ended_at"),
                row.get("duration_seconds"),
                row.get("status", "unknown"),
                row.get("last_node"),
                row.get("turns", 0),
                row.get("prompt_tokens", 0),
                row.get("completion_tokens", 0),
                row.get("total_tokens", 0),
                row.get("avg_llm_ttfb_ms"),
                row.get("error"),
            )

    await _with_retry(f"insert_sts_stats {row.get('agent_id')}", _op)


# Aggregate projection for S2S stats (no TTS columns).
_STS_STATS_AGG_COLUMNS = """
    agent_id,
    max(agent_name)                                       AS agent_name,
    count(*)                                              AS total_sessions,
    count(*) FILTER (WHERE status = 'completed')          AS completed_sessions,
    count(*) FILTER (WHERE status = 'disconnected')       AS disconnected_sessions,
    count(*) FILTER (WHERE status = 'failed')             AS failed_sessions,
    coalesce(sum(prompt_tokens), 0)::bigint               AS prompt_tokens,
    coalesce(sum(completion_tokens), 0)::bigint           AS completion_tokens,
    coalesce(sum(total_tokens), 0)::bigint                AS total_tokens,
    coalesce(sum(turns), 0)::bigint                       AS total_turns,
    avg(duration_seconds)                                 AS avg_duration_seconds,
    avg(avg_llm_ttfb_ms)                                  AS avg_llm_ttfb_ms,
    max(ended_at)                                         AS last_session_at
"""


async def get_sts_stats_all() -> list[dict]:
    """Return one aggregate summary row per S2S agent (across all stored sessions)."""

    async def _op():
        async with _pool.acquire() as conn:
            return await conn.fetch(
                f"SELECT {_STS_STATS_AGG_COLUMNS} FROM {STS_STATS_TABLE} GROUP BY agent_id "
                "ORDER BY total_sessions DESC"
            )

    rows = await _with_retry("get_sts_stats_all", _op)
    return [dict(r) for r in rows]


async def get_sts_stats_for_agent(agent_id: str, limit: int = 50) -> dict:
    """Return an aggregate summary for one S2S agent plus its most-recent sessions."""

    async def _op():
        async with _pool.acquire() as conn:
            summary = await conn.fetchrow(
                f"SELECT {_STS_STATS_AGG_COLUMNS} FROM {STS_STATS_TABLE} WHERE agent_id = $1 "
                "GROUP BY agent_id",
                agent_id,
            )
            sessions = await conn.fetch(
                f"SELECT * FROM {STS_STATS_TABLE} WHERE agent_id = $1 "
                "ORDER BY started_at DESC LIMIT $2",
                agent_id,
                limit,
            )
            return summary, sessions

    summary, sessions = await _with_retry(f"get_sts_stats_for_agent {agent_id}", _op)
    return {
        "agent_id": agent_id,
        "summary": dict(summary) if summary else None,
        "sessions": [dict(s) for s in sessions],
    }
