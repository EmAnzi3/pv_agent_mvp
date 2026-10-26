from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .base import AgentFinding

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path(os.environ.get("WIND_AGENT_DB", ROOT / "data" / "wind_agent.sqlite"))


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS agent_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                planned_tasks INTEGER NOT NULL DEFAULT 0,
                findings INTEGER NOT NULL DEFAULT 0,
                changed_items INTEGER NOT NULL DEFAULT 0,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS raw_findings (
                agent_name TEXT NOT NULL,
                source_name TEXT NOT NULL,
                external_id TEXT NOT NULL,
                source_url TEXT,
                title TEXT,
                finding_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                last_run_id TEXT,
                PRIMARY KEY (agent_name, source_name, external_id)
            );

            CREATE TABLE IF NOT EXISTS finding_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                source_name TEXT NOT NULL,
                external_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                previous_hash TEXT,
                new_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_cursors (
                source_id TEXT PRIMARY KEY,
                cursor_value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metadata_json TEXT
            );

            CREATE TABLE IF NOT EXISTS watch_status (
                watch_id TEXT PRIMARY KEY,
                last_attempt TEXT,
                last_success TEXT,
                last_error TEXT,
                last_run_id TEXT,
                metadata_json TEXT
            );
            """
        )
        conn.commit()


def get_source_cursor(source_id: str, default: str | None = None) -> str | None:
    """Return a persistent collector cursor/high-water mark.

    Cursors are operational state only. They never affect canonical evidence or
    project promotion and are safe to discard/rebuild from a backfill run.
    """

    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT cursor_value FROM source_cursors WHERE source_id = ?",
            (source_id,),
        ).fetchone()
    return str(row["cursor_value"]) if row else default


def set_source_cursor(
    source_id: str,
    cursor_value: str | int,
    metadata: dict[str, Any] | None = None,
) -> None:
    init_db()
    now = datetime.now().isoformat(timespec="seconds")
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True, default=str)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO source_cursors (source_id, cursor_value, updated_at, metadata_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                cursor_value = excluded.cursor_value,
                updated_at = excluded.updated_at,
                metadata_json = excluded.metadata_json
            """,
            (source_id, str(cursor_value), now, metadata_json),
        )
        conn.commit()


def get_watch_status(watch_id: str) -> dict[str, Any] | None:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT watch_id, last_attempt, last_success, last_error, last_run_id, metadata_json
            FROM watch_status WHERE watch_id = ?
            """,
            (watch_id,),
        ).fetchone()
    if row is None:
        return None
    metadata = {}
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except json.JSONDecodeError:
        pass
    return {
        "watch_id": row["watch_id"],
        "last_attempt": row["last_attempt"],
        "last_success": row["last_success"],
        "last_error": row["last_error"],
        "last_run_id": row["last_run_id"],
        "metadata": metadata,
    }


def mark_watch_attempt(
    watch_id: str,
    run_id: str,
    *,
    success: bool,
    error: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Persist live execution state independently from source registries.

    Registry `last_checked` values document the audited baseline. This table is
    the runtime truth used by periodic execution, avoiding commits merely to
    advance monitoring timestamps.
    """

    init_db()
    now = datetime.now().isoformat(timespec="seconds")
    previous = get_watch_status(watch_id) or {}
    last_success = now if success else previous.get("last_success")
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True, default=str)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO watch_status (
                watch_id, last_attempt, last_success, last_error, last_run_id, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(watch_id) DO UPDATE SET
                last_attempt = excluded.last_attempt,
                last_success = excluded.last_success,
                last_error = excluded.last_error,
                last_run_id = excluded.last_run_id,
                metadata_json = excluded.metadata_json
            """,
            (
                watch_id,
                now,
                last_success,
                None if success else (error or "unknown error")[:2000],
                run_id,
                metadata_json,
            ),
        )
        conn.commit()


def begin_run(planned_tasks: int, note: str | None = None) -> str:
    init_db()
    run_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    now = datetime.now().isoformat(timespec="seconds")
    with _connect() as conn:
        conn.execute(
            "INSERT INTO agent_runs (run_id, started_at, planned_tasks, note) VALUES (?, ?, ?, ?)",
            (run_id, now, planned_tasks, note),
        )
        conn.commit()
    return run_id


def _finding_payload(finding: AgentFinding) -> dict[str, Any]:
    return {
        "external_id": finding.external_id,
        "source_name": finding.source_name,
        "source_url": finding.source_url,
        "title": finding.title,
        "finding_type": finding.finding_type,
        "payload": finding.payload,
    }


def _hash_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def upsert_finding(run_id: str, agent_name: str, finding: AgentFinding) -> str:
    """Persist a raw source finding and return new/changed/unchanged.

    This mirrors the PV Agent raw/history separation. It intentionally does not
    update docs/wind canonical JSON.
    """

    init_db()
    now = datetime.now().isoformat(timespec="seconds")
    payload = _finding_payload(finding)
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    content_hash = _hash_payload(payload)

    with _connect() as conn:
        previous = conn.execute(
            """
            SELECT content_hash, first_seen
            FROM raw_findings
            WHERE agent_name = ? AND source_name = ? AND external_id = ?
            """,
            (agent_name, finding.source_name, finding.external_id),
        ).fetchone()

        if previous is None:
            event_type = "new"
            first_seen = now
            previous_hash = None
        elif previous["content_hash"] != content_hash:
            event_type = "changed"
            first_seen = previous["first_seen"]
            previous_hash = previous["content_hash"]
        else:
            event_type = "unchanged"
            first_seen = previous["first_seen"]
            previous_hash = previous["content_hash"]

        conn.execute(
            """
            INSERT INTO raw_findings (
                agent_name, source_name, external_id, source_url, title,
                finding_type, payload_json, content_hash, first_seen, last_seen, last_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(agent_name, source_name, external_id) DO UPDATE SET
                source_url = excluded.source_url,
                title = excluded.title,
                finding_type = excluded.finding_type,
                payload_json = excluded.payload_json,
                content_hash = excluded.content_hash,
                last_seen = excluded.last_seen,
                last_run_id = excluded.last_run_id
            """,
            (
                agent_name,
                finding.source_name,
                finding.external_id,
                finding.source_url,
                finding.title,
                finding.finding_type,
                payload_json,
                content_hash,
                first_seen,
                now,
                run_id,
            ),
        )

        if event_type in {"new", "changed"}:
            conn.execute(
                """
                INSERT INTO finding_events (
                    run_id, agent_name, source_name, external_id, event_type,
                    previous_hash, new_hash, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    agent_name,
                    finding.source_name,
                    finding.external_id,
                    event_type,
                    previous_hash,
                    content_hash,
                    payload_json,
                    now,
                ),
            )

        conn.commit()

    return event_type


def finish_run(run_id: str, findings: int, changed_items: int) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    with _connect() as conn:
        conn.execute(
            """
            UPDATE agent_runs
            SET completed_at = ?, findings = ?, changed_items = ?
            WHERE run_id = ?
            """,
            (now, findings, changed_items, run_id),
        )
        conn.commit()


def get_run_events(run_id: str) -> list[dict[str, Any]]:
    """Return only new/changed findings emitted by one run.

    This is the input boundary for reconciliation and digests. Reading events
    does not mutate raw state or canonical Wind Radar data.
    """

    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, run_id, agent_name, source_name, external_id, event_type,
                   previous_hash, new_hash, payload_json, created_at
            FROM finding_events
            WHERE run_id = ?
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()

    events: list[dict[str, Any]] = []
    for row in rows:
        try:
            finding_payload = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            finding_payload = {}
        events.append(
            {
                "id": row["id"],
                "run_id": row["run_id"],
                "agent_name": row["agent_name"],
                "source_name": row["source_name"],
                "external_id": row["external_id"],
                "event_type": row["event_type"],
                "previous_hash": row["previous_hash"],
                "new_hash": row["new_hash"],
                "created_at": row["created_at"],
                "finding": finding_payload,
            }
        )
    return events
