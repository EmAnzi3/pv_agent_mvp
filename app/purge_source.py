from __future__ import annotations

import argparse
from sqlalchemy import text

from app.db import engine


def scalar(conn, sql: str, params: dict) -> int:
    return int(conn.execute(text(sql), params).scalar() or 0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Purge all records for a source from pv_agent_mvp database."
    )
    parser.add_argument("--source", required=True, help="Source name, e.g. sicilia")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete records. Without this flag the command runs in dry-run mode.",
    )
    args = parser.parse_args()

    source = args.source.strip()
    if not source:
        raise SystemExit("ERROR: --source vuoto")

    mode = "APPLY" if args.apply else "DRY-RUN"

    print(f"[purge-source-v2] source: {source}")
    print(f"[purge-source-v2] mode: {mode}")

    with engine.begin() as conn:
        counts = {
            "raw_items.source_name": scalar(
                conn,
                "SELECT COUNT(*) FROM raw_items WHERE source_name = :source",
                {"source": source},
            ),
            "projects_master.primary_source": scalar(
                conn,
                "SELECT COUNT(*) FROM projects_master WHERE primary_source = :source",
                {"source": source},
            ),
            "project_events.source_name": scalar(
                conn,
                "SELECT COUNT(*) FROM project_events WHERE source_name = :source",
                {"source": source},
            ),
            "project_events collegati a project_id primary_source": scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM project_events
                WHERE project_id IN (
                    SELECT id FROM projects_master WHERE primary_source = :source
                )
                """,
                {"source": source},
            ),
            "sources.name": scalar(
                conn,
                "SELECT COUNT(*) FROM sources WHERE name = :source",
                {"source": source},
            ),
        }

        print(f"[purge-source-v2] raw_items.source_name={source}: {counts['raw_items.source_name']}")
        print(f"[purge-source-v2] projects_master.primary_source={source}: {counts['projects_master.primary_source']}")
        print(f"[purge-source-v2] project_events.source_name={source}: {counts['project_events.source_name']}")
        print(f"[purge-source-v2] project_events collegati a project_id primary_source={source}: {counts['project_events collegati a project_id primary_source']}")
        print(f"[purge-source-v2] sources.name={source}: {counts['sources.name']}")

        if not args.apply:
            print("[purge-source-v2] DRY-RUN: nessun record cancellato.")
            print("[purge-source-v2] Per cancellare davvero rilancia con --apply")
            return

        deleted_events_by_project = conn.execute(
            text(
                """
                DELETE FROM project_events
                WHERE project_id IN (
                    SELECT id FROM projects_master WHERE primary_source = :source
                )
                """
            ),
            {"source": source},
        ).rowcount or 0

        deleted_events_by_source = conn.execute(
            text("DELETE FROM project_events WHERE source_name = :source"),
            {"source": source},
        ).rowcount or 0

        deleted_projects = conn.execute(
            text("DELETE FROM projects_master WHERE primary_source = :source"),
            {"source": source},
        ).rowcount or 0

        deleted_raw = conn.execute(
            text("DELETE FROM raw_items WHERE source_name = :source"),
            {"source": source},
        ).rowcount or 0

        deleted_sources = conn.execute(
            text("DELETE FROM sources WHERE name = :source"),
            {"source": source},
        ).rowcount or 0

        print("[purge-source-v2] DELETE completata.")
        print(f"[purge-source-v2] project_events by project_id: {deleted_events_by_project}")
        print(f"[purge-source-v2] project_events by source_name: {deleted_events_by_source}")
        print(f"[purge-source-v2] projects_master: {deleted_projects}")
        print(f"[purge-source-v2] raw_items: {deleted_raw}")
        print(f"[purge-source-v2] sources: {deleted_sources}")


if __name__ == "__main__":
    main()
