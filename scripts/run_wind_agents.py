#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.wind_agents.company_watch import due_company_ids, run_company_watch
from app.wind_agents.execution_watch import build_execution_queue
from app.wind_agents.planner import build_run_plan
from app.wind_agents.reconcile import build_digest
from app.wind_agents.runner import due_agent_ids, executable_agent_ids, run_agents


def _emit(payload: dict, output: str | None = None) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)


def main() -> None:
    parser = argparse.ArgumentParser(description="Wind Radar agent runner")
    sub = parser.add_subparsers(dest="command", required=True)

    plan_cmd = sub.add_parser("plan", help="build the due queues without network collection")
    plan_cmd.add_argument("--as-of", help="YYYY-MM-DD; defaults to today")
    plan_cmd.add_argument("--output", help="optional JSON output path")

    execution_cmd = sub.add_parser(
        "execution-queue",
        help="build review-only contractor-hunt queue for canonical E4-E7 projects",
    )
    execution_cmd.add_argument("--as-of", help="YYYY-MM-DD; defaults to today")
    execution_cmd.add_argument("--output", help="optional JSON output path")

    due_cmd = sub.add_parser("due", help="show executable institutional adapters due by runtime cadence")
    due_cmd.add_argument("--as-of", help="YYYY-MM-DD; defaults to today")
    due_cmd.add_argument("--output", help="optional JSON output path")

    company_due_cmd = sub.add_parser("company-due", help="show commercial players due for direct-source monitoring")
    company_due_cmd.add_argument("--as-of", help="YYYY-MM-DD; defaults to today")
    company_due_cmd.add_argument("--output", help="optional JSON output path")

    run_cmd = sub.add_parser("run", help="execute implemented institutional source adapters")
    run_cmd.add_argument(
        "--source",
        action="append",
        choices=executable_agent_ids(),
        help="source adapter to run; repeatable; default = all implemented",
    )
    run_cmd.add_argument(
        "--due",
        action="store_true",
        help="execute only adapters due according to registry cadence + persistent live watch state",
    )
    run_cmd.add_argument("--output", help="optional JSON report path")

    company_run_cmd = sub.add_parser("company-run", help="watch direct sources for commercial-network companies")
    company_run_cmd.add_argument(
        "--company",
        action="append",
        help="company registry id to run; repeatable; default = all companies with watch URLs",
    )
    company_run_cmd.add_argument(
        "--due",
        action="store_true",
        help="execute only company watches due by registry cadence + persistent runtime state",
    )
    company_run_cmd.add_argument("--output", help="optional JSON report path")

    digest_cmd = sub.add_parser(
        "digest",
        help="reconcile new/changed findings from one or more runs and emit review-only commercial digest",
    )
    digest_cmd.add_argument(
        "--run-id",
        action="append",
        required=True,
        help="agent/company run id to include; repeatable",
    )
    digest_cmd.add_argument("--output", help="optional JSON digest path")

    args = parser.parse_args()

    if args.command == "plan":
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
        _emit(build_run_plan(as_of=as_of).as_dict(), args.output)
        return

    if args.command == "execution-queue":
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
        _emit(build_execution_queue(as_of=as_of), args.output)
        return

    if args.command == "due":
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
        _emit(
            {
                "as_of": (as_of or date.today()).isoformat(),
                "due_agents": due_agent_ids(as_of=as_of),
                "all_executable_agents": executable_agent_ids(),
            },
            args.output,
        )
        return

    if args.command == "company-due":
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
        _emit(
            {
                "as_of": (as_of or date.today()).isoformat(),
                "due_companies": due_company_ids(as_of=as_of),
            },
            args.output,
        )
        return

    if args.command == "company-run":
        _emit(
            run_company_watch(args.company, due_only=args.due),
            args.output,
        )
        return

    if args.command == "digest":
        _emit(build_digest(args.run_id), args.output)
        return

    result = run_agents(args.source, due_only=args.due)
    _emit(result, args.output)


if __name__ == "__main__":
    main()
