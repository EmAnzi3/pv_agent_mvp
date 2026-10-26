"""Wind Radar agent orchestration.

The package mirrors the proven PV Agent pattern (source collection ->
normalisation -> reconciliation/history -> reports) while keeping Wind Radar
project, company and institutional evidence layers separate.
"""

from .planner import build_run_plan

__all__ = ["build_run_plan"]
