from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class AgentTask:
    agent: str
    task_id: str
    priority: str
    cadence_days: int | None
    reason: str
    target: dict[str, Any]
    watch_urls: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AgentRunPlan:
    as_of: str
    institutional: list[AgentTask]
    companies: list[AgentTask]
    projects: list[AgentTask]

    @property
    def total_tasks(self) -> int:
        return len(self.institutional) + len(self.companies) + len(self.projects)

    def as_dict(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "total_tasks": self.total_tasks,
            "counts": {
                "institutional": len(self.institutional),
                "companies": len(self.companies),
                "projects": len(self.projects),
            },
            "institutional": [task.as_dict() for task in self.institutional],
            "companies": [task.as_dict() for task in self.companies],
            "projects": [task.as_dict() for task in self.projects],
        }
