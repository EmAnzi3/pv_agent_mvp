from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests


@dataclass
class AgentFinding:
    """Raw finding emitted by one wind agent.

    Findings are deliberately source-level. They do not mutate the canonical
    project graph and do not close contractor scopes by themselves.
    """

    external_id: str
    source_name: str
    source_url: str | None
    title: str
    finding_type: str
    payload: dict[str, Any] = field(default_factory=dict)


class BaseWindAgent:
    """Small contract modelled on pv_agent_mvp BaseCollector."""

    agent_name = "base"
    source_name = "base"
    base_url = ""

    def __init__(self, user_agent: str = "Wind-Radar-Agent/0.6") -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

    def fetch(self) -> list[AgentFinding]:
        raise NotImplementedError
