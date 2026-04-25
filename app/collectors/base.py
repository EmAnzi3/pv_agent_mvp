from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from app.config import settings


@dataclass
class CollectorResult:
    external_id: str
    source_url: str
    title: str
    payload: dict[str, Any]


class BaseCollector:
    source_name = "base"
    base_url = ""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})

    def fetch(self) -> list[CollectorResult]:
        raise NotImplementedError
