from __future__ import annotations

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


class VenetoCollector(BaseCollector):
    source_name = "veneto"
    base_url = "https://www.regione.veneto.it/web/vas-via-vinca-nuvv/progetti-2026"

    def fetch(self) -> list[CollectorResult]:
        try:
            response = self.session.get(self.base_url, timeout=settings.request_timeout)
            response.raise_for_status()
        except Exception:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        results: list[CollectorResult] = []

        rows = soup.select("tr")
        for idx, row in enumerate(rows):
            text = " ".join(row.get_text(" ", strip=True).split())
            if not text:
                continue
            lowered = text.lower()
            if "fotovolta" not in lowered and "agrivolta" not in lowered and "agrovolta" not in lowered and "bess" not in lowered:
                continue

            link = row.find("a")
            href = link.get("href") if link else None
            full_url = href if href and href.startswith("http") else self.base_url
            results.append(
                CollectorResult(
                    external_id=f"veneto-{idx}",
                    source_url=full_url,
                    title=text[:250],
                    payload={
                        "title": text[:500],
                        "status_raw": text,
                        "region": "Veneto",
                        "province": None,
                        "municipalities": [],
                        "power": text,
                        "project_type_hint": text,
                    },
                )
            )
        return results
