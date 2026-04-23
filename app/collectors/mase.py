from __future__ import annotations

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


class MASECollector(BaseCollector):
    source_name = "mase"
    base_url = "https://va.mite.gov.it/it-IT/Ricerca/Via"

    def fetch(self) -> list[CollectorResult]:
        """
        Starter collector.

        Nota importante:
        il portale MASE usa flussi e strutture che possono richiedere una
        implementazione dedicata o Playwright. Questo metodo restituisce i record
        solo se la pagina espone HTML sufficiente alla lettura.
        """
        try:
            response = self.session.get(self.base_url, timeout=settings.request_timeout)
            response.raise_for_status()
        except Exception:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        results: list[CollectorResult] = []

        for idx, card in enumerate(soup.select("a, article, div")):
            text = " ".join(card.get_text(" ", strip=True).split())
            if not text:
                continue
            lowered = text.lower()
            if "fotovolta" not in lowered and "agrivolta" not in lowered and "bess" not in lowered:
                continue
            href = card.get("href") if hasattr(card, "get") else None
            full_url = href if href and href.startswith("http") else self.base_url
            results.append(
                CollectorResult(
                    external_id=f"mase-{idx}",
                    source_url=full_url,
                    title=text[:250],
                    payload={
                        "title": text[:500],
                        "status_raw": None,
                        "region": None,
                        "province": None,
                        "municipalities": [],
                        "power": None,
                        "project_type_hint": text,
                    },
                )
            )
            if len(results) >= 50:
                break

        return results
