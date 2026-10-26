from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


OFFICE_URL = "https://www.regione.molise.it/flex/cm/pages/ServeBLOB.php/L/IT/IDPagina/15585"
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "parco eolico", "repowering")
PROJECT_CUES = ("proponente", "progetto", "comune", "mw", "autorizz", "via", "voltura", "proroga", "variante")


class MoliseWindAgent(BaseWindAgent):
    """Regione Molise wind-specific authorization/VIA channels.

    The regional office page exposes dedicated 'Eolico' and 'Eolico - VIA
    nazionale' routes. Those routes may be SPA-backed. When project rows are
    present in returned HTML they are emitted project-specific; otherwise a
    channel snapshot is retained so availability/drift remains observable
    without pretending that project data was collected.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Molise Eolico"
    base_url = OFFICE_URL

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _is_project_block(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS) and any(cue in lowered for cue in PROJECT_CUES)

    @staticmethod
    def _power_mw(text: str) -> float | None:
        match = re.search(r"(?<![\d.,])(\d+(?:[.,]\d+)?)\s*MW\b", text, flags=re.I)
        if not match:
            return None
        try:
            value = float(match.group(1).replace(",", "."))
        except ValueError:
            return None
        return value if 0 < value < 5000 else None

    @classmethod
    def _proponent(cls, text: str) -> str | None:
        match = re.search(r"\bProponente\s*:?\s*(.+?)(?=\s+(?:Comune|Comuni|Potenza|Procedura|Stato|Data|$))", text, flags=re.I)
        return cls._clean(match.group(1)).strip(" .,:;–—-")[:250] if match else None

    @classmethod
    def _municipalities(cls, text: str) -> list[str]:
        out: list[str] = []
        for match in re.finditer(r"\bComuni?\s+(?:di|interessati?\s*:?|:)\s*(.+?)(?=\s+(?:Potenza|Proponente|Procedura|Stato|Data)|\.|$)", text, flags=re.I):
            raw = re.sub(r"\([A-Z]{2}\)", "", match.group(1))
            for part in re.split(r",|;|\s+e\s+", raw, flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and len(item) <= 80 and item.lower() not in {x.lower() for x in out}:
                    out.append(item)
        return out[:20]

    @staticmethod
    def _external_id(url: str, text: str) -> str:
        for pattern in (r"\b(?:ID|Pratica|Procedimento)\s*[:#-]?\s*([A-Za-z0-9/_-]{3,})", r"\b([A-Z]{2,5}[-_/]\d{3,}[A-Za-z0-9/_-]*)\b"):
            match = re.search(pattern, text, flags=re.I)
            if match:
                value = re.sub(r"[^A-Za-z0-9_-]+", "-", match.group(1)).strip("-")
                return f"MOLISE-WIND-{value}"
        digest = hashlib.sha1((url + "|" + text).encode("utf-8")).hexdigest()[:18]
        return f"MOLISE-WIND-{digest}"

    def _get_soup(self, url: str) -> BeautifulSoup:
        response = self.session.get(url, timeout=60, allow_redirects=True)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def fetch(self) -> list[AgentFinding]:
        office = self._get_soup(OFFICE_URL)
        wind_urls: list[tuple[str, str]] = []
        for anchor in office.find_all("a", href=True):
            label = self._clean(anchor.get_text(" ", strip=True))
            if "eolico" not in label.lower():
                continue
            url = urljoin(OFFICE_URL, anchor.get("href") or "")
            if url not in {u for u, _ in wind_urls}:
                wind_urls.append((url, label))

        findings: dict[str, AgentFinding] = {}
        for url, label in wind_urls:
            try:
                soup = self._get_soup(url)
            except Exception:
                # Runner will record portal-level exceptions only if the whole
                # adapter fails. A single SPA route failure should not hide the
                # fact that the official wind channel itself was discovered.
                soup = None

            project_count = 0
            if soup is not None:
                nodes = soup.find_all(["tr", "li", "article"])
                if not nodes:
                    nodes = soup.find_all("div")
                seen_text: set[str] = set()
                for node in nodes:
                    text = self._clean(node.get_text(" ", strip=True))
                    if not (40 <= len(text) <= 5000) or text in seen_text or not self._is_project_block(text):
                        continue
                    seen_text.add(text)
                    project_count += 1
                    source_url = url
                    anchor = node.find("a", href=True)
                    if anchor:
                        source_url = urljoin(url, anchor.get("href") or "")
                    external_id = self._external_id(source_url, text)
                    findings[external_id] = AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=source_url,
                        title=text[:700],
                        finding_type="project_source",
                        payload={
                            "project_name": text[:700],
                            "proponent": self._proponent(text),
                            "region": "Molise",
                            "province": None,
                            "municipalities": self._municipalities(text),
                            "power_mw": self._power_mw(text),
                            "procedure": "VIA nazionale mirror" if "via nazionale" in label.lower() else "Autorizzazione FER / procedimento eolico",
                            "status_raw": None,
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "new_wind_source_audit/molise",
                        },
                    )

            if project_count == 0:
                external_id = "MOLISE-CHANNEL-" + hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]
                findings[external_id] = AgentFinding(
                    external_id=external_id,
                    source_name=self.source_name,
                    source_url=url,
                    title=f"Regione Molise — {label}",
                    finding_type="source_channel_snapshot",
                    payload={
                        "region": "Molise",
                        "channel_label": label,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": False,
                        "execution_scope": None,
                        "evidence_layer": "institutional_channel",
                        "runtime_note": "Official wind route discovered; no project rows were parseable from server-rendered HTML in this run (SPA/API enrichment still required).",
                    },
                )

        return list(findings.values())
