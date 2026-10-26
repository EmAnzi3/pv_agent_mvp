from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


PRIMARY_URL = "https://ambiente.regione.abruzzo.it/"
FALLBACK_URL = "https://trasparenza.regione.abruzzo.it/servizi-erogati/carta-servizi/rilascio-dei-pareri-sui-progetti-assoggettati-verifica-di"
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "parco eolico", "repowering")
PROJECT_CUES = ("progetto", "proponente", "via", "paur", "assoggettabil", "comune", "mw", "pratica")


class AbruzzoWindAgent(BaseWindAgent):
    """First-pass adapter for Regione Abruzzo environmental procedures.

    The official transparency service confirms that active VIA/PAUR projects are
    exposed by the dedicated environmental platform. If that platform is not
    resolvable/reachable from the runner, the adapter records an explicit
    degraded channel snapshot from the official transparency fallback instead
    of reporting project data that was not actually collected.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Abruzzo VIA"
    base_url = PRIMARY_URL

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _wind_project_text(cls, text: str) -> bool:
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
        for match in re.finditer(r"\bComuni?\s+(?:di|:)\s*(.+?)(?=\s+(?:Potenza|Proponente|Procedura|Stato|Data)|\.|$)", text, flags=re.I):
            raw = re.sub(r"\([A-Z]{2}\)", "", match.group(1))
            for part in re.split(r",|;|\s+e\s+", raw, flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and len(item) <= 80 and item.lower() not in {x.lower() for x in out}:
                    out.append(item)
        return out[:20]

    @staticmethod
    def _external_id(url: str, text: str) -> str:
        match = re.search(r"\b(?:VIA|VA|PAUR)\s*(?:n\.|nr\.|#|:|-)?\s*([A-Za-z0-9/_-]{3,})", text, flags=re.I)
        if match:
            value = re.sub(r"[^A-Za-z0-9_-]+", "-", match.group(1)).strip("-")
            return f"ABRUZZO-VIA-{value}"
        digest = hashlib.sha1((url + "|" + text).encode("utf-8")).hexdigest()[:18]
        return f"ABRUZZO-VIA-{digest}"

    def _get(self, url: str) -> BeautifulSoup:
        response = self.session.get(url, timeout=60, allow_redirects=True)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _channel_snapshot(self, *, primary_error: str | None = None) -> AgentFinding:
        source_url = PRIMARY_URL
        fallback_verified = False
        fallback_text = ""
        try:
            fallback = self._get(FALLBACK_URL)
            fallback_verified = True
            fallback_text = self._clean(fallback.get_text(" ", strip=True))[:4000]
            source_url = FALLBACK_URL
        except Exception as exc:
            if primary_error:
                primary_error = f"{primary_error}; fallback={type(exc).__name__}: {exc}"
            else:
                primary_error = f"fallback={type(exc).__name__}: {exc}"

        return AgentFinding(
            external_id="ABRUZZO-VIA-CHANNEL",
            source_name=self.source_name,
            source_url=source_url,
            title="Regione Abruzzo — canale Valutazioni Ambientali",
            finding_type="source_channel_snapshot",
            payload={
                "region": "Abruzzo",
                "sector": "eolico",
                "source_grade_ceiling": "A1",
                "project_specific": False,
                "execution_scope": None,
                "evidence_layer": "institutional_channel",
                "availability": "degraded_primary_unavailable" if primary_error else "channel_only",
                "primary_url": PRIMARY_URL,
                "fallback_url": FALLBACK_URL,
                "fallback_verified": fallback_verified,
                "primary_fetch_error": primary_error,
                "official_service_excerpt": fallback_text,
                "runtime_note": "No project rows were collected. Official fallback confirms the VIA/PAUR service and active-project platform; endpoint/DNS deepening remains required.",
            },
        )

    def fetch(self) -> list[AgentFinding]:
        try:
            root = self._get(PRIMARY_URL)
        except Exception as exc:
            return [self._channel_snapshot(primary_error=f"{type(exc).__name__}: {exc}")]

        host = urlparse(PRIMARY_URL).netloc
        urls = [PRIMARY_URL]
        for anchor in root.find_all("a", href=True):
            label = self._clean(anchor.get_text(" ", strip=True)).lower()
            href = anchor.get("href") or ""
            absolute = urljoin(PRIMARY_URL, href)
            if urlparse(absolute).netloc != host:
                continue
            if any(token in (label + " " + absolute.lower()) for token in ("via", "paur", "progett", "valutaz")):
                if absolute not in urls:
                    urls.append(absolute)
            if len(urls) >= 15:
                break

        findings: dict[str, AgentFinding] = {}
        parsed_project_rows = 0
        for url in urls:
            try:
                soup = root if url == PRIMARY_URL else self._get(url)
            except Exception:
                continue
            nodes = soup.find_all(["tr", "li", "article"])
            if not nodes:
                nodes = soup.find_all("div")
            seen_text: set[str] = set()
            for node in nodes:
                text = self._clean(node.get_text(" ", strip=True))
                if not (40 <= len(text) <= 5000) or text in seen_text or not self._wind_project_text(text):
                    continue
                seen_text.add(text)
                parsed_project_rows += 1
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
                        "region": "Abruzzo",
                        "province": None,
                        "municipalities": self._municipalities(text),
                        "power_mw": self._power_mw(text),
                        "procedure": "VIA / PAUR / screening",
                        "status_raw": None,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "new_wind_source_audit/abruzzo",
                    },
                )

        if parsed_project_rows == 0:
            findings["ABRUZZO-VIA-CHANNEL"] = self._channel_snapshot()

        return list(findings.values())
