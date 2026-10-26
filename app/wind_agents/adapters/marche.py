from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://contenuti.regione.marche.it/Regione-Utile/Ambiente/Valutazioni-e-Autorizzazioni-Ambientali/Valutazioni-di-Impatto-Ambientale-VIA/Avvio-Procedimenti-VIA"
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "parco eolico", "repowering")


class MarcheWindAgent(BaseWindAgent):
    """Regione Marche public VIA-start registry filtered to wind projects.

    The regional portal can reject automated runners with HTTP 403 even when the
    public page is otherwise available to a browser. In that case the adapter
    emits an explicit non-project channel snapshot instead of failing the whole
    source group or pretending that project rows were collected.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Marche VIA"
    base_url = BASE_URL

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS)

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
        match = re.search(r"\bProponente\s*:\s*(.+?)(?=(?:\.|\s+Comunicazione\b|\s+Tipo\s+protocollo\b|$))", text, flags=re.I)
        if not match:
            return None
        return cls._clean(match.group(1)).strip(" .,:;–—-")[:250] or None

    @classmethod
    def _municipalities(cls, text: str) -> list[str]:
        out: list[str] = []
        patterns = [
            r"\bComuni\s+di\s+(.+?)(?=\.\s|\s+Restart\b|\s+Procedimento\b|\s+Proponente\b|$)",
            r"\bComune\s+di\s+(.+?)(?=\.\s|\s+Proponente\b|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if not match:
                continue
            raw = re.sub(r"\([A-Z]{2}\)", "", match.group(1))
            for part in re.split(r",|\s+e\s+|;", raw, flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and len(item) <= 80 and item.lower() not in {x.lower() for x in out}:
                    out.append(item)
            if out:
                break
        return out[:20]

    @classmethod
    def _province(cls, text: str) -> str | None:
        codes = re.findall(r"\(([A-Z]{2})\)", text)
        for code in codes:
            if code in {"AN", "AP", "FM", "MC", "PU"}:
                return code
        return None

    @classmethod
    def _procedure(cls, text: str) -> str | None:
        lowered = text.lower()
        if "27-bis" in lowered or "27bis" in lowered or "paur" in lowered:
            return "PAUR"
        if "verifica di assoggettabil" in lowered or "art.19" in lowered or "art. 19" in lowered:
            return "Verifica di assoggettabilità a VIA"
        if "valutazione di impatto ambientale" in lowered or "procedimento di via" in lowered:
            return "VIA"
        return None

    @classmethod
    def _external_id(cls, text: str) -> str:
        for pattern in (r"\((V\d{4,6})\)", r"\[ID\s*:?\s*(\d+)\]", r"\b(ID\d{4,8})\b"):
            match = re.search(pattern, text, flags=re.I)
            if match:
                return "MARCHE-VIA-" + re.sub(r"[^A-Za-z0-9]+", "-", match.group(1).upper()).strip("-")
        digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:18]
        return f"MARCHE-VIA-{digest}"

    def _get(self, url: str) -> BeautifulSoup:
        response = self.session.get(
            url,
            timeout=60,
            allow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "it-IT,it;q=0.9,en;q=0.7",
                "Referer": "https://www.regione.marche.it/",
            },
        )
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _channel_snapshot(self, *, primary_error: str | None = None) -> AgentFinding:
        return AgentFinding(
            external_id="MARCHE-VIA-CHANNEL",
            source_name=self.source_name,
            source_url=BASE_URL,
            title="Regione Marche — canale Avvio Procedimenti VIA",
            finding_type="source_channel_snapshot",
            payload={
                "region": "Marche",
                "sector": "eolico",
                "source_grade_ceiling": "A1",
                "project_specific": False,
                "execution_scope": None,
                "evidence_layer": "institutional_channel",
                "availability": "degraded_primary_unavailable" if primary_error else "channel_only",
                "primary_url": BASE_URL,
                "primary_fetch_error": primary_error,
                "runtime_note": (
                    "No project rows were collected from the Marche VIA registry. "
                    "The public channel remains monitored, but a blocked/empty fetch cannot "
                    "be treated as project-specific evidence or used for canonical promotion."
                ),
            },
        )

    def fetch(self) -> list[AgentFinding]:
        try:
            soup = self._get(BASE_URL)
        except Exception as exc:
            return [self._channel_snapshot(primary_error=f"{type(exc).__name__}: {exc}")]

        candidates = soup.find_all("li")
        if not candidates:
            candidates = soup.find_all(["p", "div"])

        findings: dict[str, AgentFinding] = {}
        for node in candidates:
            text = self._clean(node.get_text(" ", strip=True))
            if len(text) < 40 or not self._is_wind(text):
                continue
            # Avoid catching navigation/help text: an actual registry item should
            # contain a project/procedure cue in addition to a wind term.
            if not any(cue in text.lower() for cue in ("proponente", "procedimento", "progetto", "protocollo", "via")):
                continue
            source_url = BASE_URL
            anchor = node.find("a", href=True)
            if anchor:
                source_url = urljoin(BASE_URL, anchor.get("href") or "")
            external_id = self._external_id(text)
            protocol = None
            match_protocol = re.search(r"\bProtocollo\s*:\s*([^|\s]+(?:\|[^\s]+)*)", text, flags=re.I)
            if match_protocol:
                protocol = self._clean(match_protocol.group(1))[:300]
            date_text = None
            match_date = re.search(r"\bData\s+(?:di\s+creazione|protocollo)\s*:\s*(\d{1,2}/\d{1,2}/\d{4})", text, flags=re.I)
            if match_date:
                date_text = match_date.group(1)

            findings[external_id] = AgentFinding(
                external_id=external_id,
                source_name=self.source_name,
                source_url=source_url,
                title=text[:700],
                finding_type="project_source",
                payload={
                    "project_name": text[:700],
                    "proponent": self._proponent(text),
                    "region": "Marche",
                    "province": self._province(text),
                    "municipalities": self._municipalities(text),
                    "power_mw": self._power_mw(text),
                    "procedure": self._procedure(text),
                    "status_raw": "Avvio/pubblicazione procedimento regionale",
                    "protocol": protocol,
                    "registry_date": date_text,
                    "sector": "eolico",
                    "source_grade_ceiling": "A1",
                    "project_specific": True,
                    "source_adapter_origin": "new_wind_source_audit/marche",
                },
            )

        if not findings:
            findings["MARCHE-VIA-CHANNEL"] = self._channel_snapshot()

        return list(findings.values())
