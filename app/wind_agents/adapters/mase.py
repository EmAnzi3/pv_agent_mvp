from __future__ import annotations

import re

from app.wind_agents.legacy.mase_via import collect_mase_via
from app.wind_agents.base import AgentFinding, BaseWindAgent


GENERIC_MASE_TITLE = "Valutazioni e Autorizzazioni Ambientali: VAS - VIA - AIA"


class MaseWindAgent(BaseWindAgent):
    """Wind-only adapter around the existing MASE collector.

    The underlying collector remains shared with the legacy Project Radar, while
    the agent constrains discovery to eolico/repowering/offshore terms and emits
    source findings instead of writing canonical data directly.

    MASE's current project pages use a generic H1. The actual project heading is
    still present at the beginning of the page text, so the wind adapter repairs
    title/power/geography at the source-finding layer without changing the shared
    legacy collector.
    """

    agent_name = "institutional_watch"
    source_name = "MASE VIA"
    base_url = "https://va.mite.gov.it"

    keywords = [
        "eolico",
        "parco eolico",
        "repowering eolico",
        "eolico offshore",
        "aerogeneratori",
    ]

    def __init__(self, max_pages_per_keyword: int = 2, max_details: int = 120) -> None:
        super().__init__()
        self.max_pages_per_keyword = max_pages_per_keyword
        self.max_details = max_details

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _project_title(cls, raw_title: object, description: object) -> str:
        raw = cls._clean(raw_title)
        desc = cls._clean(description)
        if raw and raw != GENERIC_MASE_TITLE and "VAS - VIA - AIA" not in raw:
            return raw
        if desc:
            candidate = desc.split(" - Info - ", 1)[0].strip(" -")
            if len(candidate) >= 20:
                return candidate[:1200]
        return raw or "Progetto eolico MASE"

    @staticmethod
    def _number(raw: str) -> float | None:
        value = raw.replace(" ", "")
        if "," in value:
            value = value.replace(".", "").replace(",", ".")
        try:
            parsed = float(value)
        except ValueError:
            return None
        return parsed if 0 < parsed < 5000 else None

    @classmethod
    def _wind_power_mw(cls, title: str, fallback: float | None) -> float | None:
        """Prefer explicit project totals over unit/legacy WTG power.

        Repowering titles often mention the old plant first and the new plant
        second. Taking the last explicit ``potenza complessiva`` therefore keeps
        the current rebuilt configuration in cases such as Nicosia 46.75 -> 78 MW.
        """

        total_patterns = (
            r"potenza\s+complessiva(?:\s+(?:installata|dell['’]impianto))?\s*(?:pari\s+a|di|:)?\s*([0-9][0-9.\s]*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MWp?\b",
            r"potenza\s+totale\s*(?:pari\s+a|di|:)?\s*([0-9][0-9.\s]*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MWp?\b",
        )
        totals: list[float] = []
        for pattern in total_patterns:
            for match in re.finditer(pattern, title, flags=re.I):
                value = cls._number(match.group(1))
                if value is not None:
                    totals.append(value)
        if totals:
            return totals[-1]

        # "potenza pari a X MW" is useful when no explicit total wording exists,
        # but avoid unit-power phrases tied to a single aerogeneratore.
        match = re.search(
            r"(?<!unitaria\s)potenza\s+(?:massima\s+in\s+immissione\s+)?(?:pari\s+a|di)\s*([0-9][0-9.\s]*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MWp?\b",
            title,
            flags=re.I,
        )
        if match:
            value = cls._number(match.group(1))
            if value is not None:
                return value
        return fallback

    @classmethod
    def _bess_power_mw(cls, title: str) -> float | None:
        patterns = (
            r"(?:sistema\s+di\s+)?accumulo(?:\s+integrato)?[^.;,]{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*MWp?\b",
            r"\bBESS\b[^.;,]{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*MWp?\b",
            r"\bstorage\b[^.;,]{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*MWp?\b",
        )
        for pattern in patterns:
            match = re.search(pattern, title, flags=re.I)
            if match:
                value = cls._number(match.group(1))
                if value is not None:
                    return value
        return None

    @classmethod
    def _province(cls, title: str, fallback: str | None) -> str | None:
        codes = re.findall(r"\(([A-Z]{2})\)", title)
        return codes[0] if codes else fallback

    @classmethod
    def _municipality(cls, title: str, fallback: str | None) -> str | None:
        for pattern in (
            r"\bComune\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|\s+e\s+|$)",
            r"\bComuni\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|\s+e\s+|$)",
        ):
            match = re.search(pattern, title, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" .,:;")
                if value:
                    return value
        return fallback

    def fetch(self) -> list[AgentFinding]:
        records = collect_mase_via(
            keywords=self.keywords,
            max_pages_per_keyword=self.max_pages_per_keyword,
            max_details=self.max_details,
        )

        findings: list[AgentFinding] = []
        for record in records:
            if (record.sector or "").lower() != "eolico":
                continue
            external_id = record.external_id or record.source_url or record.title
            title = self._project_title(record.title, record.description)
            wind_mw = self._wind_power_mw(title, record.power_mw)
            findings.append(
                AgentFinding(
                    external_id=str(external_id),
                    source_name=self.source_name,
                    source_url=record.source_url,
                    title=title,
                    finding_type="project_source",
                    payload={
                        "project_name": title,
                        "proponent": record.client,
                        "region": record.region,
                        "province": self._province(title, record.province),
                        "municipality": self._municipality(title, record.municipality),
                        "power_mw": wind_mw,
                        "bess_mw": self._bess_power_mw(title),
                        "phase": record.phase,
                        "sector": record.sector,
                        "description": record.description,
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_normalization": "wind adapter repaired generic MASE H1 and preferred explicit project-total MW; BESS remains separate",
                    },
                )
            )

        return findings
