from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://va.mite.gov.it"
LIST_URL = "https://va.mite.gov.it/it-IT/Comunicazione/UltimiProvvedimenti"
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "repowering", "offshore", "parco eolico")

POSITIVE = (
    "esito positivo",
    "giudizio positivo",
    "conclusa con esito positivo",
    "concluso con esito positivo",
    "compatibilità ambientale positiva",
    "compatibilita ambientale positiva",
    "parere positivo",
)
NEGATIVE = (
    "esito negativo",
    "giudizio negativo",
    "conclusa con esito negativo",
    "concluso con esito negativo",
    "parere negativo",
)


class MaseProvvedimentiWindAgent(BaseWindAgent):
    """Wind watch for MASE's latest environmental decisions.

    A positive VIA decision is recorded as project evidence but is explicitly
    guarded from being interpreted as overall AU/FID/construction authorization.
    Project-page fields are parsed separately from the decision page so that
    proponent, geography and current project MW do not inherit menu/navigation
    text or unit/legacy turbine powers.
    """

    agent_name = "institutional_watch"
    source_name = "MASE Provvedimenti"
    base_url = LIST_URL

    def __init__(self, max_pages: int = 10) -> None:
        super().__init__()
        self.max_pages = max(1, max_pages)

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _is_wind(cls, value: object) -> bool:
        text = cls._clean(value).lower()
        return any(term in text for term in WIND_TERMS)

    def _get_html(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                headers={
                    "User-Agent": "Wind-Radar-Agent/0.6",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
                    "Referer": BASE_URL,
                },
                timeout=90,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.content.decode("utf-8", errors="replace")
        except Exception:
            return None

    def _collect_items(self) -> list[dict[str, str]]:
        items: dict[str, dict[str, str]] = {}
        for page in range(1, self.max_pages + 1):
            page_url = LIST_URL if page == 1 else f"{LIST_URL}?pagina={page}"
            html_page = self._get_html(page_url)
            if not html_page:
                continue
            soup = BeautifulSoup(html_page, "html.parser")
            for anchor in soup.find_all("a", href=True):
                absolute = urljoin(page_url, anchor.get("href") or "")
                if "/Comunicazione/DettaglioUltimiProvvedimenti/" not in absolute:
                    continue
                parent = anchor.find_parent(["li", "tr", "div", "article", "section"])
                title = self._clean(anchor.get_text(" ", strip=True))
                context = self._clean(parent.get_text(" ", strip=True)) if parent else title
                if not title:
                    title = context[:900]
                items.setdefault(absolute, {"detail_url": absolute, "list_title": title[:900], "context": context[:3000]})
        return list(items.values())

    @classmethod
    def _outcome(cls, text: str) -> str | None:
        lowered = cls._clean(text).lower()
        if any(term in lowered for term in NEGATIVE):
            return "negative"
        if any(term in lowered for term in POSITIVE):
            return "positive"
        return None

    @classmethod
    def _procedure(cls, text: str) -> str | None:
        lowered = cls._clean(text).lower()
        if "verifica di ottemperanza" in lowered:
            return "Verifica di ottemperanza"
        if "verifica di assoggettabilità" in lowered or "verifica di assoggettabilita" in lowered:
            return "Verifica di assoggettabilità VIA"
        if "provvedimento unico in materia ambientale" in lowered:
            return "Provvedimento Unico in materia Ambientale"
        if "valutazione di impatto ambientale" in lowered or re.search(r"\bvia\b", lowered):
            return "VIA"
        if "scoping" in lowered:
            return "Scoping"
        return "Provvedimento ambientale"

    @classmethod
    def _decree(cls, text: str) -> str | None:
        patterns = (
            r"\bD\.M\.\s*(?:MASE[_\-]VA[_\-]DEC[_\-])?\d{4}[-_]\d+",
            r"\bDM[_\-\s]?\d{4}[-_]\d+",
            r"\bMASE[_\-]VA[_\-]DEC[_\-]\d{4}[-_]\d+",
            r"\bDecreto\s+(?:Direttoriale|Ministeriale)?\s*[^,.;\n]{1,100}",
            r"\bn\.\s*\d+\s+del\s+\d{1,2}/\d{1,2}/\d{4}",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if match:
                return cls._clean(match.group(0))[:160]
        return None

    @staticmethod
    def _date(text: str) -> str | None:
        match = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b|\b\d{4}-\d{2}-\d{2}\b", text)
        return match.group(0) if match else None

    @classmethod
    def _project_url(cls, soup: BeautifulSoup, detail_url: str) -> str | None:
        for anchor in soup.find_all("a", href=True):
            absolute = urljoin(detail_url, anchor.get("href") or "")
            if "/Oggetti/Info/" in absolute:
                return absolute
        return None

    @classmethod
    def _document_url(cls, soup: BeautifulSoup, detail_url: str) -> str | None:
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href") or ""
            label = cls._clean(anchor.get_text(" ", strip=True)).lower()
            absolute = urljoin(detail_url, href)
            if ".pdf" in href.lower() or "decreto" in label or "provvedimento" in label:
                return absolute
        return None

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
    def _project_title(cls, project_text: str, fallback: str) -> str:
        text = cls._clean(project_text)
        if text:
            # MASE project pages currently expose the actual project heading at
            # the beginning of the flat text, followed by the generic "- Info -"
            # site heading. This is more complete than the truncated decisions list.
            candidate = text.split(" - Info - ", 1)[0].strip(" -")
            if len(candidate) >= 20 and cls._is_wind(candidate):
                return candidate[:1400]
        return cls._clean(fallback)[:900]

    @classmethod
    def _power_mw(cls, project_title: str) -> float | None:
        totals: list[float] = []
        for pattern in (
            r"potenza\s+complessiva(?:\s+(?:installata|dell['’]impianto))?\s*(?:pari\s+a|di|:)?\s*([0-9][0-9.\s]*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MWp?\b",
            r"potenza\s+totale\s*(?:pari\s+a|di|:)?\s*([0-9][0-9.\s]*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MWp?\b",
        ):
            for match in re.finditer(pattern, project_title, flags=re.I):
                value = cls._number(match.group(1))
                if value is not None:
                    totals.append(value)
        if totals:
            # In repowering descriptions the legacy plant precedes the rebuilt
            # plant; keep the final explicit project total as current config.
            return totals[-1]
        match = re.search(
            r"potenza\s+(?:massima\s+in\s+immissione\s+)?(?:pari\s+a|di)\s*([0-9][0-9.\s]*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MWp?\b",
            project_title,
            flags=re.I,
        )
        return cls._number(match.group(1)) if match else None

    @classmethod
    def _bess_mw(cls, project_title: str) -> float | None:
        for pattern in (
            r"(?:sistema\s+di\s+)?accumulo(?:\s+integrato)?[^.;,]{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*MWp?\b",
            r"\bBESS\b[^.;,]{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*MWp?\b",
            r"\bstorage\b[^.;,]{0,80}?([0-9]+(?:[.,][0-9]+)?)\s*MWp?\b",
        ):
            match = re.search(pattern, project_title, flags=re.I)
            if match:
                value = cls._number(match.group(1))
                if value is not None:
                    return value
        return None

    @classmethod
    def _project_fields(cls, text: str, project_title: str) -> dict:
        proponent = None
        for pattern in (
            r"Proponente\s*:\s*(.+?)(?=\s+Tipologia\s+di\s+opera\s*:|\s+Altri\s+progetti\b|\s+Territori\s+ed\s+aree\s+marine\b|\s+Scegli\s+la\s+procedura\b|$)",
            r"Societ[aà]\s+proponente\s*:\s*(.+?)(?=\s+Tipologia\s+di\s+opera\s*:|\s+Territori\s+ed\s+aree\s+marine\b|$)",
        ):
            match = re.search(pattern, text, flags=re.I)
            if match:
                candidate = cls._clean(match.group(1)).strip(" -–—:;,.()")
                if 2 <= len(candidate) <= 250:
                    proponent = candidate
                    break

        region = None
        region_match = re.search(
            r"Regioni\s*:\s*(.+?)(?=\s+Province\s*:|\s+Comuni\s*:|\s+Aree\s+marine\s*:|\s+Scegli\s+la\s+procedura\b|$)",
            text,
            flags=re.I,
        )
        if region_match:
            region = cls._clean(region_match.group(1)).strip(" -–—:;,.()") or None

        province = None
        province_match = re.search(
            r"Province\s*:\s*(.+?)(?=\s+Comuni\s*:|\s+Aree\s+marine\s*:|\s+Scegli\s+la\s+procedura\b|$)",
            text,
            flags=re.I,
        )
        if province_match:
            province_text = cls._clean(province_match.group(1))
            province = province_text.split(",", 1)[0].strip() or None
        if not province:
            code_match = re.search(r"\(([A-Z]{2})\)", project_title)
            province = code_match.group(1) if code_match else None

        municipalities: list[str] = []
        municipalities_match = re.search(
            r"Comuni\s*:\s*(.+?)(?=\s+Aree\s+marine\s*:|\s+Scegli\s+la\s+procedura\b|$)",
            text,
            flags=re.I,
        )
        if municipalities_match:
            for part in municipalities_match.group(1).split(","):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and 2 <= len(item) <= 90 and item.lower() not in {v.lower() for v in municipalities}:
                    municipalities.append(item)
        if not municipalities:
            for match in re.finditer(r"\b(?:Comune|Comuni)\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|;|$)", project_title, flags=re.I):
                for part in re.split(r",|/|\s+e\s+", match.group(1), flags=re.I):
                    item = cls._clean(part).strip(" -–—:;,.()")
                    if item and 2 <= len(item) <= 90 and item.lower() not in {v.lower() for v in municipalities}:
                        municipalities.append(item)
                if municipalities:
                    break

        return {
            "proponent": proponent,
            "region": region,
            "province": province,
            "municipalities": municipalities[:20],
            "power_mw": cls._power_mw(project_title),
            "bess_mw": cls._bess_mw(project_title),
        }

    def fetch(self) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        seen: set[str] = set()
        for item in self._collect_items():
            detail_url = item["detail_url"]
            detail_html = self._get_html(detail_url)
            if not detail_html:
                continue
            soup = BeautifulSoup(detail_html, "html.parser")
            detail_text = self._clean(soup.get_text(" ", strip=True))
            combined = self._clean(f"{item.get('list_title', '')} {item.get('context', '')} {detail_text}")
            if not self._is_wind(combined):
                continue

            project_url = self._project_url(soup, detail_url)
            project_text = ""
            if project_url:
                project_html = self._get_html(project_url)
                if project_html:
                    project_text = self._clean(BeautifulSoup(project_html, "html.parser").get_text(" ", strip=True))

            evidence_text = self._clean(f"{combined} {project_text}")
            title = self._project_title(project_text, item.get("list_title") or "MASE provvedimento eolico")
            fields = self._project_fields(project_text or evidence_text, title)
            match = re.search(r"/DettaglioUltimiProvvedimenti/(\d+)", detail_url)
            detail_id = match.group(1) if match else re.sub(r"\W+", "-", detail_url)[-80:]
            external_id = f"MASE-PROVV-WIND-{detail_id}"
            if external_id in seen:
                continue
            seen.add(external_id)

            findings.append(
                AgentFinding(
                    external_id=external_id,
                    source_name=self.source_name,
                    source_url=detail_url,
                    title=title,
                    finding_type="project_outcome",
                    payload={
                        "project_name": title,
                        "proponent": fields.get("proponent"),
                        "region": fields.get("region"),
                        "province": fields.get("province"),
                        "municipalities": fields.get("municipalities", []),
                        "power_mw": fields.get("power_mw"),
                        "bess_mw": fields.get("bess_mw"),
                        "procedure": self._procedure(item.get("list_title") or evidence_text),
                        "outcome": self._outcome(evidence_text),
                        "decree_number": self._decree(detail_text),
                        "decree_date": self._date(detail_text),
                        "project_url": project_url,
                        "document_url": self._document_url(soup, detail_url),
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "execution_scope": None,
                        "stage_semantic_guard": "VIA/provvedimento outcome is not proof of overall AU, FID, procurement or construction stage.",
                        "source_adapter_origin": "pv_agent_mvp/mase_provvedimenti.py",
                        "source_normalization": "project page supplies full title/proponent/geography/current total MW; BESS remains separate",
                    },
                )
            )
        return findings
