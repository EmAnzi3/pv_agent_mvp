from __future__ import annotations

import hashlib
import html
import re
from datetime import date
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://portal.sardegnasira.it"
NEWS_URL = "https://portal.sardegnasira.it/impatto-ambientale"
SEARCH_URL = "https://portal.sardegnasira.it/ricerca-dei-progetti"
PORTLET = "_ViaProgetto_WAR_RegioneSardegnaportlet_"
FORM = "_ViaProgetto_WAR_RegioneSardegnaportlet_:form"

SEARCH_PROCEDURES = {
    "560": "VERIFICA",
    "566": "VIA/PAUR",
}
SEARCH_KEYWORDS = ("eolico", "eolica", "repowering")
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "repowering", "parco eolico")


class SardegnaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Sardegna SIRA collector.

    Uses both the environmental-news surface and the JSF project search. The
    search mechanics and procedure ids come from the mature PV collector; only
    the domain keywords/filter are changed to wind.
    """

    agent_name = "institutional_watch"
    source_name = "Sardegna SIRA VIA/PAUR"
    base_url = NEWS_URL

    def __init__(self, years: list[str] | None = None) -> None:
        super().__init__()
        current = date.today().year
        self.years = years or [str(current), str(current - 1)]

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _norm(cls, value: object) -> str:
        return cls._clean(value).lower()

    @classmethod
    def _is_wind(cls, value: object) -> bool:
        text = cls._norm(value)
        return any(term in text for term in WIND_TERMS)

    @staticmethod
    def _first_url(node, page_url: str) -> str:
        anchor = node.find("a", href=True) if node else None
        return urljoin(page_url, anchor["href"]) if anchor else page_url

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        for match in re.finditer(
            r"(?<![\d.,])([0-9]+(?:[.\s][0-9]{3})*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MW\b",
            text,
            flags=re.I,
        ):
            raw = match.group(1).replace(" ", "")
            if "," in raw:
                raw = raw.replace(".", "").replace(",", ".")
            try:
                value = float(raw)
            except ValueError:
                continue
            if 0 < value < 5000:
                return value
        return None

    @classmethod
    def _proponent(cls, text: str) -> str | None:
        for pattern in (
            r"Proponente\s*:?\s*(.+?)(?:\s+Comune|\s+Provincia|\s+Procedimento|\s+Oggetto|\||$)",
            r"Societ[aà]\s+(.+?)(?:\s+ha\s+presentato|\s+ha\s+depositato|\s+richiede|\||$)",
        ):
            match = re.search(pattern, text, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" .,:;")
                if 2 <= len(value) <= 200:
                    return value
        return None

    @classmethod
    def _municipality(cls, text: str) -> str | None:
        for pattern in (
            r"Comune\s*:?\s*([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’`\- ]+?)(?:\s+Provincia|\s*\([A-Z]{2}\)|\||$)",
            r"Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’`\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|\s+e\s+|$)",
        ):
            match = re.search(pattern, text, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" .,:;")
                if value:
                    return value
        return None

    @staticmethod
    def _province(text: str) -> str | None:
        match = re.search(r"\(([A-Z]{2})\)", text)
        return match.group(1) if match else None

    @classmethod
    def _status(cls, text: str) -> str | None:
        lowered = cls._norm(text)
        for needle, label in (
            ("favorevole con prescrizioni", "Favorevole con prescrizioni"),
            ("favorevole", "Favorevole"),
            ("archiviat", "Archiviato"),
            ("conclus", "Concluso"),
            ("integrazioni", "Integrazioni"),
            ("osservazioni", "Osservazioni"),
            ("in corso", "In corso"),
        ):
            if needle in lowered:
                return label
        return None

    @staticmethod
    def _external_id(kind: str, title: str, proponent: str | None, municipality: str | None, source_url: str) -> str:
        raw = "|".join([kind, title, proponent or "", municipality or "", source_url])
        return "SARDEGNA-WIND-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def _finding(
        self,
        *,
        kind: str,
        title: str,
        text: str,
        source_url: str,
        procedure: str | None = None,
        source_year: str | None = None,
    ) -> AgentFinding:
        proponent = self._proponent(text)
        municipality = self._municipality(text)
        return AgentFinding(
            external_id=self._external_id(kind, title, proponent, municipality, source_url),
            source_name=self.source_name,
            source_url=source_url,
            title=title[:700],
            finding_type="project_source",
            payload={
                "project_name": title[:700],
                "proponent": proponent,
                "region": "Sardegna",
                "province": self._province(text),
                "municipality": municipality,
                "power_mw": self._power_mw(title + " " + text),
                "procedure": procedure,
                "status_raw": self._status(text),
                "source_year": source_year,
                "sector": "eolico",
                "source_grade_ceiling": "A1",
                "project_specific": True,
                "source_adapter_origin": "pv_agent_mvp/sardegna.py",
            },
        )

    def _fetch_news(self) -> list[AgentFinding]:
        response = self.session.get(NEWS_URL, timeout=90)
        response.raise_for_status()
        soup = BeautifulSoup(response.content.decode("utf-8", errors="replace"), "html.parser")
        blocks = list(soup.select("div.news-sardegna"))
        if not blocks:
            blocks = list(soup.select(".news-list .row-fluid"))

        findings: list[AgentFinding] = []
        for block in blocks:
            text = self._clean(block.get_text(" ", strip=True))
            if len(text) < 40 or not self._is_wind(text):
                continue
            title_node = block.select_one(".news-sardegna-title h4") or block.select_one(".news-sardegna-title a")
            title = self._clean(title_node.get_text(" ", strip=True)) if title_node else text[:700]
            source_url = self._first_url(block, NEWS_URL)
            findings.append(
                self._finding(kind="news", title=title, text=text, source_url=source_url)
            )
        return findings

    def _get_search_form(self) -> tuple[str, str, str]:
        response = self.session.get(
            SEARCH_URL,
            timeout=90,
            headers={
                "User-Agent": "Wind-Radar-Agent/0.6",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        form = soup.find("form", id=FORM)
        if not form or not form.get("action"):
            raise RuntimeError("Sardegna SIRA JSF form not found")
        action = urljoin(SEARCH_URL, html.unescape(form.get("action")))
        viewstate_el = soup.find("input", {"name": "javax.faces.ViewState"})
        if not viewstate_el or not viewstate_el.get("value"):
            raise RuntimeError("Sardegna SIRA ViewState not found")
        encoded_el = soup.find("input", {"name": "javax.faces.encodedURL"})
        if encoded_el and encoded_el.get("value"):
            encoded_url = html.unescape(encoded_el.get("value"))
        else:
            encoded_url = action.replace(
                "_facesViewIdResource=",
                "_jsfBridgeAjax=true&_facesViewIdResource=",
            )
        return action, encoded_url, viewstate_el.get("value")

    def _post_search(self, *, action: str, encoded_url: str, viewstate: str, year: str, procedure_code: str, keyword: str) -> str:
        payload = {
            FORM: FORM,
            "javax.faces.encodedURL": encoded_url,
            f"{FORM}:a_focus": "",
            f"{FORM}:a_input": procedure_code,
            f"{FORM}:b": "",
            f"{FORM}:c": "",
            f"{FORM}:d_focus": "",
            f"{FORM}:d_input": year,
            f"{FORM}:e_focus": "",
            f"{FORM}:e_input": "_",
            f"{FORM}:f_focus": "",
            f"{FORM}:f_input": "_",
            f"{FORM}:g": keyword,
            f"{FORM}:toggleable_collapsed": "false",
            f"{FORM}:confirmForm": "xx",
            "javax.faces.ViewState": viewstate,
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": f"{FORM}:cerca",
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": FORM,
            f"{FORM}:cerca": f"{FORM}:cerca",
        }
        headers = {
            "User-Agent": "Wind-Radar-Agent/0.6",
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": BASE_URL,
            "Referer": SEARCH_URL,
            "Accept": "application/xml, text/xml, */*; q=0.01",
        }
        response = self.session.post(action, data=payload, headers=headers, timeout=90)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _update_snippets(response_text: str) -> list[str]:
        snippets: list[str] = []
        for match in re.finditer(r"<update[^>]*>(.*?)</update>", response_text, flags=re.I | re.S):
            content = re.sub(r"^\s*<!\[CDATA\[", "", match.group(1))
            content = re.sub(r"\]\]>\s*$", "", content)
            content = html.unescape(content).strip()
            if content:
                snippets.append(content)
        return snippets

    def _parse_search(self, response_text: str, procedure: str, year: str) -> list[AgentFinding]:
        snippets = self._update_snippets(response_text) or [response_text]
        findings: list[AgentFinding] = []
        for snippet in snippets:
            soup = BeautifulSoup(snippet, "html.parser")
            for table in soup.find_all("table"):
                table_id = (table.get("id") or "").lower()
                table_text = self._norm(table.get_text(" ", strip=True))
                if "tblresult" not in table_id and not ("proponente" in table_text and any(x in table_text for x in ("titolo", "progetto", "comune", "procedimento"))):
                    continue
                for tr in table.find_all("tr"):
                    cells = tr.find_all("td")
                    if not cells:
                        continue
                    values = [self._clean(td.get_text(" ", strip=True)) for td in cells]
                    raw_text = self._clean(" | ".join(values))
                    if not raw_text or not self._is_wind(raw_text):
                        continue
                    if "selezionare un comune" in self._norm(raw_text):
                        continue
                    title = next((value for value in values if self._is_wind(value)), raw_text)[:700]
                    source_url = self._first_url(tr, SEARCH_URL)
                    findings.append(
                        self._finding(
                            kind="search",
                            title=title,
                            text=raw_text,
                            source_url=source_url,
                            procedure=procedure,
                            source_year=year,
                        )
                    )
        return findings

    def _fetch_search(self) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        for year in self.years:
            for procedure_code, procedure_label in SEARCH_PROCEDURES.items():
                for keyword in SEARCH_KEYWORDS:
                    action, encoded_url, viewstate = self._get_search_form()
                    response_text = self._post_search(
                        action=action,
                        encoded_url=encoded_url,
                        viewstate=viewstate,
                        year=year,
                        procedure_code=procedure_code,
                        keyword=keyword,
                    )
                    findings.extend(self._parse_search(response_text, procedure_label, year))
        return findings

    def fetch(self) -> list[AgentFinding]:
        combined = self._fetch_news() + self._fetch_search()
        unique: dict[str, AgentFinding] = {}
        for finding in combined:
            unique.setdefault(finding.external_id, finding)
        return list(unique.values())
