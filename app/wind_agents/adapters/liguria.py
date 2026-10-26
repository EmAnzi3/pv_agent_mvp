from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


PRIMARY_URL = "https://siraviavas.regione.liguria.it/ElencoInCorsoVIA.aspx?Tipo=VIA"
SERVICE_URL = "https://servizi.regione.liguria.it/page/welcome/VIA"
FALLBACK_URL = "https://www.regione.liguria.it/homepage-ambiente/cosa-cerchi/via-vas-aia-aua/valutazione-impatto-ambientale-via.html"
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "parco eolico", "repowering")


class LiguriaWindAgent(BaseWindAgent):
    """Regione Liguria VIA/PAUR registry, filtered to wind.

    The legacy/current SIRAVIAVAS endpoint can return server errors to automated
    clients. When it is unavailable, the adapter verifies the official Regione
    Liguria VIA page and records a degraded channel snapshot pointing to the
    official procedures service. It never turns an outage into fake project data.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Liguria VIA"
    base_url = PRIMARY_URL

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS)

    @staticmethod
    def _norm_header(value: str) -> str:
        value = value.lower().replace("à", "a").replace("è", "e").replace("ì", "i").replace("ò", "o").replace("ù", "u")
        return re.sub(r"[^a-z0-9]+", "_", value).strip("_")

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        match = re.search(r"(?<![\d.,])(\d+(?:[.,]\d+)?)\s*MW\b", text, flags=re.I)
        if not match:
            return None
        try:
            value = float(match.group(1).replace(",", "."))
        except ValueError:
            return None
        return value if 0 < value < 5000 else None

    @classmethod
    def _municipalities(cls, value: str) -> list[str]:
        items: list[str] = []
        for part in re.split(r"\n|;|,|\s+e\s+", value or "", flags=re.I):
            item = cls._clean(part).strip(" -–—:;,.()")
            if not item or len(item) > 80:
                continue
            if item.lower() in {"comune", "comuni", "-"}:
                continue
            if item.lower() not in {x.lower() for x in items}:
                items.append(item)
        return items[:20]

    @staticmethod
    def _external_id(practice: str, object_text: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", practice or "").strip("-")
        if cleaned:
            return f"LIGURIA-VIA-{cleaned}"
        digest = hashlib.sha1(object_text.encode("utf-8")).hexdigest()[:18]
        return f"LIGURIA-VIA-{digest}"

    def _page(self, page: int) -> str:
        url = PRIMARY_URL if page == 1 else f"{PRIMARY_URL}&page={page}"
        response = self.session.get(url, timeout=60, allow_redirects=True)
        response.raise_for_status()
        return response.text

    def _channel_snapshot(self, primary_error: str) -> AgentFinding:
        source_url = FALLBACK_URL
        fallback_verified = False
        service_link_verified = False
        excerpt = ""
        try:
            response = self.session.get(FALLBACK_URL, timeout=60, allow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            fallback_verified = True
            excerpt = self._clean(soup.get_text(" ", strip=True))[:4000]
            for anchor in soup.find_all("a", href=True):
                absolute = urljoin(FALLBACK_URL, anchor.get("href") or "")
                label = self._clean(anchor.get_text(" ", strip=True)).lower()
                if "procedimenti via" in label or absolute.rstrip("/") == SERVICE_URL.rstrip("/"):
                    service_link_verified = True
                    source_url = absolute
                    break
        except Exception as exc:
            primary_error = f"{primary_error}; fallback={type(exc).__name__}: {exc}"

        return AgentFinding(
            external_id="LIGURIA-VIA-CHANNEL",
            source_name=self.source_name,
            source_url=source_url,
            title="Regione Liguria — procedimenti VIA",
            finding_type="source_channel_snapshot",
            payload={
                "region": "Liguria",
                "sector": "eolico",
                "source_grade_ceiling": "A1",
                "project_specific": False,
                "execution_scope": None,
                "evidence_layer": "institutional_channel",
                "availability": "degraded_primary_unavailable",
                "primary_url": PRIMARY_URL,
                "service_url": SERVICE_URL,
                "fallback_url": FALLBACK_URL,
                "fallback_verified": fallback_verified,
                "service_link_verified": service_link_verified,
                "primary_fetch_error": primary_error,
                "official_page_excerpt": excerpt,
                "runtime_note": "Primary proceedings endpoint unavailable to runner; official Regione Liguria VIA page and procedures-service link remain the monitored fallback. No project rows were claimed.",
            },
        )

    def fetch(self) -> list[AgentFinding]:
        findings: dict[str, AgentFinding] = {}
        try:
            first_html = self._page(1)
        except Exception as exc:
            return [self._channel_snapshot(f"{type(exc).__name__}: {exc}")]

        for page in range(1, 6):
            if page == 1:
                html = first_html
            else:
                try:
                    html = self._page(page)
                except Exception:
                    break
            soup = BeautifulSoup(html, "html.parser")
            tables = soup.find_all("table")
            found_rows = 0

            for table in tables:
                headers: list[str] = []
                for tr in table.find_all("tr"):
                    cells = tr.find_all(["th", "td"])
                    values = [self._clean(cell.get_text("\n", strip=True)) for cell in cells]
                    if not values:
                        continue
                    if not headers:
                        candidate = [self._norm_header(v) for v in values]
                        if "oggetto" in candidate and ("numero_pratica" in candidate or "proponente" in candidate):
                            headers = candidate
                            continue
                    if not headers:
                        continue
                    row = {headers[i]: values[i] if i < len(values) else "" for i in range(len(headers))}
                    object_text = self._clean(row.get("oggetto"))
                    raw = self._clean(" | ".join(values))
                    if not object_text or not self._is_wind(raw):
                        continue
                    found_rows += 1
                    practice = self._clean(row.get("numero_pratica"))
                    source_url = PRIMARY_URL if page == 1 else f"{PRIMARY_URL}&page={page}"
                    for anchor in tr.find_all("a", href=True):
                        href = anchor.get("href") or ""
                        if href and not href.startswith("mailto:"):
                            source_url = urljoin(source_url, href)
                            break
                    municipalities = self._municipalities(row.get("comune", ""))
                    external_id = self._external_id(practice, object_text)
                    findings[external_id] = AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=source_url,
                        title=object_text[:700],
                        finding_type="project_source",
                        payload={
                            "project_name": object_text[:700],
                            "proponent": self._clean(row.get("proponente")) or None,
                            "region": "Liguria",
                            "province": None,
                            "municipalities": municipalities,
                            "power_mw": self._power_mw(object_text),
                            "procedure": self._clean(row.get("tipo_procedura")) or None,
                            "practice_number": practice or None,
                            "public_phase_start": self._clean(row.get("avvio_fase_pubblica")) or None,
                            "observations_deadline": self._clean(row.get("termine_per_osservazioni")) or None,
                            "status_raw": self._clean(row.get("note")) or "Procedimento in corso",
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "new_wind_source_audit/liguria",
                            "regional_mirror_guard": "National VIA rows are enrichment only and must reconcile to MASE before any project identity action.",
                        },
                    )

            if page > 1 and found_rows == 0 and not any("pagina" in self._clean(t.get_text(" ", strip=True)).lower() for t in tables):
                break

        if not findings:
            return [self._channel_snapshot("primary reachable but no parseable wind rows")]
        return list(findings.values())
