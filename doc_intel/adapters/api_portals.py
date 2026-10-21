"""Adapters for regional portals that publish documents through a JSON API (single-page apps).

* Lombardia SILVIA (``www.silvia.servizirl.it/silviaweb/#/scheda-sintesi?...idProgetto=N``):
  project sheet ``getSchedaSintesiByIdProgetto.html`` lists the filed documents
  (``listaViewArchivioDocByProgetto``); each document has one or more files from
  ``getFileByDocumento.html?idDocumento=``, downloaded with ``downloadDocumento.html?idEdma=&nomeFile=``.
* Toscana PATTI / STAR (``servizi.patti.regione.toscana.it/star-info/avvisiPubblici/N``):
  ``servizi.toscana.it/pub-api/suap/services/avvisi/eventiPubblici/N`` returns the notice; its
  ``contenuto`` (JSON text) holds the document sections (``collegamenti`` -> ``files``) with direct links,
  and ``localizzazione`` the site outline (GeoJSON).

Structures were read from the live portals with ``python -m doc_intel explore`` (October 2026).
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable
from urllib.parse import parse_qs, quote, urlparse

from ..schema import DocumentRef
from .base import AdapterCapabilities, Discovery, DocumentAdapter, ProjectIdentity

SILVIA = "https://www.silvia.servizirl.it/silviaweb/"


def _it_date(v: str | None) -> str | None:
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", v or "")
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


class SilviaAdapter(DocumentAdapter):
    source = "lombardia"
    capabilities = AdapterCapabilities(source="lombardia", project_identity=True, procedure_ids=True,
                                       document_index=True, pagination="none", download="http",
                                       declared_totals=True, status="supported")
    URL_RE = re.compile(r"^https?://www\.silvia\.servizirl\.it/silviaweb/")

    def __init__(self, get_html: Callable[[str], str] | None = None, get_json: Callable[[str], Any] | None = None):
        super().__init__(get_html)
        self.get_json = get_json or (lambda u: json.loads(self.get_html(u)))

    @classmethod
    def handles(cls, url: str) -> bool:
        return bool(cls.URL_RE.match(url or "")) and "idProgetto=" in url

    @staticmethod
    def _project_id(url: str) -> str:
        frag = urlparse(url).fragment or urlparse(url).query
        q = parse_qs(frag.split("?", 1)[-1])
        pid = (q.get("idProgetto") or [None])[0]
        if not pid:
            m = re.search(r"idProgetto=(\d+)", url)
            pid = m.group(1) if m else None
        if not pid:
            raise LookupError(f"no idProgetto in {url}")
        return pid

    def identify(self, url: str) -> ProjectIdentity:
        pid = self._project_id(url)
        sheet = self.get_json(f"{SILVIA}getSchedaSintesiByIdProgetto.html?idProgetto={pid}")
        p = sheet.get("progetto") or {}
        props = [re.sub(r"^(Azienda|Ente)\s*:\s*", "", x.get("descrEnteAzienda") or "").strip()
                 for x in sheet.get("listProponenti") or [] if x.get("descrEnteAzienda")]
        title = p.get("descrProgetto")
        munis, prov = [], None
        mm = re.search(r"(?i)\bcomun[ei]\s+di\s+([A-ZÀ-Ýa-zà-ÿ' ]+?)\s*\(([A-Z]{2})\)", title or "")
        if mm:
            munis, prov = [mm.group(1).strip().title()], mm.group(2)
        ident = ProjectIdentity(source="lombardia", project_id=pid, info_url=url, title=title,
                                proponent=props[0] if props else None, municipalities=munis, province=prov,
                                region="Lombardia")
        ident.raw.update({"sheet": {k: sheet.get(k) for k in ("listaViewArchivioDocByProgetto",)},
                          "procedure_code": p.get("codProcedura"), "authority": (p.get("ente") or {}).get("descrEnte"),
                          "status": (p.get("macroStato") or {}).get("descrMacroStato"),
                          "filing_date": _it_date(p.get("dataDeposito")),
                          "municipalities_citation": {"url": url, "excerpt": (title or "")[:300]}})
        return ident

    def discover(self, identity: ProjectIdentity) -> Discovery:
        rows = (identity.raw.get("sheet") or {}).get("listaViewArchivioDocByProgetto") or []
        docs, errors, folders = [], [], {}
        declared = 0
        for row in rows:
            if row.get("pubblico") not in (None, "S"):
                continue
            declared += int(row.get("countAllegati") or 1)
            folder = (row.get("descrTipoDeposito") or row.get("descTipoDocumento") or "documenti").strip()
            try:
                files = self.get_json(f"{SILVIA}getFileByDocumento.html?idDocumento={row['idDocumento']}") or []
            except Exception as exc:
                errors.append(f"document {row.get('idDocumento')}: {exc}"[:200])
                continue
            for f in files:
                name = f.get("nomeFile") or "documento"
                docs.append(DocumentRef(
                    url=f"{SILVIA}downloadDocumento.html?idEdma={f['idEdma']}&nomeFile={quote(name)}",
                    procedure_id=folder, title=f"{row.get('titolo') or ''} - {name}".strip(" -"), source="lombardia",
                    extra={"index_date": _it_date(f.get("dataUpload")) or _it_date(row.get("dataDocumento")),
                           "kind": row.get("descTipoDocumento"), "size": f.get("dimensione")}))
                folders[folder] = folders.get(folder, 0) + 1
        procedures = [{"id": identity.raw.get("procedure_code") or identity.project_id, "url": identity.info_url,
                       "label": identity.raw.get("procedure_code"), "declared": declared, "enumerated": len(docs),
                       "index_pages": 1, "errors": errors}]
        return Discovery(identity, docs, procedures, [] if docs else ["no public documents in the SILVIA sheet"])


class ToscanaPattiAdapter(DocumentAdapter):
    source = "toscana"
    capabilities = AdapterCapabilities(source="toscana", project_identity=True, procedure_ids=True,
                                       document_index=True, pagination="none", download="http",
                                       declared_totals=False, status="supported")
    URL_RE = re.compile(r"^https?://servizi\.patti\.regione\.toscana\.it/star-info/avvisiPubblici/(?P<id>\d+)")
    API = "https://servizi.toscana.it/pub-api/suap/services/avvisi/eventiPubblici/{id}?dominio=undefined&codiceTerritorio=undefined"

    def __init__(self, get_html: Callable[[str], str] | None = None, get_json: Callable[[str], Any] | None = None):
        super().__init__(get_html)
        self.get_json = get_json or (lambda u: json.loads(self.get_html(u)))

    @classmethod
    def handles(cls, url: str) -> bool:
        return bool(cls.URL_RE.match(url or ""))

    def identify(self, url: str) -> ProjectIdentity:
        nid = self.URL_RE.match(url).group("id")
        data = self.get_json(self.API.format(id=nid))
        try:
            content = json.loads(data.get("contenuto") or "{}")
        except ValueError:
            content = {}
        title = content.get("opera") or data.get("descrizione")
        munis = [m.strip().title() for m in re.split(r",|;", content.get("comune") or "") if m.strip()]
        ident = ProjectIdentity(source="toscana", project_id=nid, info_url=url, title=title,
                                proponent=content.get("proponente"), municipalities=munis, region="Toscana")
        ident.raw.update({"content": content, "procedure": data.get("procedimento"), "status": data.get("stato"),
                          "published": (data.get("dataInizioPubblicazione") or "")[:10] or None,
                          "updated": (data.get("dataUltimoAggiornamento") or "")[:10] or None,
                          "localizzazione": data.get("localizzazione"),
                          "municipalities_citation": {"url": url, "excerpt": (title or "")[:300]}})
        return ident

    def discover(self, identity: ProjectIdentity) -> Discovery:
        content = identity.raw.get("content") or {}
        docs, procedures = [], []
        for sec in content.get("collegamenti") or []:
            label = (sec.get("titoloSezione") or "documenti").strip()
            n = 0
            for f in sec.get("files") or []:
                link = f.get("collegamento")
                if not link:
                    continue
                docs.append(DocumentRef(url=link, procedure_id=label, title=f.get("nome") or label, source="toscana",
                                        extra={"index_date": identity.raw.get("updated")}))
                n += 1
            procedures.append({"id": label[:60], "url": identity.info_url, "label": label, "declared": None,
                               "declared_available": False, "enumerated": n, "index_pages": 1, "errors": []})
        return Discovery(identity, docs, procedures, [] if docs else ["no document links in the notice"])


ATOS_FIELDS = [("plant", "Denominazione Impianto"), ("company", "Denominazione Ditta"), ("municipality", "Comune"),
               ("province", "Sigla Provincia"), ("active", "Attivo"), ("authorization_type", "Tipo di Autorizzazione"),
               ("energy_source", "Tipologia Fonte"), ("authorization_status", "Stato Autorizzazione"),
               ("power_mw", "Potenza MW"), ("act_number", "Numero Atto"), ("act_date", "Data Atto"),
               ("act_body", "Ente Rilasciante"), ("act_category", "Categoria Atto"), ("act_type", "Tipologia Atto"),
               ("updated", "Ultima modifica")]


def parse_atos_sheet(text: str) -> dict[str, str]:
    """Label/value pairs of an ATOS plant sheet (labels in a fixed order, values in between)."""
    t = re.sub(r"\s+", " ", text or "")
    t = t.replace("-->", " ")
    labels = [lab for _, lab in ATOS_FIELDS] + ["Dati dell'ultimo Atto", "Autorizzazione Vigente", "In caso di atto"]
    out = {}
    for key, lab in ATOS_FIELDS:
        m = re.search(re.escape(lab) + r"\s*(.*?)(?=\s*(?:" + "|".join(re.escape(x) for x in labels if x != lab) + r")\b|$)", t)
        if m and m.group(1).strip():
            out[key] = m.group(1).strip()[:200]
    return out


def _norm_company(s: str | None) -> str:
    s = (s or "").lower()
    s = re.sub(r"s\.?\s?r\.?\s?l\.?|s\.?\s?p\.?\s?a\.?|societ[aà].*", "", s)
    return re.sub(r"[^a-z0-9]", "", s)


class AtosAdapter(DocumentAdapter):
    """Toscana ATOS register of energy plants (``atos.arrr.it/scheda_impianto_fer.php?id_impianto=N``).

    The sheet records the plant and its last authorisation act but no documents. When the radar holds
    the same plant from a procedure portal (Toscana PATTI, MASE...), matched by company, or by
    municipality and power, the documents of that procedure are analysed; the sheet adds the act."""
    source = "toscana_atos"
    capabilities = AdapterCapabilities(source="toscana_atos", project_identity=True, procedure_ids=False,
                                       document_index=False, pagination="none", download="none",
                                       declared_totals=False, status="partial",
                                       obstacles=["register without documents: documents come from the linked procedure"])
    URL_RE = re.compile(r"^https?://atos\.arrr\.it/scheda_impianto_fer\.php\?.*id_impianto=(?P<id>\d+)")
    wants_records = True

    def __init__(self, get_html: Callable[[str], str] | None = None):
        super().__init__(get_html)
        self.records: list[dict] = []

    @classmethod
    def handles(cls, url: str) -> bool:
        return bool(cls.URL_RE.match(url or ""))

    def identify(self, url: str) -> ProjectIdentity:
        from bs4 import BeautifulSoup
        pid = self.URL_RE.match(url).group("id")
        text = BeautifulSoup(self.get_html(url), "html.parser").get_text(" ", strip=True)
        sheet = parse_atos_sheet(text)
        title = " - ".join(x for x in (sheet.get("plant"), sheet.get("energy_source"),
                                       f"{sheet.get('power_mw')} MW" if sheet.get("power_mw") else None) if x)
        ident = ProjectIdentity(source="toscana_atos", project_id=pid, info_url=url, title=title or None,
                                proponent=sheet.get("company"),
                                municipalities=[sheet["municipality"].title()] if sheet.get("municipality") else [],
                                province=sheet.get("province"), region="Toscana")
        ident.raw.update({"register_sheet": sheet, "municipalities_citation": {"url": url, "excerpt": text[:300]}})
        return ident

    def linked_record(self, identity: ProjectIdentity) -> dict | None:
        sheet = identity.raw.get("register_sheet") or {}
        comp = _norm_company(sheet.get("company"))
        muni = (sheet.get("municipality") or "").lower()
        try:
            mw = float((sheet.get("power_mw") or "").replace(",", "."))
        except ValueError:
            mw = None
        from . import adapter_for_url
        cands = []
        for r in self.records:
            u = r.get("url") or ""
            if self.handles(u) or not u:
                continue
            try:
                adapter_for_url(u)
            except LookupError:
                continue
            same_company = comp and comp == _norm_company(r.get("proponent"))
            same_place = muni and muni in (r.get("municipalities") or "").lower() and mw and r.get("power_mw") \
                and abs(r["power_mw"] - mw) <= 0.05 * mw
            if same_company or same_place:
                cands.append((bool(same_company) + bool(same_place), r))
        return max(cands, key=lambda c: c[0])[1] if cands else None

    def discover(self, identity: ProjectIdentity) -> Discovery:
        rec = self.linked_record(identity)
        if not rec:
            return Discovery(identity, [], [], ["register sheet only: no procedure for this plant in the radar"])
        from . import adapter_for_url
        sub = adapter_for_url(rec["url"])
        sub.get_html, sub.log = self.get_html, self.log
        sub_ident = sub.identify(rec["url"])
        disc = sub.discover(sub_ident)
        identity.raw["linked_procedure"] = {"url": rec["url"], "source": rec.get("source"), "title": sub_ident.title}
        if sub_ident.raw.get("localizzazione"):
            identity.raw["localizzazione"] = sub_ident.raw["localizzazione"]
        return Discovery(identity, disc.documents, disc.procedures, disc.errors)
