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
