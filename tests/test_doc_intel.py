"""Offline tests for the doc_intel toolbox (no network, no canonical writes)."""
from __future__ import annotations

import json
import math
import re
import tempfile
import unittest
from pathlib import Path

from doc_intel.adapters.mase import MaseAdapter
from doc_intel.canonical import compare, find_by_url
from doc_intel.dossier import render
from doc_intel.extract import extract_all
from doc_intel.extract.context import PageContext, parse_number
from doc_intel.extract.entities import valid_vat_it
from doc_intel.extract.geo import utm_to_latlon
from doc_intel.fetch import Fetcher, sniff
from doc_intel.gates import proposal_gates
from doc_intel.metrics import evaluate
from doc_intel.parse import OcrBudget, extract_pages
from doc_intel.pipeline import build_proposal, run
from doc_intel.reconcile import group
from doc_intel.store import Store
from tests.doc_intel_fixtures import FakeSession, make_pdf, make_zip

ROOT = Path(__file__).resolve().parents[1]
U = "https://va.mite.gov.it/File/Documento/"


def page(text, doc="1", n=1, title=None, proc="P1"):
    return PageContext(text, U + doc, n, "sha" + doc, None, proc, title)


def latlon_to_utm(lat, lon, zone):
    """Forward Transverse Mercator (test-only) for round-trip checks."""
    a, f, k0 = 6378137.0, 1 / 298.257223563, 0.9996
    e2 = f * (2 - f); ep2 = e2 / (1 - e2)
    phi, lam = math.radians(lat), math.radians(lon)
    lam0 = math.radians((zone - 1) * 6 - 180 + 3)
    n = a / math.sqrt(1 - e2 * math.sin(phi) ** 2)
    t = math.tan(phi) ** 2; c = ep2 * math.cos(phi) ** 2; A = math.cos(phi) * (lam - lam0)
    m = a * ((1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256) * phi
             - (3 * e2 / 8 + 3 * e2 ** 2 / 32 + 45 * e2 ** 3 / 1024) * math.sin(2 * phi)
             + (15 * e2 ** 2 / 256 + 45 * e2 ** 3 / 1024) * math.sin(4 * phi)
             - (35 * e2 ** 3 / 3072) * math.sin(6 * phi))
    x = k0 * n * (A + (1 - t + c) * A ** 3 / 6 + (5 - 18 * t + t ** 2 + 72 * c - 58 * ep2) * A ** 5 / 120) + 500000
    y = k0 * (m + n * math.tan(phi) * (A ** 2 / 2 + (5 - t + 9 * c + 4 * c ** 2) * A ** 4 / 24
                                        + (61 - 58 * t + t ** 2 + 600 * c - 330 * ep2) * A ** 6 / 720))
    return x, y


class NumberParsing(unittest.TestCase):
    def test_italian_numbers(self):
        self.assertEqual(parse_number("54,5"), 54.5)
        self.assertEqual(parse_number("1.234,56"), 1234.56)
        self.assertEqual(parse_number("60.000"), 60000)
        self.assertEqual(parse_number("0.710"), 0.71)
        self.assertEqual(parse_number("4972345.12"), 4972345.12)


class Entities(unittest.TestCase):
    def test_vat_checksum_and_association(self):
        facts = extract_all([page("MARGISOLAR S.r.l. - P.IVA 03920651209 - PEC margisolar@legalmail.it")])
        vat = [f for f in facts if f.field == "company.vat"][0]
        self.assertEqual(vat.subject, "MARGISOLAR S.r.l.")
        self.assertTrue(vat.qualifiers["checksum_ok"])
        self.assertEqual(vat.qualifiers["subject_association"], "same_line")
        self.assertEqual(vat.status, "candidate")  # checksum never makes it 'verified'
        pec = [f for f in facts if f.field == "contact.pec"][0]
        self.assertEqual(pec.value, "margisolar@legalmail.it")

    def test_invalid_vat_rejected(self):
        self.assertFalse(valid_vat_it("03920651208"))
        facts = extract_all([page("ACME S.r.l. P.IVA 03920651208")])
        self.assertEqual([f.status for f in facts if f.field == "company.vat"], ["rejected"])

    def test_margisolar_two_vats_is_conflict(self):
        facts = extract_all([page("MARGISOLAR S.r.l. P.IVA 03920651209", doc="805858"),
                             page("MARGISOLAR S.R.L. P.IVA 03920631201", doc="805857")])
        groups = group(facts)
        multi = [g for g in groups if g["field"] == "company.multiple_vat"]
        self.assertEqual(len(multi), 1)
        self.assertEqual(multi[0]["values"], ["03920631201", "03920651209"])

    def test_roles_only_from_labels(self):
        facts = extract_all([page("Progettista: Arch. Antonio Manco\nTecnico: Geol. Fabio Di Feo\n"
                                  "INTEGRA s.r.l. email integra@integraingegneria.it")])
        roles = {(f.field, f.value) for f in facts if f.field.startswith("role.")}
        self.assertIn(("role.designer", "Arch. Antonio Manco"), roles)
        # INTEGRA appears without a role label: no role is invented
        self.assertFalse(any("INTEGRA" in str(v) for _, v in roles))

    def test_conditional_contractor_is_rejected(self):
        facts = extract_all([page("Impresa esecutrice: ROSSI COSTRUZIONI S.p.A. sarà individuata in fase di gara")])
        epc = [f for f in facts if f.field == "role.contractor"]
        self.assertTrue(epc and all(f.status == "rejected" for f in epc))

    def test_explicit_contractor_requires_human_gate(self):
        facts = extract_all([page("Impresa esecutrice: ROSSI COSTRUZIONI S.p.A.")])
        groups = group(facts)
        prop = build_proposal(facts, groups)
        gates = proposal_gates(prop, facts, groups)
        row = [g for g in gates if g["item"] == "role.contractor"][0]
        self.assertTrue(row["blocking"])


class Technical(unittest.TestCase):
    def test_pv_dc_ac_bess_not_summed(self):
        facts = extract_all([page("Impianto agrivoltaico di potenza 60 MWp lato DC e 50 MW lato AC.\n"
                                  "Sistema di accumulo BESS da 10 MW / 40 MWh.")])
        got = {(f.field, f.value) for f in facts if f.field.startswith("tech.")}
        self.assertIn(("tech.pv_dc_mwp", 60.0), got)
        self.assertIn(("tech.pv_ac_mw", 50.0), got)
        self.assertIn(("tech.bess_power_mw", 10.0), got)
        self.assertIn(("tech.bess_energy_mwh", 40.0), got)
        self.assertNotIn(("tech.pv_dc_mwp", 110.0), got)

    def test_inverter_and_module(self):
        facts = extract_all([page("Inverter da 350 kW: 168\nGeneratori fotovoltaici da 0,710 kWp\n"
                                  "tracker monoassiali con moduli bifacciali da 710 Wp")])
        got = {(f.field, f.value) for f in facts}
        self.assertIn(("tech.inverter_count", 168), got)
        self.assertIn(("tech.inverter_power_kw", 350.0), got)
        self.assertIn(("tech.module_power_wp", 710.0), got)
        self.assertIn(("tech.mounting", "single_axis_tracker"), got)
        self.assertFalse(any(f == "tech.nominal_power_mw" for f, _ in got))  # 350 kW is not a plant power

    def test_grid_voltage_needs_grid_context(self):
        a = extract_all([page("Connessione alla RTN tramite sezione a 36 kV della SE Terna")])
        b = extract_all([page("Cavo di segnale da 36 kV di prova")])
        self.assertTrue(any(f.field == "grid.voltage_kv" for f in a))
        self.assertFalse(any(f.field == "grid.voltage_kv" for f in b))

    def test_historical_power_is_conflict_not_overwrite(self):
        facts = extract_all([page("Potenza complessiva 54,9 MWp", doc="old", title="Relazione tecnica"),
                             page("Potenza complessiva 54,5 MWp", doc="dec", title="Decreto VIA")])
        g = [x for x in group(facts) if x["field"] == "tech.pv_dc_mwp"][0]
        self.assertEqual(g["status"], "conflict")
        self.assertEqual(g["preference_hint"]["value"], 54.5)
        self.assertFalse(g["preference_hint"]["binding"])


class Geo(unittest.TestCase):
    def test_central_meridian_known_value(self):
        lat, lon = utm_to_latlon(500000, 4982950.4, 32)
        self.assertAlmostEqual(lon, 9.0, places=6)
        self.assertAlmostEqual(lat, 45.0, places=4)

    def test_round_trip_italy(self):
        for lat, lon, zone in ((44.85, 8.81, 32), (41.39, 15.31, 33), (46.5, 11.4, 32), (37.5, 14.0, 33)):
            e, n = latlon_to_utm(lat, lon, zone)
            la, lo = utm_to_latlon(e, n, zone)
            self.assertAlmostEqual(la, lat, places=5)
            self.assertAlmostEqual(lo, lon, places=5)

    def test_cabin_coordinate_is_not_plant(self):
        txt = "Coordinate UTM WGS84 Fuso 33\nCabina di consegna 512345.10 4581234.20"
        facts = extract_all([page(txt)])
        pt = [f for f in facts if f.field == "geo.utm_point"][0]
        self.assertEqual(pt.qualifiers["object"], "cabin")
        self.assertIn("NOT the plant", pt.limits)
        prop = [{"field": "geo.utm_point", "fact_id": pt.fact_id()}]
        self.assertTrue(proposal_gates(prop, facts, group(facts))[0]["blocking"])

    def test_vertex_table_builds_polygon(self):
        # square of 300 m x 300 m = 9 ha, explicit zone 32
        txt = ("Coordinate dei vertici delle aree - sistema UTM WGS84 32N\nArea A\n"
               "V1 488000.00 4968000.00\nV2 488300.00 4968000.00\nV3 488300.00 4968300.00\nV4 488000.00 4968300.00\n")
        facts = extract_all([page(txt, doc="805873", n=60)])
        poly = [f for f in facts if f.field == "geo.area_polygon"]
        self.assertEqual(len(poly), 1)
        self.assertEqual(poly[0].subject, "A")
        self.assertAlmostEqual(poly[0].value["area_ha"], 9.0, places=2)
        self.assertEqual(poly[0].value["crs"], "EPSG:32632")
        self.assertIsNotNone(poly[0].value["geojson"])

    def test_zone_unknown_is_not_converted(self):
        facts = extract_all([page("Vertici area\nV1 488000.00 4968000.00")])
        pt = [f for f in facts if f.field == "geo.utm_point"][0]
        self.assertNotIn("lat", pt.qualifiers)
        self.assertIn("zone not stated", pt.limits)

    def test_pvgis_point(self):
        facts = extract_all([page("Simulazione PVGIS - coordinate 41.3901, 15.3102")])
        pt = [f for f in facts if f.field == "geo.latlon_point"][0]
        self.assertEqual(pt.qualifiers["object"], "simulation_point")


class Schedule(unittest.TestCase):
    def test_safety_plan_date_is_template(self):
        txt = "PIANO DI SICUREZZA E COORDINAMENTO\nDati del cantiere\nInizio lavori: 01/09/2026\nDurata presunta: 18 mesi"
        facts = extract_all([page(txt, doc="1439043", n=133)])
        d = [f for f in facts if f.field == "schedule.date.construction_start"][0]
        self.assertEqual(d.value, "2026-09-01")
        self.assertEqual(d.temporal, "template_reference")
        prop = [{"field": d.field, "fact_id": d.fact_id()}]
        self.assertTrue(proposal_gates(prop, facts, group(facts))[0]["blocking"])

    def test_gantt_date_is_planned_not_event(self):
        facts = extract_all([page("Cronoprogramma dei lavori\nInizio lavori previsto: settembre 2026")])
        d = [f for f in facts if f.field.startswith("schedule.date.")][0]
        self.assertEqual(d.temporal, "planned")
        self.assertEqual(d.value, "2026-09")

    def test_occurred_event(self):
        facts = extract_all([page("Con verbale di consegna lavori, in data 01/09/2026 sono iniziati i lavori (inizio lavori)")])
        d = [f for f in facts if f.field.startswith("schedule.date.")][0]
        self.assertEqual(d.temporal, "event_occurred")

    def test_durations_scope_and_scenario(self):
        txt = ("Con squadre in parallelo la realizzazione richiederà 10-12 mesi.\n"
               "La durata dei lavori di costruzione è stimata in 18 mesi.\n"
               "Le attività di dismissione e ripristino avranno durata pari a 16 mesi.")
        facts = extract_all([page(txt)])
        d = {(f.field, f.value["max"], f.temporal) for f in facts if f.field.startswith("schedule.duration")}
        self.assertIn(("schedule.duration.construction", 12, "scenario"), d)
        self.assertIn(("schedule.duration.construction", 18, "relative_duration"), d)
        self.assertIn(("schedule.duration.decommissioning", 16, "relative_duration"), d)
        self.assertFalse(any(f.field.startswith("schedule.date") for f in facts))  # no invented dates


class Fetching(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_signatures(self):
        self.assertEqual(sniff(make_pdf(["x"])), "pdf")
        self.assertEqual(sniff(make_zip({"a.pdf": make_pdf(["x"])})), "zip")
        self.assertEqual(sniff(b"<!DOCTYPE html><html>"), "html")

    def test_zip_cache_revision_and_failure(self):
        z = make_zip({"decreto.pdf": make_pdf(["Decreto"]), "note.txt": b"x", "inner.zip": make_zip({"b.pdf": make_pdf(["B"])})})
        sess = FakeSession({U + "1": z, U + "2": IOError("reset")})
        f = Fetcher(self.store, session=sess, retries=1, sleeper=lambda s: None)
        man = {"source": "mase", "project_id": "t", "documents": {}}
        r = f.fetch(U + "1", man)
        self.assertEqual(r.state, "new")
        self.assertEqual(sorted(m.name for m in r.members), ["decreto.pdf", "inner.zip/b.pdf"])
        self.assertEqual(r.skipped_members[0]["name"], "note.txt")
        self.assertEqual(f.fetch(U + "1", man).state, "cached")
        self.assertEqual(sess.calls.count(U + "1"), 1)
        sess.routes[U + "1"] = make_pdf(["new revision"])
        self.assertEqual(f.fetch(U + "1", man, refresh=True).state, "revised")
        self.assertEqual(len(man["documents"][U + "1"]["history"]), 1)
        bad = f.fetch(U + "2", man)
        self.assertEqual(bad.state, "failed")
        self.assertEqual(man["documents"][U + "2"]["attempts"], 1)
        self.assertEqual(sess.calls.count(U + "2"), 2)  # bounded retries


class Parsing(unittest.TestCase):
    def test_digital_and_deferred_ocr_and_cache(self):
        with tempfile.TemporaryDirectory() as d:
            store = Store(d)
            raw = make_pdf(["Relazione tecnica impianto agrivoltaico potenza 60 MWp lato DC con moduli", ""])
            out = extract_pages(raw, "s1", store, ocr=True, budget=OcrBudget(0))
            self.assertEqual(out["summary"]["digital"], 1)
            self.assertEqual(out["summary"]["ocr_deferred"], 1)
            self.assertEqual(out["ocr_pending"], [2])
            again = extract_pages(raw, "s1", store, ocr=False)
            self.assertTrue(again.get("from_cache"))

    def test_deadline_defers_and_next_run_resumes_only_pending(self):
        import time as _t
        with tempfile.TemporaryDirectory() as d:
            store = Store(d)
            raw = make_pdf(["Relazione tecnica impianto agrivoltaico potenza 60 MWp lato DC con moduli", "", ""])
            first = extract_pages(raw, "s2", store, ocr=True, budget=OcrBudget(100, deadline=_t.time() - 1))
            self.assertEqual(first["ocr_pending"], [2, 3])
            calls = []
            import doc_intel.parse as P
            orig = P._ocr_page
            P._ocr_page = lambda b, n, t, dpi, l: (calls.append(n) or ("testo ricavato da ocr " * 5, "ocr", None))
            try:
                second = extract_pages(raw, "s2", store, ocr=True, budget=OcrBudget(1))
            finally:
                P._ocr_page = orig
            self.assertEqual(calls, [2])            # only pending pages, within budget
            self.assertEqual(second["ocr_pending"], [3])
            self.assertTrue(second["resumed"])
            self.assertEqual(second["summary"]["digital"], 1)


@unittest.skipUnless(__import__("shutil").which("tesseract") and __import__("shutil").which("pdftoppm"),
                     "tesseract/poppler not installed")
class RealOcr(unittest.TestCase):
    def test_render_and_ocr_page(self):
        from doc_intel.parse import PopplerBackend, ocr_png
        txt = ocr_png(PopplerBackend(make_pdf(["P.IVA 03920651209 Inizio lavori 01/09/2026"])).render_png(1, 300))
        self.assertIn("03920651209", txt)
        self.assertIn("01/09/2026", txt)


INFO_HTML ="""<html><body><h1>Progetto per la realizzazione di un impianto fotovoltaico, denominato "Tortona 2", di potenza pari a 60 MW,
 da realizzarsi nei Comuni di Tortona (AL), Pozzolo Formigaro (AL) e Frugarolo (AL).</h1>
<p>Proponente: Margisolar S.r.l. Tipologia di opera: Impianti fotovoltaici</p>
<nav><a href="/it-IT/Oggetti/Documentazione/9999/1">Menu other object</a></nav>
<table><tr><td>VIA</td><td><a href="/it-IT/Oggetti/Documentazione/77/100">Documentazione</a></td></tr>
<tr><td>Ottemperanza</td><td><a href="/it-IT/Oggetti/Documentazione/77/200">Documentazione</a></td></tr></table></body></html>"""


def index_html(declared, ids, last_page):
    rows = "".join(f'<tr><td>Elaborato {i} relazione</td><td>01/02/2023</td><td><a href="/File/Documento/{i}">scarica</a></td></tr>' for i in ids)
    pager = "".join(f'<a href="?pagina={p}">{p}</a>' for p in range(1, last_page + 1))
    return f"<html><body><h3>({'n. ' + str(declared)}) Documenti</h3><table>{rows}</table>{pager}</body></html>"


def fake_site():
    base = "https://va.mite.gov.it/it-IT/Oggetti/Documentazione/77/"
    return {
        "https://va.mite.gov.it/it-IT/Oggetti/Info/77": INFO_HTML,
        base + "100": index_html(3, [11, 12], 2),
        base + "100?pagina=2": index_html(3, [13], 2),
        base + "200": index_html(2, [21, 11], 1),
    }


class MaseAdapterTests(unittest.TestCase):
    def test_identity_procedures_and_pagination(self):
        site = fake_site()
        ad = MaseAdapter(get_html=lambda u: site[u])
        ident = ad.identify("https://va.mite.gov.it/it-IT/Oggetti/Info/77")
        self.assertEqual([p["id"] for p in ident.procedures], ["100", "200"])  # menu link ignored
        self.assertEqual(ident.municipalities, ["Tortona", "Pozzolo Formigaro", "Frugarolo"])
        self.assertEqual(ident.province, "AL")
        self.assertEqual(ident.proponent, "Margisolar S.r.l.")
        disc = ad.discover(ident)
        procs = {p["id"]: p for p in disc.procedures}
        self.assertEqual((procs["100"]["declared"], procs["100"]["enumerated"], procs["100"]["index_pages"]), (3, 3, 2))
        self.assertEqual(procs["200"]["duplicates_across_procedures"], 1)
        self.assertEqual(len(disc.documents), 5)


class EndToEnd(unittest.TestCase):
    def test_pipeline_dossier_canonical_and_gates(self):
        site = fake_site()
        pdfs = {
            11: make_pdf(["MARGISOLAR S.r.l. P.IVA 03920651209\nImpianto agrivoltaico 60 MWp lato DC e 50 MW lato AC"]),
            12: make_pdf(["MARGISOLAR S.r.l. P.IVA 03920631201\nProgettista: Ing. Mario Rossi"]),
            13: make_pdf(["Coordinate UTM WGS84 32N vertici\nArea A\nV1 488000.00 4968000.00\nV2 488300.00 4968000.00\n"
                          "V3 488300.00 4968300.00\nV4 488000.00 4968300.00"]),
            21: make_pdf(["PIANO DI SICUREZZA E COORDINAMENTO\nInizio lavori: 01/09/2026"]),
        }
        routes = {U + str(k): v for k, v in pdfs.items()}
        with tempfile.TemporaryDirectory() as d:
            data = Path(d) / "data.json"
            data.write_text(json.dumps({"records": [{"url": "https://va.mite.gov.it/it-IT/Oggetti/Info/77",
                                                      "proponent": "Margisolar S.r.l.", "power_mw": 60.0,
                                                      "municipalities": "TORTONA", "province": "AL",
                                                      "project_key": "url:x"}]}))
            fetcher = Fetcher(Store(d + "/store"), session=FakeSession(routes), sleeper=lambda s: None)
            dossier = run("https://va.mite.gov.it/it-IT/Oggetti/Info/77", d + "/store", str(data), ocr=False,
                          get_html=lambda u: site[u], fetcher=fetcher, log=lambda m: None)
            a = dossier["acquisition"]
            self.assertEqual((a["references"], a["distinct_sha256"], a["duplicate_references"]), (5, 4, 1))
            delta = {r["field"]: r for r in dossier["delta"]}
            self.assertEqual(delta["proponent"]["class"], "confermato")
            self.assertEqual(delta["power_mw"]["class"], "confermato")
            self.assertEqual(delta["municipalities"]["class"], "nuovo_verificato")
            self.assertEqual(delta["municipalities"]["proposal"]["add"], ["frugarolo", "pozzolo formigaro"])
            self.assertTrue(any(g["field"] == "company.multiple_vat" for g in dossier["groups"]))
            self.assertFalse(any(p["field"] == "company.vat" for p in dossier["proposal"]))  # conflict not proposed
            self.assertEqual(dossier["review"]["auto_approval"], False)
            from doc_intel.card import build_card, write_card
            card = build_card(dossier)
            self.assertEqual(card["project_key"], "url:x")
            tiles = {t["key"]: t for t in card["sintesi"]}
            self.assertEqual(tiles["potenza"]["value"], "60 MW")
            self.assertNotIn("Tensione di connessione", json.dumps(card, ensure_ascii=False))
            self.assertEqual(card["ubicazione"]["comuni"], ["Tortona", "Pozzolo Formigaro", "Frugarolo"])
            self.assertEqual(card["ubicazione"]["provincia"], "AL")
            names = [e["name"] for e in card["soggetti"]]
            self.assertEqual(len([n for n in names if "margisolar" in n.lower()]), 1)     # one company, however spelled
            self.assertIn("bars", card["cronoprogramma"])
            self.assertNotIn("sha", json.dumps(card).lower().replace("sharing", ""))
            p_ = write_card(card, d + "/schede")
            self.assertTrue((Path(d) / "schede" / "index.json").is_file() and p_.is_file())
            from doc_intel.__main__ import digest
            titles = [t for t, _ in digest(dossier)]
            self.assertEqual(titles[:3], ["acquisition", "delta", "conflicts"])
            json.dumps(digest(dossier), default=str)
            md = render(dossier)
            self.assertIn("Confronto con il record canonico", md)
            ev = evaluate(dossier, {"items": [{"field": "tech.pv_ac_mw", "value": 50, "document": "11", "page": 1}],
                                    "expected_conflicts": [{"field": "company.multiple_vat", "subject": "Margisolar S.r.l."}],
                                    "forbidden": [{"field": "role.epc"}]})
            self.assertEqual(ev["recall_anchored"], 1.0)
            self.assertFalse(ev["blocking"])
            # second run reuses cache: no new downloads
            calls_before = len(fetcher.session.calls)
            run("https://va.mite.gov.it/it-IT/Oggetti/Info/77", d + "/store", str(data), ocr=False,
                get_html=lambda u: site[u], fetcher=fetcher, log=lambda m: None)
            self.assertEqual(len(fetcher.session.calls), calls_before)

    def test_canonical_exact_url_not_substring(self):
        recs = [{"url": "https://va.mite.gov.it/it-IT/Oggetti/Info/8788", "project_key": "a"},
                {"url": "https://va.mite.gov.it/it-IT/Oggetti/Info/11492", "project_key": "url:7335001a607c8788"}]
        self.assertEqual(find_by_url(recs, "https://va.mite.gov.it/it-IT/Oggetti/Info/8788")["match_count"], 1)


class RegressionsFromFirstLiveRun(unittest.TestCase):
    """Cases observed in the first CI run on the two MASE benchmarks (run 39262291923)."""

    def test_address_snc_is_not_a_company(self):
        facts = extract_all([page("Soprintendenza ABAP - Caserma Pasubio Via Pavia snc - PEC sabap-al@pec.cultura.gov.it")])
        subj = [f.subject for f in facts if f.field == "contact.pec"]
        self.assertTrue(subj and all(x and x.startswith("Soprintendenza") for x in subj), subj)

    def test_person_is_never_proponent(self):
        facts = extract_all([page("Committente: Dott. Geol. Gian Mario Repregosi")])
        self.assertFalse([f for f in facts if f.field == "role.proponent"])

    def test_vat_in_title_block_below_name(self):
        facts = extract_all([page("MARGISOLAR S.r.l.\nVia Roma 1 - 15057 Tortona (AL)\nP.IVA 03920631201")])
        vat = [f for f in facts if f.field == "company.vat"][0]
        self.assertEqual(vat.subject, "MARGISOLAR S.r.l.")
        self.assertEqual(vat.qualifiers["subject_association"], "previous_line_2")

    def test_component_power_not_plant(self):
        facts = extract_all([page("Il sottocampo 3 ha potenza pari a 8,16 MWp.\nPotenza complessiva dell'impianto 60,00865 MWp")])
        scopes = {(f.value, f.subject) for f in facts if f.field == "tech.pv_dc_mwp"}
        self.assertIn((8.16, "component"), scopes)
        self.assertIn((60.00865, "plant"), scopes)

    def test_vertex_list_restarting_is_split(self):
        rows = "\n".join(["Coordinate vertici UTM WGS84 32N", "Area 2",
                           "1 481949 4964811", "2 482257 4964814", "3 482318 4964439", "4 481906 4964438",
                           "1 482309 4964835", "2 482549 4964902", "3 482467 4964275", "4 482394 4964251"])
        polys = [f for f in extract_all([page(rows)]) if f.field == "geo.area_polygon"]
        self.assertEqual(sorted(p.subject for p in polys), ["2#1", "2#2"])

    def test_notifica_fields_are_template(self):
        txt = "Durata effettiva dei lavori\nInizio lavori: 01/09/2026\nFine lavori: 30/08/2028\nIndirizzo del cantiere"
        dates = [f for f in extract_all([page(txt)]) if f.field.startswith("schedule.date")]
        self.assertTrue(dates and all(d.temporal == "template_reference" for d in dates))

    def test_old_circular_dates_are_ignored(self):
        txt = "Collaudo e circolari: Circolare N. 9/2001 del 12/01/2001 - Riflessi"
        self.assertFalse([f for f in extract_all([page(txt)]) if f.field.startswith("schedule.date")])

    def test_title_with_prescriptions(self):
        h = ('<p>Oggetto: Progetto impianto "Tortona 2" da realizzarsi nei Comuni di Tortona (AL), Pozzolo Formigaro (AL) '
             'e Frugarolo (AL). - Prescrizioni: 1 a) b) Proponente: Margisolar S.r.l. Tipologia di opera: x Regione: Piemonte</p>')
        ident = MaseAdapter(get_html=lambda u: "").parse_info(h, "u", "1")
        self.assertEqual(ident.municipalities, ["Tortona", "Pozzolo Formigaro", "Frugarolo"])
        self.assertEqual(ident.region, "Piemonte")


class RegressionsFromSecondLiveRun(unittest.TestCase):
    """Run 39266134203 (Tortona 2 digest)."""

    def test_latlon_split_across_table_cells(self):
        txt = "Sito IMPIANTO Comune di Tortona\nLatitudine N\nLongitudine E\nSpigolo N/E\n44.84600\n8.78820\nSpigolo S/O\n44.82932\n8.77739"
        pts = [f for f in extract_all([page(txt)]) if f.field == "geo.latlon_point"]
        self.assertEqual([(p.value["lat"], p.value["lon"]) for p in pts], [(44.846, 8.7882), (44.82932, 8.77739)])
        self.assertTrue(all(p.qualifiers["object"] == "site_bbox_corner" for p in pts))

    def test_public_body_vat_not_given_to_company_above(self):
        txt = "MARGISOLAR S.r.l.\nOggetto: parere\nArpa Piemonte Codice Fiscale – Partita IVA 07176380017"
        vat = [f for f in extract_all([page(txt)]) if f.field == "company.vat"][0]
        self.assertEqual(vat.qualifiers["subject_association"], "same_line_public_body")
        self.assertTrue(vat.subject.lower().startswith("arpa"))

    def test_energy_yield_is_not_bess(self):
        facts = extract_all([page("La produzione annua stimata è pari a 98.000 MWh/anno")])
        self.assertFalse([f for f in facts if f.field == "tech.bess_energy_mwh"])

    def test_internal_mv_is_not_grid_voltage(self):
        facts = extract_all([page("Le cabine di campo sono collegate in MT a 30 kV fino alla stazione di connessione RTN")])
        self.assertEqual({f.field for f in facts if "kv" in f.field}, {"grid.internal_mv_kv"})

    def test_admin_deadlines_are_not_works(self):
        facts = extract_all([page("Il proponente dovrà trasmettere entro 30 (trenta) giorni dall'inizio lavori la comunicazione")])
        self.assertFalse([f for f in facts if f.field.startswith("schedule.duration")])

    def test_competing_proponents_form_one_conflict(self):
        facts = extract_all([page("Proponente: MARGISOLAR S.r.l.", doc="a"), page("Il committente è la società MARGISOLAR ENERGY S.r.l.", doc="b")])
        g = [x for x in group(facts) if x["field"] == "role.proponent"][0]
        self.assertEqual(g["status"], "conflict")
        self.assertFalse([p for p in build_proposal(facts, group(facts)) if p["field"] == "role.proponent"])


class RegressionsFromThirdLiveRun(unittest.TestCase):
    def test_accumulo_is_not_cumulative_impact(self):
        facts = extract_all([page("Impianto agrivoltaico di potenza pari a 54,5 MW, con sistema di accumulo integrato da 10 MW")])
        bess = [f for f in facts if f.field == "tech.bess_power_mw"]
        self.assertTrue(bess and all(f.subject != "other_project" for f in bess))
        self.assertEqual([f.value for f in bess], [10.0])  # 54,5 MW is the PV plant, not storage

    def test_failed_ocr_pages_are_retried(self):
        with tempfile.TemporaryDirectory() as d:
            store = Store(d)
            raw = make_pdf(["Relazione tecnica impianto agrivoltaico potenza 60 MWp lato DC con moduli", ""])
            store.put_text("s9", {"sha256": "s9", "member": None, "backend": "x", "backend_errors": [], "page_count": 2,
                                  "pages": [{"n": 1, "text": "Relazione tecnica impianto agrivoltaico potenza 60 MWp lato DC con moduli",
                                             "method": "digital", "chars": 60, "error": None},
                                            {"n": 2, "text": "", "method": "ocr_failed", "chars": 0, "error": "ocr: timeout"}],
                                  "ocr_pending": [], "dpi": 300, "summary": {}})
            import doc_intel.parse as P
            orig = P._ocr_page
            P._ocr_page = lambda b, n, t, dpi, l: ("testo ricavato da ocr " * 5, "ocr", None)
            try:
                out = extract_pages(raw, "s9", store, ocr=True)
            finally:
                P._ocr_page = orig
            self.assertEqual(out["summary"]["ocr"], 1)
            self.assertEqual(out["summary"]["ocr_failed"], 0)


class Registry(unittest.TestCase):
    def test_vies_resolves_margisolar_luisolar(self):
        from doc_intel.registry import ViesClient, check_vats
        reg = {"03920651209": {"isValid": True, "name": "MARGISOLAR S.R.L.", "address": "VIALE ANTONIO SILVANI 2 40122 BOLOGNA BO"},
               "03920631201": {"isValid": True, "name": "LUISOLAR ENERGY S.R.L.", "address": "VIALE ANTONIO SILVANI 2 40122 BOLOGNA BO"}}
        client = ViesClient(get_json=lambda url: reg[url.rsplit("/", 1)[-1]], pause=0)
        facts = extract_all([page("Proponente: MARGISOLAR S.r.l. P.IVA 03920651209", doc="a"),
                             page("MARGISOLAR S.r.l.\nVia X\nP.IVA 03920631201", doc="b")])
        check_vats(facts, client)
        v = {f.value: f for f in facts if f.field == "company.vat"}
        self.assertTrue(v["03920651209"].qualifiers["registry_match"])
        self.assertEqual(v["03920631201"].status, "rejected")       # association error exposed, not re-assigned
        groups = group(facts)
        self.assertFalse([g for g in groups if g["field"] == "company.multiple_vat"])
        prop = [p for p in build_proposal(facts, groups) if p["field"] == "company.vat"]
        gates = proposal_gates(prop, facts, groups)
        self.assertEqual([g["blocking"] for g in gates], [False])


class VatGroups(unittest.TestCase):
    """Aliai (MASE 8824): Cherry Picking's own VAT is not active in VIES because it adheres to a VAT group."""

    def test_vat_group_and_inactive_member(self):
        from doc_intel.registry import ViesClient, check_vats
        txt = ("Proponente: Cherry Picking S.r.l.\nSede legale Corso Buenos Aires, 54 - 20124 Milano P.I. IT02016600898\n"
               "La società aderisce al gruppo IVA GRUPPO SONNEDIX P.IVA 11998240011")
        facts = extract_all([page(txt)])
        fields = {(f.field, f.value) for f in facts if f.field.startswith("company.")}
        self.assertIn(("company.vat_group", "11998240011"), fields)
        reg = {"02016600898": {"isValid": False, "name": ""}, "11998240011": {"isValid": True, "name": "GRUPPO SONNEDIX"}}
        check_vats(facts, ViesClient(get_json=lambda u: reg[u.rsplit("/", 1)[-1]], pause=0))
        member = [f for f in facts if f.value == "02016600898"][0]
        self.assertNotEqual(member.status, "rejected")
        self.assertIn("VAT group", member.limits)


class RowTables(unittest.TestCase):
    def test_field_centres_in_row_table(self):
        txt = ("Di seguito le coordinate (WGS84) del punto centrale di ogni campo:\nAliai 1 Aliai 2 Aliai 3\n"
               "Latitudine 37,4512 37,4630 37,4988\nLongitudine 14,7001 14,7310 14,7652")
        pts = [f for f in extract_all([page(txt)]) if f.field == "geo.latlon_point"]
        self.assertEqual([(p.subject, p.value["lat"], p.value["lon"]) for p in pts],
                         [("Aliai 1", 37.4512, 14.7001), ("Aliai 2", 37.463, 14.731), ("Aliai 3", 37.4988, 14.7652)])
        self.assertTrue(all(p.qualifiers["object"] == "centroid" for p in pts))


class Probe(unittest.TestCase):
    def test_pick_samples_prefers_procedure_urls_and_excludes_mase(self):
        from doc_intel.probe import pick_samples
        recs = [{"source": "piemonte", "url": "http://x/archive", "power_mw": 5},
                {"source": "piemonte", "url": "http://x/archive", "power_mw": 9},
                {"source": "veneto", "url": "https://s/index.php/s/AAAA", "power_mw": 1},
                {"source": "veneto", "url": "https://s/index.php/s/BBBB", "power_mw": 7},
                {"source": "mase", "url": "https://va.mite.gov.it/x"}]
        s = pick_samples(recs)
        self.assertEqual(sorted(s), ["piemonte", "veneto"])
        self.assertFalse(s["piemonte"]["url_identifies_procedure"])
        self.assertEqual(s["veneto"]["url"], "https://s/index.php/s/BBBB")

    def test_classify_links(self):
        from doc_intel.probe import classify_links
        html = ('<html><title>Pratica</title><a href="/doc/rel.pdf">Relazione</a><a href="?pagina=2">2</a>'
                '<a href="https://app.box.com/s/abcdefghij">box</a><a href="/chi-siamo">chi siamo</a></html>')
        c = classify_links(html, "https://ente.it/pratica/1")
        self.assertEqual(c["doc_link_count"], 2)
        self.assertEqual(len(c["pager_links"]), 1)


class RegionalAdapters(unittest.TestCase):
    def test_emilia_romagna_html_index(self):
        from doc_intel.adapters import adapter_for_url
        url = "https://serviziambiente.regione.emilia-romagna.it/viavasweb/ricerca/dettaglio/6902"
        html = ('<h1>Impianto agrivoltaico X - Procedura di VIA</h1><h3>Documenti Presenti</h3>'
                '<div><a href="#p1">Progetto Iniziale</a> <span>↴</span> 2 documenti</div><table>'
                '<tr><td>03/07/2026</td><td><a href="/viavasweb/pubblicazioni/scaricaAllegato/589650/6902">Relazione tecnica</a></td></tr>'
                '<tr><td>03/07/2026</td><td><a href="/viavasweb/pubblicazioni/scaricaAllegato/589653/6902">Layout</a></td></tr></table>'
                '<div><a href="#p2">Pareri</a> ↴ 2 documenti</div><a href="/viavasweb/attivazione/scaricaAllegato/102002/6902">Parere</a>'
                '<a href="/viavasweb/ricerca">Nuova ricerca</a>')
        ad = adapter_for_url(url)
        ad.get_html = lambda u: html
        disc = ad.discover(ad.identify(url))
        self.assertEqual(disc.identity.project_id, "6902")
        self.assertEqual(len(disc.documents), 3)
        self.assertEqual({p["id"]: (p["declared"], p["enumerated"]) for p in disc.procedures},
                         {"Progetto Iniziale": (2, 2), "Pareri": (2, 1)})   # one declared attachment missing is visible
        self.assertEqual(disc.documents[0].extra["index_date"], "2026-07-03")

    def test_veneto_nextcloud_webdav_tree(self):
        from doc_intel.adapters import adapter_for_url
        url = "https://sharing.regione.veneto.it/index.php/s/iMFk8Zs5t86GYP7"
        def resp(href, coll=False, size=None):
            prop = "<d:resourcetype><d:collection/></d:resourcetype>" if coll else f"<d:resourcetype/><d:getcontentlength>{size}</d:getcontentlength>"
            return f"<d:response><d:href>{href}</d:href><d:propstat><d:prop>{prop}</d:prop></d:propstat></d:response>"
        root = "/public.php/webdav"
        listing = {"/": [resp(root + "/", True), resp(root + "/Elaborati/", True), resp(root + "/Istanza.pdf", size=100)],
                   "/Elaborati/": [resp(root + "/Elaborati/", True), resp(root + "/Elaborati/Relazione%20tecnica.pdf", size=200)]}
        def propfind(u, token):
            self.assertEqual(token, "iMFk8Zs5t86GYP7")
            path = u.split("/public.php/webdav", 1)[1] or "/"
            return '<d:multistatus xmlns:d="DAV:">' + "".join(listing[path.replace("%2F", "/")]) + "</d:multistatus>"
        ad = adapter_for_url(url)
        ad.propfind = propfind
        ad.get_html = lambda u: "<title>PROC_62-26 - Sharing Regione Veneto</title>"
        ident = ad.identify(url)
        disc = ad.discover(ident)
        self.assertEqual(ident.title, "PROC_62-26 - Sharing Regione Veneto")
        self.assertEqual([d.title for d in disc.documents], ["Elaborati/Relazione tecnica.pdf".rsplit("/", 1)[-1], "Istanza.pdf"])
        self.assertEqual(disc.documents[0].extra["auth"], ["iMFk8Zs5t86GYP7", ""])


class RegionalRunRegressions(unittest.TestCase):
    """Veneto PROC_62-26 and Emilia-Romagna 6902 runs."""

    def test_canonical_tenfold_power_flagged(self):
        facts = extract_all([page("Impianto agrivoltaico di potenza complessiva 26.617,76 kWp", doc=str(i)) for i in range(5)] +
                            [page("potenza complessiva dell'impianto 266,61776 MWp", doc="x")])
        rows = {r["field"]: r for r in compare({"power_mw": 266.618}, group(facts), {})}
        self.assertEqual(rows["power_mw"]["class"], "conflittuale")
        self.assertAlmostEqual(rows["power_mw"]["proposal"]["set"], 26.61776)

    def test_referenced_act_dates_and_cod_cer(self):
        txt = ("Secondo il preventivo di connessione (cod. 202500554) del 10/04/2025, l'entrata in esercizio avverrà dopo l'AU\n"
               "Conferimento a discarica (cod. CER 17.03.02) 20/03/2026")
        self.assertFalse([f for f in extract_all([page(txt)]) if f.field.startswith("schedule.date")])


class ErCountNodes(unittest.TestCase):
    def test_counts_as_separate_text_nodes(self):
        from doc_intel.adapters import adapter_for_url
        url = "https://serviziambiente.regione.emilia-romagna.it/viavasweb/ricerca/dettaglio/6902"
        html = ('<div><a href="#a">Progetto Iniziale</a> <i class="fa"></i> <b>95 documenti</b></div>'
                '<table><tr><td>03/07/2026</td><td><a href="/viavasweb/progettoIniziale/scaricaAllegato/1/6902">Rel</a></td></tr></table>'
                '<div><a href="#b">Pareri</a> <i class="fa"></i> <b>10 documenti</b></div>'
                '<a href="/viavasweb/pareri/scaricaAllegato/2/6902">Parere</a>')
        ad = adapter_for_url(url)
        ad.get_html = lambda u: html
        disc = ad.discover(ad.identify(url))
        self.assertEqual({p["id"]: (p["declared"], p["enumerated"]) for p in disc.procedures},
                         {"Progetto Iniziale": (95, 1), "Pareri": (10, 1)})


class BoxAdapter(unittest.TestCase):
    def test_box_listing_pages_and_subfolders(self):
        import json as _j
        from doc_intel.adapters import adapter_for_url
        def page_html(items, page_count=1, total=None):
            data = {"/app-api/enduserapp/shared-folder": {"items": items, "pageCount": page_count, "totalCount": total}}
            return f"<script>Box.postStreamData = {_j.dumps(data)};</script>"
        root = "https://regionelazio.app.box.com/v/VIA-1-2020"
        site = {root: page_html([{"typedID": "f_1", "name": "Istanza.pdf"}, {"typedID": "d_9", "name": "Elaborati"}], 2, 3),
                root + "?page=2": page_html([{"typedID": "f_2", "name": "Relazione.pdf"}], 2, 3),
                "https://regionelazio.app.box.com/v/VIA-1-2020/folder/9": page_html([{"typedID": "f_3", "name": "Layout.pdf"}], 1, 1)}
        ad = adapter_for_url(root)
        ad.get_html = lambda u: site[u]
        disc = ad.discover(ad.identify(root))
        self.assertEqual(sorted(d.title for d in disc.documents), ["Istanza.pdf", "Layout.pdf", "Relazione.pdf"])
        self.assertIn("file_id=f_3", [d.url for d in disc.documents if d.title == "Layout.pdf"][0])
        top = [p for p in disc.procedures if p["id"] == "/"][0]
        self.assertEqual((top["declared"], top["enumerated"], top["index_pages"]), (3, 3, 2))


class BoxGuards(unittest.TestCase):
    def test_folder_url_returning_parent_stops_recursion(self):
        import json as _j
        from doc_intel.adapters import adapter_for_url
        root = "https://regionelazio.app.box.com/v/VIA-2-2020"
        listing = {"x": {"items": [{"typedID": "d_1", "name": "A"}, {"typedID": "d_2", "name": "B"},
                                   {"typedID": "d_3", "name": "C"}, {"typedID": "d_4", "name": "D"},
                                   {"typedID": "f_9", "name": "Istanza.pdf"}], "pageCount": 1}}
        html = f"<script>Box.postStreamData = {_j.dumps(listing)};</script>"
        calls = []
        ad = adapter_for_url(root)
        ad.get_html = lambda u: (calls.append(u), html)[1]   # every folder URL returns the root listing
        disc = ad.discover(ad.identify(root))
        self.assertLessEqual(len(calls), 4)
        self.assertTrue(any("parent listing" in e for e in disc.errors))  # after 3 echoes recursion stops
        self.assertEqual([d.title for d in disc.documents], ["Istanza.pdf"])

    def test_er_external_observation_link(self):
        from doc_intel.adapters.regional import HTML_RULES
        self.assertTrue(HTML_RULES[0].doc_link.search(
            "https://serviziambiente.regione.emilia-romagna.it/viavasweb/osservazioni/scaricaAllegatoEsternoFt/54050/6902"))


class LazioRunRegressions(unittest.TestCase):
    def test_self_reference_folder_does_not_stop_siblings(self):
        import json as _j
        from doc_intel.adapters import adapter_for_url
        root = "https://regionelazio.app.box.com/v/VIA-3-2020"
        def h(items):
            return f"<script>Box.postStreamData = {_j.dumps({'x': {'items': items, 'pageCount': 1}})};</script>"
        top = [{"typedID": "d_100", "name": "060-2019"}, {"typedID": "d_1", "name": "Integrazioni"},
               {"typedID": "f_9", "name": "Istanza.pdf"}]
        site = {root: h(top), root + "/folder/100": h(top), root + "/folder/1": h([{"typedID": "f_5", "name": "Rel.pdf"}])}
        ad = adapter_for_url(root)
        ad.get_html = lambda u: site[u]
        disc = ad.discover(ad.identify(root))
        self.assertEqual(sorted(d.title for d in disc.documents), ["Istanza.pdf", "Rel.pdf"])

    def test_extension_act_is_event_not_start(self):
        f = [x for x in extract_all([page("Proroga inizio lavori - D.D. n.2620 del 17/12/2021")]) if x.field.startswith("schedule.date")]
        self.assertEqual([(x.field, x.value, x.temporal) for x in f],
                         [("schedule.date.construction_start_extension", "2021-12-17", "event_occurred")])

    def test_lowercase_prefix_company(self):
        from doc_intel.extract.entities import find_companies
        self.assertEqual(find_companies("e-Solar S.r.l. - Società con unico socio")[0][1], "e-Solar S.r.l.")


class MemoryAndDeletion(unittest.TestCase):
    """Documents are deleted after analysis; memory of what was done drives re-runs."""

    def setUp(self):
        self.site = fake_site()
        self.pdfs = {U + "11": make_pdf(["MARGISOLAR S.r.l. P.IVA 03920651209 impianto agrivoltaico 60 MWp lato DC"]),
                     U + "12": make_pdf(["Progettista: Ing. Mario Rossi relazione tecnica di progetto impianto"]),
                     U + "13": make_pdf(["Relazione geologica dell'area di impianto e opere di connessione"]),
                     U + "21": make_pdf(["Parere della Soprintendenza sul progetto dell'impianto fotovoltaico"])}

    def _run(self, d, session, **kw):
        fetcher = Fetcher(Store(d), session=session, sleeper=lambda s: None)
        return run("https://va.mite.gov.it/it-IT/Oggetti/Info/77", d, None, ocr=False,
                   get_html=lambda u: self.site[u], fetcher=fetcher, log=lambda m: None, **kw)

    def test_bytes_deleted_facts_kept_and_reused(self):
        with tempfile.TemporaryDirectory() as d:
            s1 = FakeSession(dict(self.pdfs))
            d1 = self._run(d, s1)
            st = Store(d)
            self.assertEqual(st.size_report()["blobs"], 0)
            self.assertEqual(st.size_report()["text"], 0)
            self.assertGreater(st.size_report()["facts"], 0)
            self.assertTrue(any(f["field"] == "company.vat" for f in d1["facts"]))
            s2 = FakeSession(dict(self.pdfs))
            d2 = self._run(d, s2)
            self.assertEqual(s2.calls, [])                       # nothing downloaded again
            self.assertEqual(d2["acquisition"]["states"], {"reused": 4})   # 4 distinct URLs, 5 references
            self.assertEqual(len(d2["facts"]), len(d1["facts"]))  # dossier rebuilt from memory

    def test_reanalyse_extracts_again_even_if_bytes_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            self._run(d, FakeSession(dict(self.pdfs)))
            st = Store(d)
            for sha in [e["sha256"] for e in st.load_manifest("mase", "77")["documents"].values()]:
                p = st.get_facts(sha); p["facts"] = []; st.put_facts(sha, p)   # simulate stale facts
            s2 = FakeSession(dict(self.pdfs))
            d2 = self._run(d, s2, reanalyse=True)
            self.assertEqual(len(s2.calls), 4)
            self.assertTrue(any(f["field"] == "company.vat" for f in d2["facts"]))

    def test_changed_file_is_downloaded_again(self):
        with tempfile.TemporaryDirectory() as d:
            self._run(d, FakeSession(dict(self.pdfs)))
            new = make_pdf(["MARGISOLAR S.r.l. P.IVA 03920651209 impianto agrivoltaico 66 MWp lato DC revisione"])
            routes = dict(self.pdfs, **{U + "11": new})
            s2 = FakeSession(routes, head_headers={U + "11": {"Content-Length": str(len(new))}})
            d2 = self._run(d, s2)
            self.assertEqual(s2.calls, [U + "11"])
            self.assertEqual(d2["acquisition"]["states"].get("revised"), 1)
            man = Store(d).load_manifest("mase", "77")
            self.assertEqual(len(man["documents"][U + "11"]["history"]), 1)
            self.assertIn(66.0, [f["value"] for f in d2["facts"] if f["field"] == "tech.pv_dc_mwp"])

    def test_new_document_in_index_is_processed_and_removed_one_flagged(self):
        with tempfile.TemporaryDirectory() as d:
            self._run(d, FakeSession(dict(self.pdfs)))
            base = "https://va.mite.gov.it/it-IT/Oggetti/Documentazione/77/"
            self.site[base + "200"] = index_html(2, [22], 1)        # 21 removed, 22 added
            routes = dict(self.pdfs, **{U + "22": make_pdf(["Nuova integrazione documentale del proponente sul progetto"])})
            s2 = FakeSession(routes)
            d2 = self._run(d, s2)
            self.assertIn(U + "22", s2.calls)
            self.assertIn(U + "21", d2["acquisition"]["removed_from_index"])

    def test_incomplete_ocr_keeps_bytes_until_done(self):
        with tempfile.TemporaryDirectory() as d:
            routes = dict(self.pdfs, **{U + "13": make_pdf(["", ""])})
            fetcher = Fetcher(Store(d), session=FakeSession(routes), sleeper=lambda s: None)
            run("https://va.mite.gov.it/it-IT/Oggetti/Info/77", d, None, ocr=True, ocr_max_pages=0,
                get_html=lambda u: self.site[u], fetcher=fetcher, log=lambda m: None)
            man = Store(d).load_manifest("mase", "77")
            self.assertFalse(man["documents"][U + "13"]["processed"]["complete"])
            self.assertTrue(Store(d).has_blob(man["documents"][U + "13"]["sha256"]))


class SourcesVerification(unittest.TestCase):
    def test_report_rows_and_files(self):
        import doc_intel.sources as S
        recs = [{"source": "veneto", "url": "https://sharing.regione.veneto.it/index.php/s/AAAA", "power_mw": 5},
                {"source": "calabria", "url": "https://www.regione.calabria.it/bandi/x/", "power_mw": 5},
                {"source": "sicilia", "url": "https://si-vvi.regione.sicilia.it/x", "power_mw": 5}]
        def fake_probe(url, render=None):
            err = "timeout" if "sicilia" in url else None
            return {"http": {"error": "timed out", "error_class": err} if err else {"status": 200, "doc_link_count": 0},
                    "verdict": {"status": "x", "method": "y"}}
        fake_dossier = {"acquisition": {"references": 3, "processed_references": 3, "download_failures": 0,
                                        "pages_total": 10, "pages_ocr": 0},
                        "facts": [{"field": "role.proponent"}], "canonical": {"status": "unique"},
                        "gates": {"acquisition": {"issues": []}}}
        import doc_intel.pipeline as P
        orig_probe, orig_run = S.probe_url, P.run
        S.probe_url, P.run = fake_probe, (lambda *a, **k: fake_dossier)
        try:
            with tempfile.TemporaryDirectory() as d:
                data = Path(d) / "data.json"
                data.write_text(json.dumps({"records": recs}))
                rows = {r["source"]: r for r in S.verify_sources(str(data), d + "/st", d + "/out", log=lambda m: None)}
                self.assertEqual(rows["veneto"]["status"], "supportata")
                self.assertEqual(rows["calabria"]["status"], "da sviluppare (nessun adapter)")
                self.assertEqual(rows["sicilia"]["status"], "non raggiungibile")
                self.assertTrue((Path(d) / "out" / "fonti_latest.md").is_file())
        finally:
            S.probe_url, P.run = orig_probe, orig_run


class CardFieldsFromReview(unittest.TestCase):
    """Fields requested on 20/10/2026: available vs panel area, piles (tracker distinction), lots, Gantt phases."""

    def test_real_document_wording(self):
        t = ("La centrale è articolata in due sezioni elettriche\n"
             "Pali videocamere (n.46)\n"
             "Attacco palo: idoneo per pali di diametro da min. 42mm\n"
             "superficie complessiva proiettata al suolo di 16,898 ha.\n"
             "Il terreno reso disponibile per l'intervento ha una superficie pari a circa 101,39 ettari\n"
             "determinano un numero complessivi massimo di pali infissi al suolo pari a 8.783.")
        got = {(f.field, f.subject, f.value) for f in extract_all([page(t)])
               if f.field.startswith(("tech.area", "tech.piles", "tech.lots"))}
        self.assertEqual(got, {("tech.area_panels_ha", None, 16.898), ("tech.area_available_ha", None, 101.39),
                               ("tech.piles_count", "totale", 8783)})

    def test_area_piles_lots(self):
        t = ("L'impianto è suddiviso in 4 lotti recintati per una superficie complessiva di 110 ha.\n"
             "La superficie occupata dai moduli è pari a 48,5 ha.\n"
             "Saranno infissi n. 12.450 pali per i tracker monoassiali e 820 pali per la recinzione.\n"
             "Il numero complessivo di pali da infiggere è pari a 13.270\n"
             "Moduli conformi alla norma CEI EN 61215")
        got = {(f.field, f.subject, f.value) for f in extract_all([page(t)])
               if f.field.startswith(("tech.area", "tech.piles", "tech.lots", "tech.module_count"))}
        self.assertEqual(got, {("tech.area_available_ha", None, 110.0), ("tech.area_panels_ha", None, 48.5),
                               ("tech.lots_count", None, 4), ("tech.piles_count", "tracker", 12450),
                               ("tech.piles_count", "recinzione", 820), ("tech.piles_count", "totale", 13270)})

    def test_gantt_phases(self):
        t = ("CRONOPROGRAMMA DEI LAVORI\nAllestimento cantiere dal 1° al 2° mese\nInfissione pali mesi 2-6\n"
             "Posa moduli dal mese 4 al mese 10\nCollaudi e messa in servizio 11-12")
        ph = [f.value for f in extract_all([page(t)]) if f.field == "schedule.phase"]
        self.assertEqual([(p["name"], p["start_month"], p["end_month"]) for p in ph],
                         [("Allestimento cantiere", 0, 2), ("Infissione pali", 1, 6), ("Posa moduli", 3, 10),
                          ("Collaudi e avviamento", 10, 12)])


class NoHardcodedBenchmarks(unittest.TestCase):
    def test_toolbox_has_no_benchmark_ids(self):
        for py in (ROOT / "doc_intel").rglob("*.py"):
            src = py.read_text(encoding="utf-8")
            for ident in ("6479", "8788", "11109", "16669", "19438", "12932"):
                self.assertNotRegex(src, rf"\b{ident}\b", f"{ident} hard-coded in {py}")


class GoldFiles(unittest.TestCase):
    def test_gold_files_are_well_formed(self):
        for p in (ROOT / "doc_intel" / "gold").glob("*.json"):
            g = json.loads(p.read_text(encoding="utf-8"))
            self.assertTrue(g["items"])
            for item in g["items"]:
                self.assertTrue({"field", "value", "document"} <= item.keys(), item)
            vats = [i["value"] for i in g["items"] if i["field"] == "company.vat"]
            self.assertTrue(all(valid_vat_it(v) for v in vats))


if __name__ == "__main__":
    unittest.main()


class CardContacts(unittest.TestCase):
    def _fact(self, field, value, subject=None, excerpt=""):
        return {"field": field, "value": value, "subject": subject, "qualifiers": {},
                "citation": {"url": "u", "page": 1, "excerpt": excerpt}}

    def test_public_bodies_out_and_owner_from_address(self):
        from doc_intel.card import _subjects
        d = {"facts": [self._fact("contact.pec", "pianidellamarina@pec.it", "e-Solar S.r.l."),
                       self._fact("contact.pec", "provinciavt@legalmail.it", "e-Solar S.r.l."),
                       self._fact("contact.phone", "+390651689356", "e-Solar S.r.l.", "TEL +39.06.51689356 WWW.REGIONE.LAZIO.IT"),
                       self._fact("contact.phone", "0761483246", None, "tel . 0761483246; pangeastudio@virgilio.it"),
                       self._fact("contact.email", "agroambiente@tor.it", "MARGISOLAR S.r.l."),
                       self._fact("company.vat", "04390411009", "Piani della Marina"),
                       self._fact("company.vat", "04390411009", "Piani della Marina")],
             "groups": [],
             "registry": {"04390411009": {"valid": True, "name": "SOCIETA' AGICOLA PIANI DELLA MARINA S.S."}}}
        d["facts"][4]["qualifiers"] = {"subject_association": "previous_line_2", "person": "Dott. agronomo DELIO BARBIERI",
                                       "person_association": "same_line"}
        main, others, dropped = _subjects(d, {}, "MARGISOLAR S.r.l.", "u")
        allc = {c["value"]: e["name"] for e in main + others for c in e["contacts"]}
        self.assertEqual(allc["pianidellamarina@pec.it"], "Societa' Agicola Piani Della Marina S.S.")
        self.assertIn("Barbieri", allc["agroambiente@tor.it"])        # signed by the agronomist, not the proponent
        self.assertNotIn("provinciavt@legalmail.it", allc)
        self.assertNotIn("+390651689356", allc)
        self.assertNotIn("0761483246", allc)                            # no owner: not shown
        self.assertEqual(dropped, 1)
        self.assertEqual([e["roles"] for e in main if "Margisolar" in e["name"]], [["Proponente"]])


class GanttFromDrawing(unittest.TestCase):
    """Programme of works drawn as bars: rebuilt from word positions and filled rectangles."""

    def _page(self):
        words = [(200 + 20 * v - 4, 46, 200 + 20 * v + 4, 54, str(v), 0, 0) for v in range(1, 13)]
        for i, n in enumerate(["1 Allestimento cantiere", "2 Infissione pali 60 gg", "3 Montaggio tracker e moduli", "4 Collaudi"]):
            words.append((20, 76 + 20 * i, 150, 84 + 20 * i, n, 1 + i, 0))
        X = lambda t: 200 + 20 * (t + 0.5)
        rects = [(X(0), 76, X(1), 84, (.2, .5, .8)), (X(1), 96, X(5), 104, (.2, .5, .8)),
                 (X(3), 116, X(9), 124, (.2, .5, .8)), (X(10), 136, X(12), 144, (.9, .3, .1)), (0, 0, 600, 800, (1, 1, 1))]
        return words, rects

    def test_rows_and_periods(self):
        from doc_intel.gantt_vector import reconstruct
        words, rects = self._page()
        g = reconstruct(words, rects, 600, 800, "Cronoprogramma dei lavori (mesi)")
        self.assertEqual(g["unit"], "month")
        self.assertEqual([(r["name"], r["start"], r["end"]) for r in g["rows"]],
                         [("Allestimento cantiere", 0.0, 1.0), ("Infissione pali", 1.0, 5.0),
                          ("Montaggio tracker e moduli", 3.0, 9.0), ("Collaudi", 10.0, 12.0)])

    def test_card_uses_drawn_phases(self):
        from doc_intel.extract.schedule import extract_gantt
        from doc_intel.card import _gantt
        from doc_intel.gantt_vector import reconstruct
        words, rects = self._page()
        ctx = page("Cronoprogramma")
        ctx.gantt = reconstruct(words, rects, 600, 800, "mesi")
        facts = [json.loads(json.dumps(f.__dict__, default=lambda o: o.__dict__)) for f in extract_gantt(ctx)]
        g = _gantt({"facts": facts, "groups": []}, {})
        self.assertEqual([b["name"] for b in g["bars"]][:2], ["Allestimento cantiere", "Infissione pali"])
        self.assertIsNone(g["note"])

    @unittest.skipUnless(__import__("importlib").util.find_spec("fitz") and __import__("importlib").util.find_spec("reportlab"),
                         "PyMuPDF and reportlab needed")
    def test_real_pdf_drawing(self):
        import io
        import fitz
        from reportlab.pdfgen import canvas
        from doc_intel.gantt_vector import from_fitz_page
        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=(600, 400))
        c.setFont("Helvetica", 8)
        c.drawString(20, 380, "CRONOPROGRAMMA DEI LAVORI - mesi")
        for v in range(1, 13):
            c.drawCentredString(200 + 20 * v, 350, str(v))
        spans = [("Allestimento cantiere", 0, 1), ("Infissione pali", 1, 5), ("Montaggio strutture", 3, 9), ("Collaudi", 10, 12)]
        for i, (n, a, b) in enumerate(spans):
            y = 320 - 20 * i
            c.drawString(20, y, n)
            c.setFillColorRGB(.2, .5, .8)
            c.rect(200 + 20 * (a + 0.5), y - 2, 20 * (b - a), 9, fill=1, stroke=0)
            c.setFillColorRGB(0, 0, 0)
        c.save()
        doc = fitz.open(stream=buf.getvalue(), filetype="pdf")
        g = from_fitz_page(doc[0])
        self.assertEqual([(r["name"], round(r["start"]), round(r["end"])) for r in g["rows"]],
                         [(n, a, b) for n, a, b in spans])


class ProjectStatus(unittest.TestCase):
    """Past planned dates flag the project, graded by what was filed afterwards."""

    def _d(self, start=None, end=None, act=None, filed=None):
        g = []
        if start:
            g.append({"field": "schedule.date.construction_start", "values": [{"value": start, "temporal": ["planned"]}]})
        if end:
            g.append({"field": "schedule.date.construction_end", "values": [{"value": end, "temporal": ["planned"]}]})
        if act:
            g.append({"field": "schedule.date.construction_start_extension", "values": [{"value": act, "temporal": ["event_occurred"]}]})
        return {"groups": g, "inventory": [{"index_date": filed}] if filed else []}

    def test_levels(self):
        from datetime import date
        from doc_intel.card import _status
        t = date(2026, 10, 20)
        self.assertEqual(_status(self._d("2020-06-30", "2021-06-30"), {}, t)["level"], "potenzialmente_realizzato")
        self.assertEqual(_status(self._d("2020-06-30", "2021-06-30", act="2021-12-17", filed="2024-03-10"), {}, t)["level"],
                         "date_superate")
        self.assertEqual(_status(self._d("2026-09-01"), {}, t)["level"], "potenzialmente_avviato")
        self.assertEqual(_status(self._d("2027-03-01"), {}, t)["level"], "previsto")
        self.assertIsNone(_status(self._d(), {}, t))


class SiteLocation(unittest.TestCase):
    def test_text_formats(self):
        from doc_intel.extract.geo import extract as geo_extract
        t = ("UBICAZIONE Comune di Tarquinia (VT) COORDINATE 42° 19' 55,7\" N 11° 40' 22,8\" E\n"
             "Il sito è individuato dalle seguenti coordinate geografiche (EPSG:4326): Latitudine 45,091549° N; Longitudine 11,185199° E.\n"
             "Le coordinate chilometriche di Gauss Boaga fuso ovest del baricentro dell'area sono\nE 1452350 N 4519870")
        pts = [(round(f.value["lat"], 3), round(f.value["lon"], 3), f.qualifiers["object"]) for f in geo_extract(page(t))
               if f.field == "geo.latlon_point"]
        self.assertIn((42.332, 11.673, "site_point"), pts)
        self.assertIn((45.092, 11.185, "site_point"), pts)
        self.assertTrue(any(abs(a - 40.83) < 0.01 and abs(b - 8.43) < 0.01 for a, b, _ in pts))   # Gauss-Boaga, Sassari

    def test_geopdf_extent(self):
        from doc_intel.geopdf import extents

        class Doc:
            objs = {1: "<< /Type /Page /VP [ 2 0 R ] >>",
                    2: "<< /Type /Viewport /BBox [ 0 0 400 300 ] /Measure 3 0 R >>",
                    3: "<< /Subtype /GEO /GPTS [ 44.83 8.77 44.85 8.77 44.85 8.80 44.83 8.80 ] >>"}

            def xref_object(self, x, compressed=False):
                return self.objs[x]

        class Page:
            xref = 1

            class rect:
                width, height = 800, 600
        e = extents(Doc(), Page())
        self.assertEqual(e[0]["centre"], (44.84, 8.785))
        self.assertEqual(e[0]["share"], 0.25)

    def test_declared_point_beats_scattered_points_and_connection_kept_apart(self):
        from doc_intel.card import _site_point
        F = lambda lat, lon, obj: {"field": "geo.latlon_point", "value": {"lat": lat, "lon": lon},
                                   "qualifiers": {"object": obj}, "citation": {"url": "u", "page": 1, "excerpt": ""}}
        d = {"facts": [F(44.84, 8.78, "site_point"), F(44.841, 8.781, "unknown"), F(44.839, 8.779, "survey_point"),
                       F(45.20, 9.40, "unknown"), F(44.80, 8.70, "substation")]}
        point, kind, _, conn, lots = _site_point(d, {})
        self.assertEqual(lots, [])
        self.assertEqual(point, (44.84, 8.78))
        self.assertIn("dichiarate", kind)
        self.assertEqual((conn["lat"], conn["lon"]), (44.8, 8.7))


@unittest.skipUnless(__import__("importlib").util.find_spec("PIL"), "Pillow needed")
class GanttFromImage(unittest.TestCase):
    def test_bars_and_ocr_words(self):
        from PIL import Image, ImageDraw
        from doc_intel.gantt_vector import from_raster
        im = Image.new("RGB", (700, 300), "white")
        d = ImageDraw.Draw(im)
        words = []
        for v in range(1, 13):                          # month axis, cell centres at 200 + 30 v
            words.append((200 + 30 * v - 5, 20, 200 + 30 * v + 5, 32, str(v), 1, 1))
        spans = [("Allestimento cantiere", 0, 2), ("Infissione pali", 2, 6), ("Posa moduli", 4, 10), ("Collaudi", 10, 12)]
        for i, (n, a, b) in enumerate(spans):
            y = 60 + 40 * i
            words.append((10, y, 150, y + 12, n, 10 + i, 1))
            d.rectangle([200 + 30 * (a + 0.5), y, 200 + 30 * (b + 0.5) - 1, y + 12], fill=(30, 110, 190))
        tsv = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n" + "\n".join(
            f"5\t1\t{w[5]}\t1\t{w[6]}\t{k}\t{w[0]}\t{w[1]}\t{w[2] - w[0]}\t{w[3] - w[1]}\t90\t{t}"
            for w in words for k, t in enumerate(w[4].split(), 1))
        g = from_raster((im.tobytes(), 700, 300), tsv, "Cronoprogramma mesi")
        self.assertEqual([(r["name"], round(r["start"]), round(r["end"])) for r in g["rows"]], spans)


class LotCoordinates(unittest.TestCase):
    def test_one_point_per_lot(self):
        from doc_intel.card import _site_point
        F = lambda lat, lon, obj, lot: {"field": "geo.latlon_point", "value": {"lat": lat, "lon": lon}, "subject": lot,
                                        "qualifiers": {"object": obj}, "citation": {"url": "u", "page": 1, "excerpt": ""}}
        d = {"facts": [F(44.760, 11.670, "site_point", "Lotto A"), F(44.770, 11.690, "site_point", "Lotto B"),
                       F(44.771, 11.691, "unknown", "Lotto B"), F(44.765, 11.680, "unknown", None)]}
        point, _, _, _, lots = _site_point(d, {})
        self.assertEqual([(x["lotto"], x["lat"], x["lon"]) for x in lots], [("Lotto A", 44.76, 11.67), ("Lotto B", 44.77, 11.69)])
        self.assertIn("maps", lots[0]["maps_url"])

    def test_title_typo_power_replaced_by_documents(self):
        from doc_intel.card import _summary
        d = {"groups": [{"field": "tech.pv_dc_mwp", "subject": "plant", "values": [{"value": 26.61776, "occurrences": 863, "example": None}]}],
             "facts": []}
        tiles, open_points = _summary(d, {"title": "Impianto Melara con potenza nominale di 266617,76 KwP"}, {}, {})
        t = next(t for t in tiles if t["key"] == "potenza")
        self.assertEqual(t["value"], "26,618 MWp")
        self.assertIn("266617,76 KwP", t["sub"])


class GanttHierarchy(unittest.TestCase):
    """MS Project layout: indented names, duration column, 'Mese N' timescale, outlined (hatched) bars."""

    def test_levels_durations_axis_and_outlined_bars(self):
        from doc_intel.gantt_vector import reconstruct
        words = []
        for v in range(1, 13):
            words += [(200 + 20 * v - 14, 46, 200 + 20 * v - 3, 54, "Mese"), (200 + 20 * v - 2, 46, 200 + 20 * v + 2, 54, str(v))]
        rows = [("Fase di ingegneria", 20, "60 g", 0, 2, "f"), ("Rilievi", 30, "20 g", 0, 1, "sr"),
                ("Progetti esecutivi", 30, "40 g", 1, 2, "sr"), ("Posa FV", 20, "300 g", 2, 12, "f"),
                ("Posa pali", 30, "120 g", 2, 6, "sr")]
        rects = []
        X = lambda t: 200 + 20 * (t + 0.5)
        for i, (n, ind, dur, a, b, kind) in enumerate(rows):
            y = 76 + 20 * i
            words += [(ind, y, ind + 6 * len(n), y + 8, n, 10 + i, 0), (170, y, 185, y + 8, dur, 50 + i, 0)]
            rects.append((X(a), y, X(b), y + 8, (0.2, 0.3, 0.8) if kind == "f" else (0.5, 0.5, 0.5), kind))
        g = reconstruct(words, rects, 600, 400, "Cronoprogramma")
        got = [(r["name"], r["level"], r["summary"], r.get("duration"), round(r["start"]), round(r["end"])) for r in g["rows"]]
        self.assertEqual(got, [("Fase di ingegneria", 0, True, "60 g", 0, 2), ("Rilievi", 1, False, "20 g", 0, 1),
                               ("Progetti esecutivi", 1, False, "40 g", 1, 2), ("Posa FV", 0, True, "300 g", 2, 12),
                               ("Posa pali", 1, False, "120 g", 2, 6)])
        self.assertEqual(g["axis_labels"][0]["label"], "Mese 1")


class WorkQueue(unittest.TestCase):
    def _setup(self, d, records, cards=None, seen=None, attempts=None):
        data = Path(d) / "data.json"
        data.write_text(json.dumps({"records": records}), encoding="utf-8")
        mem = Path(d) / "mem"
        (mem / "schede").mkdir(parents=True)
        (mem / "queue").mkdir()
        for k, meta in (cards or {}).items():
            (mem / "schede" / meta["file"]).write_text(json.dumps({"project_key": k}))
        if seen is not None:
            (mem / "queue" / "seen.json").write_text(json.dumps(seen))
        if attempts is not None:
            (mem / "queue" / "attempts.json").write_text(json.dumps(attempts))
        return data, mem

    def test_new_first_then_backlog_by_power_skipping_cards_recent_and_local_sources(self):
        from datetime import date
        from doc_intel.queue import plan
        R = lambda k, u, mw: {"project_key": k, "url": u, "power_mw": mw}
        recs = [R("a", "https://va.mite.gov.it/it-IT/Oggetti/Info/1", 10), R("b", "https://va.mite.gov.it/it-IT/Oggetti/Info/2", 90),
                R("c", "https://va.mite.gov.it/it-IT/Oggetti/Info/3", 50), R("d", "https://si-vvi.regione.sicilia.it/x", 300),
                R("e", "https://va.mite.gov.it/it-IT/Oggetti/Info/5", 70), R("f", "https://va.mite.gov.it/it-IT/Oggetti/Info/6", 5),
                R("g", "https://dati.terna.it/x", 400)]
        with tempfile.TemporaryDirectory() as d:
            data, mem = self._setup(d, recs, cards={"c": {"file": "c.json"}}, seen={"a": "2026-01-01", "b": "2026-01-01",
                                    "c": "2026-01-01", "d": "2026-01-01", "e": "2026-01-01", "g": "2026-01-01"},
                                    attempts={"e": {"last": "2026-10-15", "runs": 1}})
            items = plan(data, mem, max_items=3, today=date(2026, 10, 21))
            self.assertEqual([(i["project_key"], i["reason"]) for i in items], [("f", "new"), ("b", "backlog"), ("a", "backlog")])
            att = json.loads((mem / "queue" / "attempts.json").read_text())
            self.assertEqual(att["b"]["last"], "2026-10-21")
            self.assertIn("f", json.loads((mem / "queue" / "seen.json").read_text()))

    def test_sync_downloads_only_changed_cards(self):
        from doc_intel.sync import sync_cards

        class R:
            def __init__(self, obj):
                self.text = json.dumps(obj)

            def json(self):
                return json.loads(self.text)

            def raise_for_status(self):
                pass
        calls = []
        tree = {"tree": [{"path": "schede/k1.json", "sha": "a1", "type": "blob"}, {"path": "schede/k2.json", "sha": "b2", "type": "blob"},
                         {"path": "schede/index.json", "sha": "zz", "type": "blob"}, {"path": "manifests/x.json", "sha": "c", "type": "blob"}]}

        def get(u):
            calls.append(u.rsplit("/", 1)[-1])
            return R(tree if "trees" in u else {"project_key": u.rsplit("/", 1)[-1][:2], "updated": "2026-10-21"})
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "k2.json").write_text(json.dumps({"project_key": "k2"}))
            (Path(d) / ".hashes.json").write_text(json.dumps({"k2.json": "b2"}))
            res = sync_cards(d, tree_url="https://api/git/trees/x", raw_base="https://raw/", get=get)
            self.assertEqual(res, {"downloaded": 1, "total": 2, "errors": 0})
            self.assertEqual(calls, ["x", "k1.json"])
            self.assertEqual(set(json.loads((Path(d) / "index.json").read_text())), {"k1", "k2"})


class AtosRegister(unittest.TestCase):
    """Toscana ATOS plant sheet: register act reported as written, documents from the linked procedure."""
    SHEET = ("<html><body>Scheda Impianto Energetico Denominazione Impianto AGRI PADULE Denominazione Ditta REVALUE PV1 S.R.L. "
             "Comune SOVICILLE Sigla Provincia SI Attivo SI --> Autorizzazione Vigente Tipo di Autorizzazione AU. FER "
             "Tipologia Fonte AGRIVOLTAICO Stato Autorizzazione --> Potenza MW 128.03 Dati dell'ultimo Atto Numero Atto 20450 "
             "Data Atto 25/09/2025 Ente Rilasciante Regione Toscana Categoria Atto Altro (VERIFICA DI ASSOGGETTABILITà) "
             "Tipologia Atto Decreto Ultima modifica 04/06/2026 11:54:03 --> In caso di atto il cui</body></html>")
    URL = "https://atos.arrr.it/scheda_impianto_fer.php?mn=fer&id_impianto=12481"

    def test_sheet_and_link(self):
        from doc_intel.adapters import adapter_for_url
        from doc_intel.adapters.api_portals import AtosAdapter
        a = adapter_for_url(self.URL)
        self.assertIsInstance(a, AtosAdapter)
        a.get_html = lambda u: self.SHEET
        ident = a.identify(self.URL)
        sh = ident.raw["register_sheet"]
        self.assertEqual(sh["power_mw"], "128.03")
        self.assertNotIn("authorization_status", sh)
        self.assertEqual(sh["act_category"], "Altro (VERIFICA DI ASSOGGETTABILITà)")
        a.records = [{"url": self.URL, "proponent": "REVALUE PV1 S.R.L."},
                     {"url": "https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici/110",
                      "proponent": "REVALUE PV 1 S.R.L.", "municipalities": "SOVICILLE", "power_mw": 128.0},
                     {"url": "https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici/88",
                      "proponent": "Altra S.r.l.", "municipalities": "Pisa", "power_mw": 128.0}]
        self.assertTrue(a.linked_record(ident)["url"].endswith("/110"))
        a.records = a.records[2:]
        disc = a.discover(ident)
        self.assertEqual(disc.documents, [])

    def test_card_reports_act_without_upgrading_it(self):
        from doc_intel.card import _register
        reg = _register({"info_url": self.URL, "raw": {"register_sheet": {
            "act_type": "Decreto", "act_number": "20450", "act_date": "25/09/2025",
            "act_category": "Altro (VERIFICA DI ASSOGGETTABILITà)", "act_body": "Regione Toscana"}}})
        self.assertEqual(reg["label"], "Registro impianti: Decreto n. 20450 del 25/09/2025")
        self.assertNotIn("autorizzat", (reg["label"] + reg["reason"]).lower())


class ApiPortalAdapters(unittest.TestCase):
    """Lombardia SILVIA and Toscana PATTI: documents listed by JSON APIs (structures read from the live portals)."""

    def test_silvia(self):
        from doc_intel.adapters.api_portals import SilviaAdapter
        url = "https://www.silvia.servizirl.it/silviaweb/#/scheda-sintesi?idTipoEnte=6&idTipoProcedura=3&idProgetto=20881&idProvenienza=2"
        api = {
            "https://www.silvia.servizirl.it/silviaweb/getSchedaSintesiByIdProgetto.html?idProgetto=20881": {
                "progetto": {"descrProgetto": "PROGETTO “CASCINA GRANDE” ... SITO NEL COMUNE DI DORNO (PV) CON POTENZA DI 121.696,68kWp",
                             "codProcedura": "VIA0058-PV", "dataDeposito": "14/04/2026", "ente": {"descrEnte": "PROVINCIA DI PAVIA"},
                             "macroStato": {"descrMacroStato": "Termini sospesi"}},
                "listProponenti": [{"descrEnteAzienda": "Azienda: Grande Rinnovabili S.r.l."}],
                "listaViewArchivioDocByProgetto": [
                    {"idDocumento": "134770", "titolo": "comunicazione 10 bis", "dataDocumento": "27/07/2026", "pubblico": "S",
                     "descrTipoDeposito": "10 BIS", "countAllegati": 1}]},
            "https://www.silvia.servizirl.it/silviaweb/getFileByDocumento.html?idDocumento=134770": [
                {"idEdma": "490137834", "nomeFile": "26.247letv_signed prot 46616.pdf", "dataUpload": "27/07/2026"}]}
        ad = SilviaAdapter(get_json=lambda u: api[u])
        self.assertTrue(SilviaAdapter.handles(url))
        ident = ad.identify(url)
        self.assertEqual((ident.proponent, ident.municipalities, ident.province), ("Grande Rinnovabili S.r.l.", ["Dorno"], "PV"))
        disc = ad.discover(ident)
        self.assertEqual(disc.documents[0].url, "https://www.silvia.servizirl.it/silviaweb/downloadDocumento.html?idEdma=490137834"
                                                "&nomeFile=26.247letv_signed%20prot%2046616.pdf")
        self.assertEqual(disc.documents[0].extra["index_date"], "2026-07-27")
        self.assertEqual((disc.procedures[0]["declared"], disc.procedures[0]["enumerated"]), (1, 1))

    def test_toscana_patti(self):
        from doc_intel.adapters.api_portals import ToscanaPattiAdapter
        content = {"opera": "AUTORIZZAZIONE UNICA ... AGRIVOLTAICO AVANZATO DI POTENZA PARI A 128 MWp", "proponente": "REVALUE PV 1 S.R.L.",
                   "comune": "SOVICILLE", "collegamenti": [{"titoloSezione": "AVVIO: avviso, documentazione iniziale",
                                                            "files": [{"nome": "00_elenco_elaborati.pdf",
                                                                       "collegamento": "https://servizi.patti.regione.toscana.it/gea-bo/avvisi/GetDocPubblico?idf=1"}]}]}
        api = {"id": 110, "contenuto": json.dumps(content), "procedimento": "Verifica di Assoggettabilità a VIA",
               "dataUltimoAggiornamento": "2025-09-29T08:52:23.058Z",
               "localizzazione": {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {
                   "type": "Polygon", "coordinates": [[[11.20, 43.25], [11.22, 43.25], [11.22, 43.27], [11.20, 43.27], [11.20, 43.25]]]}}]}}
        ad = ToscanaPattiAdapter(get_json=lambda u: api)
        ident = ad.identify("https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici/110")
        self.assertEqual((ident.proponent, ident.municipalities), ("REVALUE PV 1 S.R.L.", ["Sovicille"]))
        disc = ad.discover(ident)
        self.assertEqual([d.title for d in disc.documents], ["00_elenco_elaborati.pdf"])
        from doc_intel.pipeline import _portal_facts
        poly = _portal_facts(ident)
        self.assertEqual(poly[0].qualifiers["object"], "official_outline")
        from doc_intel.card import _site_point
        f = json.loads(json.dumps(poly[0].__dict__, default=lambda o: o.__dict__))
        point, kind, *_ = _site_point({"facts": [f]}, {})
        self.assertAlmostEqual(point[0], 43.258, places=2)
        self.assertIn("portale regionale", kind)
