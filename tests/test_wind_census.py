import sys, unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from doc_intel.wind_census import _power, build


def raw(oid, title, prop, mun, statuses=("In corso",), power=None):
    return {"external_id": f"mase_{oid}", "source_url": f"https://va.mite.gov.it/it-IT/Oggetti/Info/{oid}", "title": title,
            "payload": {"title": title, "proponent": prop, "municipalities": mun, "region": "Campania", "province": "BN",
                        "object_id": str(oid), "power": power}, "statuses": list(statuses), "procedures": []}


def ferx(pos, soc, com, kw):
    return {"posizione": pos, "codice": f"FER{pos}", "ragione_sociale": soc, "piva": "1", "comune": com, "provincia": "BN",
            "regione": "Campania", "kw": kw, "kw_ammessi": kw, "progetto": None}


class WindCensus(unittest.TestCase):
    def test_power(self):
        self.assertEqual(_power("parco eolico da 66 MW con accumulo da 20 MW"), 66.0)
        self.assertEqual(_power("accumulo da 30 MW e impianto eolico da 49,6 MW"), 49.6)
        self.assertEqual(_power("offshore da 1.110 MW"), 1110.0)

    def test_build(self):
        rows = [raw(1, "Impianto eolico 'Foiano' da 66 MW", "Edison Rinnovabili S.p.A.", ["Foiano di Val Fortore"]),
                raw(2, "Impianto eolico da 40 MW", "Alfa Wind S.r.l.", ["Molinara"], statuses=("Archiviata",)),
                raw(3, "Modifica impianto eolico da 30 MW", "Beta S.r.l.", ["Ginestra"], statuses=("Archiviata",)),
                raw(10751, "Parco eolico offshore da 900 MW", "Kailia S.r.l.", ["Brindisi"])]
        curated = [{"id": "off-kailia", "name": "Kailia", "mw": 900, "municipalities": ["Brindisi"], "stage_label": "Iter",
                    "sources": [{"url": "https://va.mite.gov.it/it-IT/Oggetti/Info/10751"}]}]
        fx = [ferx(14, "EDISON RINNOVABILI SPA", "Foiano di Val Fortore", 66000), ferx(19, "IVPC SRL", "Foiano di Val Fortore", 103700)]
        c = build(rows, curated, fx, today="2026-10-27")
        by = {r["project_key"]: r for r in c["records"]}
        self.assertNotIn("wind:mase_2", by)                    # all archived: dead project
        self.assertIn("wind:mase_3", by)                       # archived side procedure: kept
        self.assertEqual(by["wind:mase_1"]["ferx"][0]["posizione"], 14)
        self.assertEqual(by["wind:mase_10751"]["curato"]["id"], "off-kailia")
        self.assertIn("wind:ferx_FER19", by)                   # IVPC: another company, its own lead
        self.assertEqual(c["summary"]["archiviati_esclusi"], 1)


if __name__ == "__main__":
    unittest.main()


class WindPower(unittest.TestCase):
    def test_title_cases(self):
        self.assertEqual(_power("7 nuovi aerogeneratori, smantellamento degli attuali 19 aerogeneratori di potenza complessiva pari a 11,4 MW; "
                                "7 nuovi aerogeneratori della potenza unitaria di 6 MW"), 42.0)
        self.assertEqual(_power("costituito da 8 aerogeneratori, ciascuno di potenza nominale pari a 8 MW, da realizzarsi"), 64.0)
        self.assertEqual(_power("13 WTG della potenza di 93,60 MWp"), 93.6)
        self.assertEqual(_power("potenza massima complessiva di MW 144 nei comuni"), 144.0)
        self.assertEqual(_power("33 aerogeneratori della potenza nominale fino a 6 MW, per una potenza complessiva sino a 198 MW"), 198.0)
        self.assertEqual(_power("sistema di accumulo da 40 MWh e eolico 30 MW"), 30.0)

    def test_place_words_are_not_company_words(self):
        rows = [raw(5, "Impianto eolico da 43,2 MW", "Gruppo Visconti Brindisi S.r.l.", ["Brindisi"])]
        c = build(rows, [], [ferx(8, "BRINDISI SANTA TERESA S.R.L.", "Brindisi", 31050)], today="2026-10-27")
        self.assertEqual(c["records"][0]["ferx"] if c["records"][0]["source"] == "mase" else None, [])


class WindRegional(unittest.TestCase):
    def reg(self, src, ext, title, mun, prop=None, power_mw=None, status="In corso"):
        return {"external_id": f"{src}:{ext}", "source": src, "source_url": f"https://example.org/{ext}", "title": title,
                "payload": {"title": title, "proponent": prop, "municipalities": mun, "region": "Puglia", "province": "FG",
                            "status_raw": status, "procedure": "PAUR", "power_mw": power_mw}}

    def test_regional_added_merged_and_ferx(self):
        mase = [raw(1, "Impianto eolico da 66 MW", "Alfa Wind S.r.l.", ["Ascoli Satriano"])]
        regional = [self.reg("sistema_puglia_energia", 1, "Impianto eolico da 66 MW", ["Ascoli Satriano"], "Alfa Wind S.r.l."),
                    self.reg("puglia_opendata", 2, "Eolico - Candela (FG) - 16,5 MW", ["Candela"], None, 16.5),
                    self.reg("sistema_puglia_energia", 3, "Impianto eolico da 16,5 MW", ["Candela"], "Wind Energy Candela S.r.l.")]
        fx = [ferx(18, "WIND ENERGY CANDELA S.R.L.", "Candela", 16500)]
        c = build(mase, [], fx, today="2026-10-28", regional=regional)
        by = {r["project_key"]: r for r in c["records"]}
        self.assertEqual(c["summary"]["regionali_gia_al_mase"], 2)       # same plant at MASE / in two regional lists
        self.assertEqual(c["summary"]["da_regioni"], 1)
        cand = by["wind:puglia_opendata:2"]
        self.assertEqual(cand["ferx"][0]["posizione"], 18)
        self.assertEqual(c["summary"]["lead_ferx"], 0)
