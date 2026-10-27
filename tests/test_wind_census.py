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
