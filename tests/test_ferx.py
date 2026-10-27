import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from doc_intel.ferx import parse_text, match, split_place

TXT = """1 FER500301 ENERGIA PULITA ITALIANA 2 S.R.L 04004641207 SICILIA CATANIA RAMACCA Nuova Costruzione 49,829% NO NO SI NO 12/9/25 11.31   42.000,000   42.000,000   42 .000,000
2 FER505966 KINGDOM SOLAR 3 S.R.L. 11445250969 LAZIO VITERBO PIANSANO Nuova Costruzione 38,456% NO NO NO NO 31/10/25 0.19   49.320,000   49.320,000   25 .000,000
3 FER500878 KI ITALIA S.R.L. 03079730309 FRIULI VENEZIA GIULIA UDINE PALMANOVA Potenziamento 34,931% NO NO SI NO NO 10/9/25 17.36   9.800,000
9.800,000   9.800,000
4 FER503553 SERTEKH 1 S.R.L. 03231640735 PUGLIA BARI GRAVINA IN PUGLIA Nuova Costruzione 38,747% NO NO NO NO 12/9/25 9.53   104.880,000   104.880,000"""


class Ferx(unittest.TestCase):
    def test_parse(self):
        rows = parse_text(TXT, "P")
        self.assertEqual([r["posizione"] for r in rows], [1, 2, 3, 4])
        self.assertEqual((rows[1]["kw"], rows[1]["kw_ammessi"]), (49320.0, 25000.0))
        self.assertEqual((rows[2]["regione"], rows[2]["provincia"], rows[2]["comune"]), ("Friuli Venezia Giulia", "Udine", "Palmanova"))
        self.assertEqual(rows[3]["comune"], "Gravina in Puglia")

    def test_match_rules(self):
        rows = parse_text(TXT, "P")
        recs = [{"project_key": "a", "municipalities": "Ramacca", "power_mw": 45, "proponent": "Energia Pulita Italiana 2 srl"},
                {"project_key": "b", "municipalities": "Piansano", "power_mw": 49.3, "proponent": "Altra Società srl"},
                {"project_key": "c", "municipalities": "Gravina in Puglia", "power_mw": 20, "proponent": "X"}]
        m = match(rows, recs, {"c": {"03231640735"}})
        self.assertEqual(m["a"][0]["basis"], "comune + proponente")
        self.assertNotIn("b", m)                     # same power, different company: not a match
        self.assertEqual(m["c"][0]["basis"], "P. IVA del proponente")


    def test_wind_matching_lots_and_companies(self):
        from doc_intel.ferx import match_wind
        row = lambda pos, soc, com, kw, proc="FER X transitorio – eolico (PC_EOL_2025_01)": {
            "posizione": pos, "codice": f"C{pos}", "ragione_sociale": soc, "piva": "1", "comune": com, "provincia": "",
            "regione": "", "categoria": "", "kw": kw, "kw_ammessi": kw, "procedura": proc}
        awarded = [row(1, "ERG WIND ENERGY S.R.L.", "Greci", 27000), row(2, "ERG WIND ENERGY S.R.L.", "Montaguto", 16800),
                   row(3, "ASJA AMBIENTE ITALIA S.R.L.", "Sclafani Bagni", 55000), row(4, "WINDFLOW SRL", "Sclafani Bagni", 9000),
                   row(5, "ERG SPA", "Greci", 27000, "FER X transitorio – fotovoltaico")]
        projects = [{"id": "gm", "name": "Greci-Montaguto", "mw": 43.8, "municipalities": ["Greci", "Montaguto"], "spv": "ERG"},
                    {"id": "as", "name": "Alia-Sclafani", "mw": 55, "municipalities": ["Sclafani Bagni"], "spv": "Asja Ambiente Italia"}]
        res = {r["posizione"]: (r["progetto"] or {}).get("id") for r in match_wind(awarded, projects)}
        self.assertEqual(res, {1: "gm", 2: "gm", 3: "as", 4: None})      # PV rows ignored, other company never matched


if __name__ == "__main__":
    unittest.main()
