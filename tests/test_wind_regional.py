import sys, unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app.collectors.basilicata as bas
import app.collectors.sicilia as sic
from app.collectors import wind_regional as wr


class WindRegional(unittest.TestCase):
    def test_modules_restored(self):
        before = list(sic.COMMERCIAL_PV_KEYWORDS)
        with wr._patched(sic, wr.SPECS["sicilia"]["patch"]):
            col = sic.SiciliaCollector()
            self.assertTrue(col._is_commercial_pv_project("Progetto di un impianto eolico da 30 MW nel comune di Mazara del Vallo"))
            self.assertFalse(col._is_commercial_pv_project("Impianto fotovoltaico da 30 MW nel comune di Mazara del Vallo"))
        self.assertEqual(sic.COMMERCIAL_PV_KEYWORDS, before)        # the PV radar keeps its lists

    def test_basilicata_wind_relevance(self):
        values = dict(wr.SPECS["basilicata"]["patch"])
        values["EXCLUDE_KEYWORDS"] = wr._without_wind(bas.EXCLUDE_KEYWORDS)
        with wr._patched(bas, values):
            col = bas.BasilicataCollector()
            wr._windify("basilicata", col, "Basilicata")
            text = "Impianto eolico da 33 MW nei comuni di Pietragalla e Cancellara (PZ) - Proponente Alfa Wind S.r.l."
            mun = col._extract_municipalities(text)
            self.assertIn("Pietragalla", mun)
            item = {"title": text, "proponent": "Alfa Wind S.r.l.", "power_mw": 33.0, "municipalities": mun,
                    "province": col._extract_province(text, mun), "plain_text_sample": text, "procedure": "VIA"}
            self.assertEqual(item["province"], "PZ")
            self.assertTrue(col._is_relevant(item))
        self.assertIn("eolico", bas.EXCLUDE_KEYWORDS)


if __name__ == "__main__":
    unittest.main()
