import unittest

try:
    from app.collectors.sardegna import SardegnaCollector
except ImportError:  # collector dependencies (dotenv) not installed in this environment
    SardegnaCollector = None


@unittest.skipIf(SardegnaCollector is None, "collector dependencies missing")
class SardegnaProponentTest(unittest.TestCase):
    def setUp(self):
        self.c = object.__new__(SardegnaCollector)   # parsing helpers only, no network session

    def test_truncated_legal_form(self):
        text = ("si comunica che è stata pubblicata la documentazione relativa al procedimento di Verifica di "
                "assoggettabilità alla V.I.A, per l'intervento proposto dalla Società CCEN ORISTANO 1 S.r., "
                "denominato \"Progetto di un impianto agrofotovoltaico\" " + "x" * 300)
        self.assertEqual(self.c._extract_proponent(text), "CCEN ORISTANO 1 S.r.l.")

    def test_full_legal_form_unchanged(self):
        self.assertEqual(self.c._extract_proponent("Proponente: Società Apollo Decimoputzu S.r.l. Comune di X"),
                         "Apollo Decimoputzu S.r.l.")

    def test_search_row_merged_into_notice(self):
        from app.collectors.base import CollectorResult

        def res(title, proponent, towns, power):
            return CollectorResult("id-" + title[:12], "u", title, {"title": title, "proponent": proponent,
                                   "municipalities": towns, "power": power, "province": None})
        news = [res('Verifica - Impianto "Agrofotovoltaico ORI 1" ricadente nel comune di Siamanna e altri comuni',
                    None, ["Siamanna"], "44,7426 MWp"),
                res("Verifica - Impianto AgriFV, con potenza di 24'893.44 kWp, nel comune di Sassari.",
                    "Inconel Energy Italia S.r.l.", ["Sassari"], "24'893.44 kWp")]
        search = [res('Impianto agrofotovoltaico denominato "ORI 1", della potenza di 44,7426 MWp',
                      "CCEN ORISTANO 1 SRL % CCEN ORISTANO 1 GMBH", ["ORISTANO, SIMAXIS, SANTA GIUSTA"], "44,7426 MWp"),
                  res("Impianto di tipo agrovoltaico con una potenza installata pari a 24'893.44 kWp",
                      "INCONEL ENERGY ITALIA S.R.L.", ["SASSARI"], "24'893.44 kWp"),
                  res('Impianto agrivoltaico denominato "MORES TRE MONTI" 39,41 MWp', "MARIGO ENERGY S.R.L.",
                      ["TORRALBA"], "39,41 MWp")]
        kept = self.c._merge_search_into_news(news, search)
        self.assertEqual([k.payload["proponent"] for k in kept], ["MARIGO ENERGY S.R.L."])
        self.assertEqual(news[0].payload["proponent"], "CCEN ORISTANO 1 SRL")
        self.assertEqual(news[0].payload["municipalities"], ["Siamanna", "Oristano", "Simaxis", "Santa Giusta"])
        self.assertEqual(news[1].payload["proponent"], "Inconel Energy Italia S.r.l.")

    def test_same_power_alone_is_not_enough(self):
        from app.collectors.base import CollectorResult
        a = CollectorResult("a", "u", "x", {"title": 'Impianto "Alfa"', "proponent": "Alfa S.r.l.",
                                           "municipalities": ["Olbia"], "power": "20 MW"})
        b = CollectorResult("b", "u", "y", {"title": 'Impianto "Beta"', "proponent": "Beta S.r.l.",
                                           "municipalities": ["Cagliari"], "power": "20 MW"})
        self.assertEqual(len(self.c._merge_search_into_news([a], [b])), 1)


if __name__ == "__main__":
    unittest.main()
