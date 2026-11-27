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


if __name__ == "__main__":
    unittest.main()
