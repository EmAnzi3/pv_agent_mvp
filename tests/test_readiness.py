import sys, unittest
from datetime import date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from doc_intel.readiness import classify

T = date(2026, 10, 27)
AUTH = {"atto": "Decreto n. 1", "data": "01/01/2025"}


def card(rows=(), stato=None, auth=AUTH, ex=None):
    return {"tempistica": {"righe": list(rows), "autorizzazione": auth}, "stato": stato, "esecuzione": ex}


class Readiness(unittest.TestCase):
    def test_built_is_not_ranked(self):
        c = card([{"voce": "Fine lavori", "livello": "documentata", "valore": "11/02/2025", "dettaglio": "natura non determinabile"}])
        r = classify(c, today=T)
        self.assertEqual((r["phase"], r["level"]), ("realizzato", None))

    def test_planned_end_is_not_built(self):
        c = card([{"voce": "Fine lavori", "livello": "documentata", "valore": "11/02/2025", "dettaglio": "previsione di progetto"}])
        self.assertNotEqual(classify(c, today=T)["phase"], "realizzato")

    def test_start_within_12_months(self):
        c = card([{"voce": "Cronoprogramma con date", "livello": "documentata", "valore": "01/03/2027 → 01/12/2027"}])
        r = classify(c, today=T)
        self.assertEqual(r["phase"], "entro_12_mesi")
        self.assertEqual(r["score"], 5 + 1)

    def test_ferx_and_site_act_raise_the_level(self):
        ex = {"livello": "A", "imprese": [{"impresa": "Comal S.p.A.", "source": {"document": "Comunicazione avvio lavori"}}]}
        r = classify(card(ex=ex), ferx=[{"procedura": "P", "posizione": 1}], today=T)
        self.assertEqual(r["phase"], "in_corso")
        self.assertEqual(r["level"], "Alta")
        self.assertTrue(any("FER X" in x for x in r["reasons"]))

    def test_expired_act(self):
        c = card([{"voce": "Inizio lavori", "livello": "stimata", "valore": "non prima di gennaio 2020"},
                  {"voce": "Validità dell'atto", "livello": "prescritta", "valore": "fino al 01/01/2025"}])
        r = classify(c, today=T)
        self.assertEqual(r["phase"], "autorizzato")
        self.assertIn("superato", r["window"])



class VerifiedWind(unittest.TestCase):
    def test_verified_stage_overrides(self):
        from doc_intel.readiness import _verified_stage
        r = _verified_stage({}, {"stage": "E7", "fase": "Construction"})
        self.assertEqual((r["phase"], r["level"]), ("in_corso", "Alta"))
        self.assertEqual(_verified_stage({}, {"stage": "E8"})["phase"], "realizzato")
        self.assertEqual(_verified_stage({"phase": "in_iter"}, {"stage": "E2"}), {"phase": "in_iter"})

if __name__ == "__main__":
    unittest.main()


class ReadinessRecord(unittest.TestCase):
    def test_ottemperanza_and_archived(self):
        r = classify(card(), today=T, record={"project_type": "Verifica di Ottemperanza (PNIEC-PNRR)", "title": "x"})
        self.assertEqual(r["phase"], "possibile")
        self.assertTrue(any("ottemperanza" in x for x in r["reasons"]))
        a = classify(card(), ferx=[{"procedura": "P", "posizione": 1}], today=T,
                     record={"title": "Determinazione n. G08791 del 07/07/2022 - Archiviato Impianto"})
        self.assertNotEqual(a["level"], "Alta")
