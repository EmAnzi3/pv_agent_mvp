import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from exclude_archived_mase import parse_statuses

ONE = ("Scegli la procedura Procedura Codice istanza online Codice procedura Data avvio Stato procedura "
       "Valutazione Impatto Ambientale (PNIEC-PNRR) WEB-VIA FER-VIAVIAF00000536 14463 23/11/2025 Archiviata "
       "Dettagli procedura Codice istanza online WEB-VIA Codice procedura (ID_VIP/ID_MATTM) 14463 "
       "Data presentazione istanza: 23/11/2025 Stato procedura: Archiviata")
TWO = ("Scegli la procedura Procedura Codice istanza online Codice procedura Data avvio Stato procedura "
       "Verifica di Ottemperanza (PNIEC-PNRR) WEB-VOA - VOAVO00000265 15177 28/08/2026 Istruttoria tecnica "
       "CTPNRR-PNIEC Dettagli procedura Documentazione Data presentazione istanza: 28/08/2026 Stato procedura: "
       "Istruttoria tecnica CTPNRR-PNIEC Valutazione Impatto Ambientale (PNIEC-PNRR) 7809 17/12/2021 Conclusa "
       "Dettagli procedura Documentazione Data avvio consultazione pubblica: 15/06/2022")


class ArchivedMase(unittest.TestCase):
    def test_single_archived(self):
        self.assertEqual(parse_statuses(ONE), ["Archiviata"])

    def test_live_project_with_two_procedures(self):
        self.assertEqual(parse_statuses(TWO), ["Istruttoria tecnica CTPNRR-PNIEC", "Conclusa"])

    def test_no_table(self):
        self.assertEqual(parse_statuses("pagina senza procedure"), [])


if __name__ == "__main__":
    unittest.main()
