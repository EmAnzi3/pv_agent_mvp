import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from carry_over_project_keys import carry_over

P = "https://www.sistema.puglia.it/x?id=63472"
M = "https://va.mite.gov.it/it-IT/Oggetti/Info/8387"


class CarryOverKeys(unittest.TestCase):
    def test_base_switch_keeps_previous_key(self):
        old = [{"project_key": "url:puglia", "url": P, "_dedupe_urls": [P]}]
        new = [{"project_key": "url:mase", "url": M, "_dedupe_urls": [M, P]}]
        audit = carry_over(old, new)
        self.assertEqual(new[0]["project_key"], "url:puglia")
        self.assertEqual(audit[0]["rule"], "shared_official_url")

    def test_same_url_new_text_key(self):
        u = "https://www.silvia.servizirl.it/#/x?idProgetto=14934"
        old = [{"project_key": "prj:a", "url": u}]
        new = [{"project_key": "prj:b", "url": u, "_dedupe_urls": [u]}]
        carry_over(old, new)
        self.assertEqual(new[0]["project_key"], "prj:a")

    def test_split_gives_key_once(self):
        u1, u2 = "https://si/976", "https://si/977"
        old = [{"project_key": "prj:mazara", "url": u1, "_dedupe_urls": [u1, u2]}]
        new = [{"project_key": "prj:uno", "url": u2}, {"project_key": "prj:due", "url": u1}]
        carry_over(old, new)
        self.assertEqual([r["project_key"] for r in new], ["prj:uno", "prj:mazara"])

    def test_stable_and_new_untouched(self):
        old = [{"project_key": "k1", "url": "https://a/1"}]
        new = [{"project_key": "k1", "url": "https://a/1"}, {"project_key": "k2", "url": "https://b/2"}]
        self.assertEqual(carry_over(old, new), [])
        self.assertEqual([r["project_key"] for r in new], ["k1", "k2"])


if __name__ == "__main__":
    unittest.main()


class LimitedPortal(unittest.TestCase):
    def test_drawings_filter_keeps_reports_and_programme(self):
        import re
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from doc_intel.pipeline import DRAWING_TITLES, SCHED_RE, PRIORITY_DOCS
        skip = lambda t: bool(re.search(DRAWING_TITLES, t, re.I) and not SCHED_RE.search(t) and not PRIORITY_DOCS.search(t))
        self.assertTrue(skip("08 - SIA - LAYOUT GENERALE DEL PARCO FOTOVOLTAICO"))
        self.assertTrue(skip("P.T.P. STRALCIO TAVOLA 9.B"))
        self.assertFalse(skip("Tav. 12 - Cronoprogramma dei lavori"))
        self.assertFalse(skip("Relazione tecnica con planimetrie"))
        self.assertFalse(skip("Studio di Impatto Ambientale - Sintesi non tecnica"))

    def test_daily_quota(self):
        import tempfile
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from doc_intel import local_batch as lb
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "memoria"
            store.mkdir()
            h = "si-vvi.regione.sicilia.it"
            self.assertEqual(lb._quota_left(store, h), 150)
            self.assertIsNone(lb._quota_left(store, "va.mite.gov.it"))
            lb._quota_add(store, h, 90)
            self.assertEqual(lb._quota_left(store, h), 0)      # one project per day
