import unittest

from doc_intel.adapters import adapter_for_url
from doc_intel.adapters.regional import CampaniaPaurAdapter

URL = "http://viavas.regione.campania.it/opencms/opencms/VIAVAS/VIA_files_new/Progetti/prg_10072_prot_2025.25767_del_20-01-2025.via"
PAGE = "<html><body>Area VIA &raquo; 10072 PAUR per impianto agrivoltaico Malvizza della potenza di 21,94 MWp in Montecalvo Irpino (AV) Progetto in fase ' Preistruttoria '.</body></html>"
ROOT = "/sites/VIA-VAS/Documenti condivisi/02_PAUR"


def fake_api(path):
    if "02_PAUR')/Folders" in path:
        return {"value": [{"Name": "8847"}, {"Name": "10072"}, {"Name": "100720"}, {"Name": "Forms"}]}
    if "02_PAUR/10072')/Files" in path:
        return {"value": [{"Name": "2025_02_19_prot_ANAS_149954.pdf", "Length": "284192",
                           "ServerRelativeUrl": f"{ROOT}/10072/2025_02_19_prot_ANAS_149954.pdf"}]}
    if "02_PAUR/10072')/Folders" in path:
        return {"value": [{"Name": "2025_01_20_prot_25767_Pandora_istanza"}]}
    if "Pandora_istanza')/Files" in path:
        return {"value": [{"Name": "Relazione tecnica d'impianto.pdf", "Length": "1000", "TimeLastModified": "2025-01-21T10:00:00Z",
                           "ServerRelativeUrl": f"{ROOT}/10072/2025_01_20_prot_25767_Pandora_istanza/Relazione tecnica d'impianto.pdf"}]}
    if "Pandora_istanza')/Folders" in path:
        return {"value": []}
    raise AssertionError(path)


class CampaniaPaurTests(unittest.TestCase):
    def test_registered(self):
        self.assertIsInstance(adapter_for_url(URL), CampaniaPaurAdapter)
        self.assertFalse(CampaniaPaurAdapter.handles("https://viavas.regione.campania.it/opencms/opencms/VIAVAS/Home"))

    def test_identify_and_discover(self):
        a = CampaniaPaurAdapter(get_html=lambda u: PAGE, api=fake_api)
        ident = a.identify(URL)
        self.assertEqual(ident.project_id, "10072")
        self.assertTrue(ident.title.startswith("PAUR per impianto agrivoltaico Malvizza"))
        d = a.discover(ident)
        self.assertEqual(len(d.documents), 2)
        self.assertEqual({x.procedure_id for x in d.documents}, {"10072", "10072/2025_01_20_prot_25767_Pandora_istanza"})
        rel = next(x for x in d.documents if "Relazione" in x.title)
        self.assertIn("d''impianto", rel.url.replace("%27", "'"))        # apostrophes doubled for the REST path
        self.assertEqual(rel.extra["index_date"], "2025-01-21")
        self.assertEqual(rel.extra["auth"], {"prime": CampaniaPaurAdapter.SHARE})
        anas = next(x for x in d.documents if "ANAS" in x.title)
        self.assertEqual(anas.extra["index_date"], "2025-02-19")

    def test_no_folder_yet(self):
        a = CampaniaPaurAdapter(get_html=lambda u: PAGE, api=lambda p: {"value": [{"Name": "8847"}]})
        d = a.discover(a.identify(URL))
        self.assertEqual(d.documents, [])
        self.assertIn("no folder 10072", d.errors[0])


class FetcherPrimeTests(unittest.TestCase):
    def test_prime_visits_share_once(self):
        from doc_intel.fetch import Fetcher

        class Resp:
            status_code = 200
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def iter_content(self, n): return [b"%PDF-1.4 x"]
            def raise_for_status(self): pass

        class Sess:
            def __init__(self): self.calls = []
            def get(self, url, **kw):
                self.calls.append(url)
                return Resp()

        s = Sess()
        f = Fetcher(store=None, session=s)
        f._primed = set()
        for _ in range(2):
            raw, _h = f._download_inner("https://sp/file", {"prime": "https://sp/share"})
        self.assertEqual(s.calls, ["https://sp/share", "https://sp/file", "https://sp/file"])
        self.assertEqual(raw, b"%PDF-1.4 x")


if __name__ == "__main__":
    unittest.main()
