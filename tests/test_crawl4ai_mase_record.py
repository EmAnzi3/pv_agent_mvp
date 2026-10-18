import unittest
from experiments.crawl4ai_mase_record import choose_existing_mase_record, snippets, safe_url

class TestMASERecordPilot(unittest.TestCase):
    def test_selects_existing_record_and_ignores_other_sources(self):
        source = {"records": [
            {"title":"Other", "source_url":"https://example.com/123"},
            {"title":"Impianto Z", "source":"MASE", "source_url":"https://va.mite.gov.it/it-IT/Oggetti/Info/123"},
        ]}
        record, url = choose_existing_mase_record(source)
        self.assertEqual(record["title"], "Impianto Z")
        self.assertTrue(url.endswith("/123"))

    def test_does_not_select_unrelated_records(self):
        with self.assertRaises(ValueError):
            choose_existing_mase_record({"records":[{"title":"X","source_url":"https://example.com"}]})

    def test_output_is_review_only(self):
        hits = snippets("Potenza nominale di 19,8 MW", "https://va.mite.gov.it/test", "pdf", 4)
        self.assertEqual(hits[0]["pdf_page"], 4)
        self.assertEqual(hits[0]["verification"], "unverified_candidate")

    def test_offsite_link_rejected(self):
        self.assertIsNone(safe_url("https://example.com/private.pdf", "https://va.mite.gov.it/"))

if __name__ == "__main__":
    unittest.main()
