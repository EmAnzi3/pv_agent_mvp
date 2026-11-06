import unittest

from doc_intel.wind_changes_html import build_html


class WindChangesHtml(unittest.TestCase):
    def test_sections_and_escaping(self):
        rep = {"date": "2026-11-06", "census": "2026-11-06", "previous_census": "2026-11-02", "generated": "2026-11-06T08:15:00Z",
               "counts": {"nuovi": 1, "rimossi": 0, "modificati": 2, "avanzamenti": 1, "precedenti": 10, "attuali": 11},
               "mw": {"nuovi": 29.4, "rimossi": 0, "nuovi_onshore": 29.4},
               "nuovi": [{"title": "Impianto <Le Toppe>", "url": "https://x/1", "power_mw": 29.4, "region": "Basilicata"}],
               "rimossi": [],
               "modificati": [
                   {"title": "A", "progress": True, "power_mw": 86.04,
                    "changes": [{"field": "readiness", "label": "Priorità cantiere", "old": "", "new": "Media", "progress": True}]},
                   {"title": "B", "progress": False,
                    "changes": [{"field": "card", "label": "Scheda documenti", "old": False, "new": True, "progress": False}]}]}
        h = build_html(rep)
        self.assertIn("Avanzamenti verso il cantiere (1)", h)
        self.assertIn("Nuove schede documentali (1)", h)
        self.assertIn("&lt;Le Toppe&gt;", h)
        self.assertIn("86,0", h)
        self.assertIn("06/11/2026", h)


if __name__ == "__main__":
    unittest.main()
