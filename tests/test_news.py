import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from doc_intel.news import parse_feed, relevant, mw_values, municipalities, builders, match

FEED = """<?xml version="1.0"?><rss xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel>
<item><title>ESPE si aggiudica commessa EPC per agrivoltaico da 53,8 MWp a Tuscania</title>
<link>https://example.org/a</link><pubDate>Fri, 05 Jun 2026 08:00:00 +0000</pubDate>
<content:encoded><![CDATA[<p>ESPE S.p.A. realizzerà come EPC contractor l'impianto agrivoltaico di Tuscania (VT) da 53,8 MWp
per Solar Italy 3 S.r.l.; avvio dei lavori a settembre.</p>]]></content:encoded></item>
<item><title>Conto termico, nuove regole</title><link>https://example.org/b</link><description>incentivi caldaie</description></item>
</channel></rss>"""


class News(unittest.TestCase):
    def test_parse_and_filter(self):
        items = parse_feed(FEED)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["date"], "2026-06-05")
        self.assertTrue(relevant(items[0]))
        self.assertFalse(relevant(items[1]))

    def test_extract(self):
        t = parse_feed(FEED)[0]
        text = t["title"] + " " + t["text"]
        self.assertIn(53.8, mw_values(text))
        self.assertIn("Tuscania", municipalities(text))
        self.assertIn("ESPE", builders(text))
        self.assertEqual(mw_values("impianto da 1.250 kWp e uno da 12.500,5 MWp"), [1.25])  # >3000 MW dropped

    def test_match_requires_two_signals(self):
        it = parse_feed(FEED)[0]
        recs = [{"project_key": "k1", "municipalities": "Tuscania", "power_mw": 53.25, "proponent": "X"},
                {"project_key": "k2", "municipalities": "Tuscania", "power_mw": 150, "proponent": "Y"},
                {"project_key": "k3", "municipalities": "Viterbo", "power_mw": 53.8, "proponent": "Z"}]
        got = {r["project_key"]: b for r, b in match(it, recs)}
        self.assertEqual(got, {"k1": "comune + MW"})


if __name__ == "__main__":
    unittest.main()
