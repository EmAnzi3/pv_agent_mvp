import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import compare_json_report as cjr  # noqa: E402


class ZoneJsonTest(unittest.TestCase):
    """docs/changes_latest.json: the news shown by the dashboard «La mia zona»."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_write_json_skips_terna_and_labels_progress(self):
        new = {"project_key": "prj:1", "region": "Puglia", "province": "FG", "municipalities": "Lucera",
               "title": "Impianto Lucera", "proponent": "Alfa S.r.l.", "power_mw": "44,5"}
        terna = {"source": "terna_econnextion", "region": "Puglia", "title": "Terna FG"}
        result = {"added": [new, terna], "removed": [], "changed": [],
                  "progress": [{"record": new, "diffs": [
                      {"label": "Priorità cantiere", "old": "—", "new": "Media"},
                      {"label": "Fase", "old": "In iter", "new": "Autorizzato"}]}]}
        out = self.tmp / "changes_latest.json"
        cjr.write_json(out, result, since="2026-11-10")
        data = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(data["since"], "2026-11-10")
        self.assertEqual([x["project_key"] for x in data["nuovi"]], ["prj:1"])
        self.assertEqual(data["nuovi"][0]["power_mw"], 44.5)
        self.assertEqual(data["avanzamenti"][0]["what"], "Autorizzato")

    def test_progress_without_phase_uses_label_and_value(self):
        rec = {"project_key": "prj:2", "region": "Lazio"}
        result = {"added": [], "removed": [], "changed": [],
                  "progress": [{"record": rec, "diffs": [{"label": "FER X", "old": "no", "new": "incentivo assegnato"}]}]}
        out = self.tmp / "c.json"
        cjr.write_json(out, result)
        self.assertEqual(json.loads(out.read_text(encoding="utf-8"))["avanzamenti"][0]["what"], "FER X: incentivo assegnato")

    def test_realised_goes_to_its_own_list(self):
        import pv_signals
        base = {"level": "", "phase": "", "phase_label": "", "ferx": False, "epc": "", "news": 0}
        prev = {"k": dict(base, level="Media", phase="possibile", phase_label="Avvio possibile entro 12 mesi")}
        cur = {"k": dict(base, phase="realizzato", phase_label="Realizzato o in esercizio", epc="A")}
        ch = pv_signals.compare(prev, cur)["k"]
        self.assertEqual(ch[0]["label"], pv_signals.REALIZED)
        self.assertNotIn("Priorità cantiere", [d["label"] for d in ch])
        rec = {"project_key": "k", "region": "Lazio"}
        out = self.tmp / "r.json"
        cjr.write_json(out, {"added": [], "progress": [], "realized": [{"record": rec, "diffs": ch, "kind": "realizzato"}]})
        data = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(data["counts"]["realizzati"], 1)
        self.assertEqual(data["realizzati"][0]["what"], "Realizzato o in esercizio")
        self.assertEqual(data["avanzamenti"], [])

    def test_generated_at_reads_old_snapshot(self):
        old = self.tmp / "old.json"
        old.write_text(json.dumps({"summary": {"generated_at": "2026-11-13 08:47:54"}, "records": []}), encoding="utf-8")
        self.assertEqual(cjr._generated_at(old), "2026-11-13")
        self.assertEqual(cjr._generated_at(self.tmp / "missing.json"), "")


if __name__ == "__main__":
    unittest.main()
