import json
import tempfile
import unittest
from pathlib import Path

from doc_intel import wind_changes as wc


def rec(key, mw=50, status="Istruttoria tecnica CTPNRR-PNIEC", ferx=None, side=False):
    return {"project_key": key, "title": f"Parco eolico {key}", "region": "Puglia", "power_mw": mw,
            "proponent": "Alfa Wind Srl", "project_type": "Eolico onshore", "procedure_status": status,
            "secondaria": side, "ferx": [{"codice": c} for c in (ferx or [])], "url": "https://va.mite.gov.it/x"}


class CompareTests(unittest.TestCase):
    def test_new_removed_and_progress(self):
        prev = wc.snapshot({"records": [rec("a"), rec("b"), rec("c", mw=30)]}, {}, {}, {})
        cur = wc.snapshot({"records": [rec("a", ferx=["FER1"]), rec("c", mw=36), rec("d", mw=72)]},
                          {"projects": {"a": {"level": "Alta", "phase_label": "Entro 12 mesi"}}},
                          {"a": {"file": "x", "epc": "A"}}, {})
        r = wc.compare(prev, cur)
        self.assertEqual([x["project_key"] for x in r["nuovi"]], ["d"])
        self.assertEqual([x["project_key"] for x in r["rimossi"]], ["b"])
        a = next(c for c in r["modificati"] if c["project_key"] == "a")
        self.assertTrue(a["progress"])
        self.assertEqual({d["field"] for d in a["changes"]}, {"ferx", "readiness", "card", "epc"})
        c = next(c for c in r["modificati"] if c["project_key"] == "c")
        self.assertFalse(c["progress"])
        self.assertEqual(r["modificati"][0]["project_key"], "a")          # progress first
        self.assertEqual(r["counts"]["avanzamenti"], 1)

    def test_cosmetic_changes_ignored(self):
        a, b = rec("a"), rec("a")
        b["title"], b["proponent"] = "PARCO EOLICO A (nuovo titolo)", "alfa  wind srl"
        r = wc.compare(wc.snapshot({"records": [a]}, {}, {}, {}), wc.snapshot({"records": [b]}, {}, {}, {}))
        self.assertEqual(r["modificati"], [])


class RunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.mem = Path(self.tmp.name)
        (self.mem / "wind").mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, records):
        (self.mem / "wind" / "census.json").write_text(json.dumps({"updated": "2026-11-02", "records": records}), encoding="utf-8")

    def test_baseline_then_report_then_partial(self):
        self.write([rec(k) for k in "abcde"])
        self.assertEqual(wc.run(self.mem, today="2026-11-02")["baseline"], "created")
        self.write([rec(k) for k in "abcde"] + [rec("f")])
        self.assertEqual(wc.run(self.mem, today="2026-11-09")["nuovi"], 1)
        idx = json.loads((self.mem / "wind" / "changes" / "index.json").read_text(encoding="utf-8"))
        self.assertEqual(idx["reports"][0]["date"], "2026-11-09")
        self.write([rec("a")])                                           # portal down: partial census
        self.assertTrue(wc.run(self.mem, today="2026-11-16")["partial"])
        self.write([rec(k) for k in "abcdef"] + [rec("g")])
        r = wc.run(self.mem, today="2026-11-16")
        self.assertEqual((r["nuovi"], r["rimossi"]), (1, 0))             # still compared with the good census

    def test_two_runs_same_day_merge(self):
        self.write([rec(k) for k in "abcde"])
        wc.run(self.mem, today="2026-11-01")
        self.write([rec(k) for k in "abcde"] + [rec("f")])
        wc.run(self.mem, today="2026-11-02")
        self.write([rec(k, ferx=["F"] if k == "a" else None) for k in "abcde"] + [rec("f", mw=60), rec("g")])
        r = wc.run(self.mem, today="2026-11-02")
        day = json.loads((self.mem / "wind" / "changes" / "2026-11-02.json").read_text(encoding="utf-8"))
        self.assertEqual(sorted(x["project_key"] for x in day["nuovi"]), ["f", "g"])
        self.assertEqual([c["project_key"] for c in day["modificati"]], ["a"])
        self.assertEqual(next(x for x in day["nuovi"] if x["project_key"] == "f")["power_mw"], 60)
        self.assertEqual(r["nuovi"], 2)


if __name__ == "__main__":
    unittest.main()
