import json
import os
import tempfile
import unittest
from pathlib import Path

from .core import PublishedData, Radar, card_path


class Fixtures(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.a = {"project_key": "prj:a", "region": "Puglia", "province": "BA, TA",
                  "municipalities": "Bari", "title": "Alpha", "power_mw": 30,
                  "project_type": "FV", "source": "mase", "url": "https://example.org/a"}
        self.b = {**self.a, "project_key": "prj:b", "title": "Beta", "power_mw": None}
        self.terna = {**self.a, "project_key": "prj:t", "power_mw": 900,
                      "is_terna": True}
        self.write("main/docs/data.json", {"summary": {"generated_at": "2026-11-27"}, "records": [self.a, self.b, self.terna]})
        self.write("main/docs/changes_latest.json", {"date": "2026-11-27", "since": "2026-11-23", "nuovi": [self.a]})
        self.write("doc-intel-state/readiness/index.json", {"updated": "2026-11-27", "projects": {"prj:a": {"phase": "possibile", "reasons": ["estimate"]}}})
        self.write("doc-intel-state/ferx/matches.json", {"projects": {"prj:a": [{"basis": "comune + proponente", "kw": 1000, "piva": "excluded"}]}})
        self.write("doc-intel-state/schede/live_index.json", {"prj:a": {"file": "prj_a.json"}})
        self.write("doc-intel-state/schede/prj_a.json", {"project_key": "prj:a", "soggetti": [{"name": "Company", "roles": ["Proponente"], "contacts": [{"value": "not-returned"}]}], "documentazione": {"completa": False}})
        self.radar = Radar(PublishedData(self.root, ttl=0))

    def write(self, path, value):
        p = self.root / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(value), encoding="utf-8")

    def test_search(self):
        x = self.radar.search(region="PUGLIA", province="TA", min_mw=20)
        self.assertEqual([r["project_key"] for r in x["projects"]], ["prj:a"])

    def test_pagination_and_unknown(self):
        self.assertEqual(self.radar.search(limit=1, offset=1)["projects"][0]["project_key"], "prj:b")
        self.assertEqual(self.radar.search(region="Lazio")["total_matches"], 0)
        with self.assertRaises(ValueError):
            self.radar.search(min_mw=float("nan"))

    def test_duplicate_fails_closed(self):
        self.write("main/docs/data.json", {"records": [self.a, self.a]})
        with self.assertRaises(ValueError):
            self.radar.records()

    def test_details_do_not_expose_contacts(self):
        d = self.radar.detail("prj:a")
        self.assertEqual(d["card_status"], "available")
        self.assertNotIn("not-returned", json.dumps(d))
        self.assertFalse(d["document_coverage"]["completa"])
        self.assertFalse(self.radar.detail("prj:t")["found"])

    def test_mismatched_card(self):
        self.write("doc-intel-state/schede/prj_a.json", {"project_key": "prj:b"})
        self.assertEqual(self.radar.detail("prj:a")["card_status"], "identity_mismatch")

    def test_changes(self):
        d = self.radar.changes(region="Puglia")
        self.assertEqual(d["since"], "2026-11-23")
        self.assertEqual(d["changes"][0]["official_source"], self.a["url"])

    def test_readiness_ferx_health(self):
        self.assertEqual(self.radar.readiness("prj:a")["readiness"]["phase"], "possibile")
        self.assertNotIn("piva", self.radar.ferx("prj:a")["matches"][0])
        self.assertEqual(self.radar.health()["readiness"]["canonical_matches"], 1)
        (self.root / "doc-intel-state/readiness/index.json").unlink()
        self.assertFalse(self.radar.health()["readiness"]["available"])

    def test_allowlist(self):
        for path in ("../secret", "https://example.org/other", "schede/../../secret.json"):
            with self.assertRaises(ValueError):
                card_path({"file": path})
        with self.assertRaises(ValueError):
            self.radar.data.read("main", ".env")

    def test_invalid_or_missing_artifact(self):
        (self.root / "main/docs/data.json").write_text("invalid")
        with self.assertRaises(RuntimeError):
            self.radar.records()


@unittest.skipUnless(os.environ.get("PV_MCP_LIVE_TEST") == "1", "explicit live integration only")
class PublishedIntegration(unittest.TestCase):
    def test_real_published_radar(self):
        radar = Radar(PublishedData())
        data, rows = radar.records()
        self.assertGreater(len(rows), 0)
        health = radar.health()
        for name in ("changes", "readiness", "cards", "ferx"):
            self.assertTrue(health[name]["available"], name)
        self.assertGreater(health["cards"]["canonical_matches"], 0)
        index = radar.data.get("cards")
        matches = sorted(set(rows) & set(index))
        details = radar.detail(matches[0])
        self.assertTrue(details["found"])
        self.assertIn(details["card_status"], ("available", "identity_mismatch"))
        self.assertGreater(len(radar.search()["projects"]), 0)
        print("Live canonical records:", len(rows), "matched cards:", len(matches),
              "sample card status:", details["card_status"], "snapshot:", data.get("summary", {}).get("generated_at"))


if __name__ == "__main__":
    unittest.main()
