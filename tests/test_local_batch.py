import json
import tempfile
import unittest
from pathlib import Path

from doc_intel import local_batch as lb

SI = "https://si-vvi.regione.sicilia.it/viavas/index.php/it/?id={}"
PU = "https://www.sistema.puglia.it/portal/page/portal/SistemaPuglia/energia?id={}"


class SelectTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Path(self.tmp.name) / "mem"
        self.store.mkdir()
        q = [{"url": SI.format(i), "project_key": f"s{i}"} for i in range(50)]
        q += [{"url": PU.format(i), "project_key": f"p{i}"} for i in range(8)]
        self.queue = q

    def tearDown(self):
        self.tmp.cleanup()

    def test_limited_portal_does_not_crowd_out_others(self):
        items, reserve = lb._select(self.queue, self.store, 30)
        hosts = [i["project_key"][0] for i in items]
        self.assertEqual(hosts.count("s"), 1)
        self.assertEqual(hosts.count("p"), 8)
        self.assertEqual(reserve, [])

    def test_quota_used_today_excludes_portal(self):
        lb._quota_add(self.store, "si-vvi.regione.sicilia.it", 10)
        items, _ = lb._select(self.queue, self.store, 30)
        self.assertTrue(all(i["project_key"].startswith("p") for i in items))
        self.assertEqual(len(items), 8)


if __name__ == "__main__":
    unittest.main()
