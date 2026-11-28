"""Exercise actual Streamable HTTP tools/list and tools/call over loopback."""
import asyncio
import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from .test_core import Fixtures


class ProtocolTest(unittest.TestCase):
    def test_http(self):
        fixture = Fixtures()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        env = dict(os.environ, PV_MCP_SNAPSHOT_ROOT=str(fixture.root))
        with tempfile.TemporaryFile() as log:
            proc = subprocess.Popen([sys.executable, "-m", "pv_mcp.server"], env=env, stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if proc.poll() is not None:
                        log.seek(0)
                        self.fail(log.read().decode(errors="replace"))
                    try:
                        urllib.request.urlopen("http://127.0.0.1:8769/mcp", timeout=0.5)
                    except urllib.error.HTTPError:
                        break
                    except OSError:
                        time.sleep(0.1)
                    else:
                        break
                async def run():
                    async with streamablehttp_client("http://127.0.0.1:8769/mcp") as (read, write, _):
                        async with ClientSession(read, write) as session:
                            await session.initialize()
                            names = {tool.name for tool in (await session.list_tools()).tools}
                            self.assertEqual(names, {"search_pv_projects", "get_project_details", "get_construction_readiness", "get_recent_changes", "get_ferx_matches", "get_data_health"})
                            result = await session.call_tool("search_pv_projects", {"region": "Puglia", "min_mw": 20})
                            self.assertFalse(result.isError)
                            self.assertIn("prj:a", str(result))
                            self.assertNotIn("prj:t", str(result))
                asyncio.run(run())
            finally:
                proc.terminate()
                proc.wait(timeout=10)
