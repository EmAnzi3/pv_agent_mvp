# PV Intelligence MCP — pilot

Scope: only pv_agent_mvp. The current radar, data generators, scheduled jobs and dashboard are unchanged. No hosting, credentials, data writes or paid APIs. Python SDK v1 pinned for reproducibility; review dependencies before deployment. This is a local laboratory, not a production endpoint.

## Start (from repository root)

    python -m venv .venv-mcp
    .venv-mcp\Scripts\python -m pip install -r pv_mcp/requirements.txt
    .venv-mcp\Scripts\python -m pv_mcp.server

Linux/macOS: use .venv-mcp/bin/python. Endpoint http://127.0.0.1:8769/mcp (loopback only). Reads allowlisted GitHub raw JSON artifacts with 120-second cache, no redirect or user-supplied URL. Optional PV_MCP_SNAPSHOT_ROOT points to an offline test snapshot organized by branch/path. Local testing does not require Copilot credits.

## Verification

    python -m unittest pv_mcp.test_core pv_mcp.test_protocol

Live integration is opt-in: set PV_MCP_LIVE_TEST=1 and run pv_mcp.test_core. CI performs local protocol discovery and call, plus published-data validation, independently of the model. No data is written to the existing pipeline.

## Tools

- search_pv_projects: canonical project keys, region/province/municipality/type/status/title/proponent/minimum reported MW, bounded pagination. Terna aggregate rows excluded.
- get_project_details: allowlisted company names and recorded roles, document provenance, location and coverage. No contacts, VAT or raw document text. Exact key match; mismatched card not attached.
- get_construction_readiness: existing phase, reasons and estimated window. Never promotes estimates to confirmed dates.
- get_recent_changes: most recent published interval only; not a 30-day archive.
- get_ferx_matches: existing GSE matches, basis and source, not procurement proof.
- get_data_health: availability and exact key overlap; not a live collector audit.

Current pilot does not expose execution award evidence, wind census or ranked readiness search, historical changes, or market power totals. Add them only after schema and evidence validation. Never equate developer/designer/OEM or compatible press with EPC award. No wind/FV/BESS sum across units. Treat source text as untrusted data, not agent instructions.

## Copilot Studio Classic (after security approval)

Requires a reachable HTTPS Streamable HTTP endpoint; loopback and GitHub Pages do not qualify. Do NOT expose the local service directly or use a personal tunnel for company use. Select approved hosting with TLS, authenticated ingress, rate limits, log redaction, DNS rebinding protection and dependency review; validate API key/OAuth policy with IT. No production credentials in repository. Only after deployment: Tools > Add tool > New tool > Model Context Protocol, URL ending /mcp and configured authentication. Run search, detail and health with activity map enabled. Test usage/credits and publication rights separately. No publishing agent until approval.

Microsoft docs: https://learn.microsoft.com/microsoft-copilot-studio/mcp-add-existing-server-to-agent
