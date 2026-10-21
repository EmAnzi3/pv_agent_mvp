"""Copy the project cards produced in the cloud (memory branch) into the dashboard folder (docs/schede).

Run by aggiorna_dashboard_senza_docker.bat. One request lists the memory branch (git tree with blob
hashes); only cards whose hash changed are downloaded, so the step takes seconds. The local
docs/schede/index.json (project_key -> file) is rebuilt from the cards on disk and read by the
dashboard to show the 'Scheda' button. Failures never block the dashboard update.
"""
from __future__ import annotations

import json
from pathlib import Path

TREE = "https://api.github.com/repos/EmAnzi3/pv_agent_mvp/git/trees/doc-intel-state?recursive=1"
RAW = "https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/"


def sync_cards(out_dir: str | Path, tree_url: str = TREE, raw_base: str = RAW, get=None) -> dict:
    import requests
    get = get or (lambda u: requests.get(u, timeout=(10, 60), headers={"Accept": "application/vnd.github+json"}))
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    state_path = out / ".hashes.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        state = {}
    r = get(tree_url)
    r.raise_for_status()
    remote = {e["path"].split("/", 1)[1]: e["sha"] for e in r.json().get("tree", [])
              if e.get("type") == "blob" and e["path"].startswith("schede/") and e["path"].endswith(".json")
              and not e["path"].endswith("/index.json")}
    downloaded = errors = 0
    for name, sha in remote.items():
        target = out / name
        if state.get(name) == sha and target.is_file():
            continue
        try:
            rr = get(raw_base + "schede/" + name)
            rr.raise_for_status()
            json.loads(rr.text)                      # never publish a truncated card
            target.write_text(rr.text, encoding="utf-8")
            state[name] = sha
            downloaded += 1
        except Exception:
            errors += 1
    index = {}
    for f in sorted(out.glob("*.json")):
        if f.name == "index.json":
            continue
        try:
            c = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if c.get("project_key"):
            index[c["project_key"]] = {"file": f.name, "updated": c.get("updated")}
    (out / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1, sort_keys=True), encoding="utf-8")
    state_path.write_text(json.dumps(state, indent=0, sort_keys=True), encoding="utf-8")
    return {"downloaded": downloaded, "total": len(index), "errors": errors}
