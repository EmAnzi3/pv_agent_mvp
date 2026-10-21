"""Local runner for the portals that refuse foreign IPs (Sicilia SI-VVI, Sistema Puglia, Basilicata).

Run from the user's PC (Italian network) by analizza_fonti_locali.bat. Same work as one night of the cloud batch,
on the local-only sources: the processing memory is the same branch (doc-intel-state), so cards appear on the
public page as soon as the memory is pushed.

Steps: memory branch checked out in .doc_intel_store (git worktree) -> queue of local-only projects (new first,
then by power) -> one dossier and card per project -> downloaded files deleted -> card index rebuilt ->
memory committed and pushed. Each project is independent: an error is reported and the batch goes on.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

WINDOWS_TESSERACT = (r"C:\Program Files\Tesseract-OCR", r"C:\Program Files (x86)\Tesseract-OCR",
                     os.path.join(os.environ.get("LOCALAPPDATA", ""), "Tesseract-OCR"),        # per-user install
                     os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Tesseract-OCR"))


def default_store() -> Path:
    """Memory checkout OUTSIDE the repository: the repo often sits in OneDrive, which locks and syncs
    thousands of small files (and an old '.doc_intel_store' folder may already be there)."""
    base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~/.cache")
    return Path(base) / "pv_agent" / "memoria_doc_intel"


def _rm(path: Path) -> None:
    """Delete a folder even when Windows marks files read-only; never fail the batch for it."""
    import stat

    def onerror(func, p, _exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except OSError:
            pass
    if path.exists():
        shutil.rmtree(path, onerror=onerror)


def _git(*args: str, cwd: str | Path = ".", check: bool = True) -> subprocess.CompletedProcess:
    res = subprocess.run(["git", *args], cwd=str(cwd), text=True, capture_output=True)
    if check and res.returncode:
        raise RuntimeError(f"git {' '.join(args[:2])}: {(res.stderr or res.stdout).strip()[:400]}")
    return res


def _ensure_memory(store: Path) -> None:
    # fetch into a shared ref (FETCH_HEAD is per worktree and would be stale in the memory checkout)
    _git("fetch", "-q", "origin", "+refs/heads/doc-intel-state:refs/remotes/origin/doc-intel-state")
    if not (store / ".git").exists():
        _rm(store)
        store.parent.mkdir(parents=True, exist_ok=True)
        _git("worktree", "prune", check=False)
        _git("worktree", "add", "--detach", str(store), "origin/doc-intel-state")
    else:
        _git("reset", "-q", "--hard", "origin/doc-intel-state", cwd=store)
    for name in ("blobs", "text"):
        _rm(store / name)


def _ocr_available() -> bool:
    if shutil.which("tesseract"):
        return True
    for d in ((os.environ.get("TESSERACT_DIR") or ""),) + WINDOWS_TESSERACT:
        if not d:
            continue
        if Path(d, "tesseract.exe").exists():
            print(f"[locale] Tesseract trovato in {d}")
            os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
            return True
    return False


def _push(store: Path, message: str) -> bool:
    _git("add", "-A", cwd=store)
    if _git("diff", "--cached", "--quiet", cwd=store, check=False).returncode == 0:
        return True
    _git("-c", "user.name=doc-intel-local", "-c", "user.email=doc-intel-local@users.noreply.github.com",
         "commit", "-q", "-m", message, cwd=store)
    for i in range(5):
        _git("pull", "-q", "--rebase", "-X", "theirs", "origin", "doc-intel-state", cwd=store, check=False)
        if _git("push", "-q", "origin", "HEAD:doc-intel-state", cwd=store, check=False).returncode == 0:
            return True
        time.sleep(10 * (i + 1))
    return False


def main(data: str, max_items: int, budget_min: float, store_dir: str | None = None,
         out_dir: str | None = None) -> int:
    from .card import build_card, write_card
    from .pipeline import run
    from .queue import plan
    from .sync import build_index

    store = Path(store_dir).resolve() if store_dir else default_store()
    print(f"[locale] memoria in {store}")
    _ensure_memory(store)
    ocr = _ocr_available()
    print(f"[locale] OCR (Tesseract): {'disponibile' if ocr else 'NON disponibile: le tavole scansionate saranno saltate'}")
    items = plan(data, store, max_items=max_items, mode="both", local=True)
    print(f"[locale] progetti in questo blocco: {len(items)}")
    done = 0
    out = Path(out_dir) if out_dir else store.parent / "dossier"
    out.mkdir(parents=True, exist_ok=True)
    for n, it in enumerate(items, 1):
        print(f"\n[locale] {n}/{len(items)} {it['url']}")
        try:
            d = run(it["url"], str(store), data, ocr=ocr, ocr_max_pages=600, workers=3, registry="vies",
                    time_budget_s=budget_min * 60)
            (out / f"{d['project']['source']}-{d['project']['project_id']}.json").write_text(
                json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
            if not d["acquisition"].get("references"):
                print("[locale]   nessun documento trovato: scheda non creata")
            else:
                card = build_card(d)
                if card.get("project_key"):
                    write_card(card, store / "schede")
                    done += 1
                    print(f"[locale]   scheda creata: {d['acquisition']['references']} documenti, {d['acquisition']['pages_total']} pagine")
        except Exception as exc:                                  # one portal error never stops the batch
            print(f"[locale]   ERRORE: {str(exc)[:300]}")
        finally:
            for name in ("blobs", "text"):
                _rm(store / name)                                 # downloaded files are never kept
    idx = build_index(store / "schede")
    (store / "schede" / "live_index.json").write_text(json.dumps(idx, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    ok = _push(store, f"doc-intel memory: local batch ({done} cards)")
    print(f"\n[locale] schede create: {done}/{len(items)}; memoria {'salvata su GitHub' if ok else 'NON salvata (riprova piu tardi)'}")
    return 0 if ok else 1
