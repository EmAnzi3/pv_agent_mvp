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
import re
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
        # cards of a previous run whose push failed (network, GitHub busy) are pushed now, never discarded
        ahead = _git("rev-list", "--count", "origin/doc-intel-state..HEAD", cwd=store, check=False).stdout.strip()
        if ahead.isdigit() and int(ahead) > 0:
            print(f"[locale] {ahead} salvataggi del giro precedente non ancora su GitHub: li invio ora")
            if not _push(store, "doc-intel memory: local batch (recovered)"):
                raise RuntimeError("memoria del giro precedente non inviata: riprova piu' tardi (nulla e' stato perso)")
            _git("fetch", "-q", "origin", "+refs/heads/doc-intel-state:refs/remotes/origin/doc-intel-state")
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
        _git("rebase", "--abort", cwd=store, check=False)          # leftovers of an interrupted run
        _git("merge", "--abort", cwd=store, check=False)
        pl = _git("pull", "-q", "--rebase", "-X", "theirs", "origin", "doc-intel-state", cwd=store, check=False)
        if pl.returncode:
            # a rebase that stops (e.g. a file changed on both sides) is undone and replaced by a merge that
            # keeps this run's version of the conflicting files: the cloud rebuilds shared indexes anyway
            _git("rebase", "--abort", cwd=store, check=False)
            pl = _git("pull", "-q", "--no-rebase", "--no-edit", "-X", "ours", "origin", "doc-intel-state", cwd=store, check=False)
            if pl.returncode:
                _git("merge", "--abort", cwd=store, check=False)
                print(f"[locale]   git pull non riuscito: {(pl.stderr or pl.stdout).strip()[:300]}")
        ps = _git("push", "-q", "origin", "HEAD:doc-intel-state", cwd=store, check=False)
        if ps.returncode == 0:
            return True
        print(f"[locale]   invio a GitHub non riuscito (tentativo {i + 1}/5): {(ps.stderr or ps.stdout).strip()[:300]}")
        time.sleep(10 * (i + 1))
    return False


def _forget_attempt(store: Path, key: str, prefix: str = "") -> None:
    att = store / "queue" / f"{prefix}attempts.json"
    try:
        data_att = json.loads(att.read_text(encoding="utf-8"))
        data_att.pop(key, None)
        att.write_text(json.dumps(data_att, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    except (OSError, ValueError):
        pass


# Portals that asked us (by blocking) to keep the load low: at most one project and DAILY_DOCS
# documents per day, drawings not downloaded, one document at a time with a pause (fetch.POLITE_HOSTS).
LIMITED_PORTALS = {"si-vvi.regione.sicilia.it": {"projects": 1, "docs": 150}}


def _quota_file(store: Path) -> Path:
    return store.parent / "portal_quota.json"


def _quota_used(store: Path, host: str) -> dict:
    import datetime as _dt
    try:
        q = json.loads(_quota_file(store).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        q = {}
    return q.get(_dt.date.today().isoformat(), {}).get(host, {"projects": 0, "docs": 0})


def _quota_add(store: Path, host: str, docs: int) -> None:
    import datetime as _dt
    f, today = _quota_file(store), _dt.date.today().isoformat()
    try:
        q = json.loads(f.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        q = {}
    q = {today: q.get(today, {})}                       # only today's counters are kept
    u = q[today].setdefault(host, {"projects": 0, "docs": 0})
    u["projects"] += 1
    u["docs"] += docs
    try:
        f.write_text(json.dumps(q, indent=1), encoding="utf-8")
    except OSError:
        pass


def _quota_left(store: Path, host: str) -> int | None:
    """None: no limit for this portal; 0: today's quota is used up; n: documents still allowed today."""
    lim = LIMITED_PORTALS.get(host)
    if not lim:
        return None
    u = _quota_used(store, host)
    if u["projects"] >= lim["projects"]:
        return 0
    return max(0, lim["docs"] - u["docs"])


def _select(queue: list[dict], store: Path, max_items: int) -> tuple[list[dict], list[dict]]:
    """Block for this run: a portal with a daily cap gets only the projects it still allows today, so
    the remaining room goes to the other portals instead of being filled by projects that would be
    postponed (before: 270 Sicilian projects at the top of the queue crowded out Puglia and Basilicata)."""
    from urllib.parse import urlparse
    room: dict[str, int] = {}
    picked = []
    for it in queue:
        host = urlparse(it["url"]).netloc
        lim = LIMITED_PORTALS.get(host)
        if lim:
            if host not in room:
                room[host] = max(0, lim["projects"] - _quota_used(store, host)["projects"]) if _quota_left(store, host) else 0
            if room[host] <= 0:
                continue
            room[host] -= 1
        picked.append(it)
    return picked[:max_items], picked[max_items:max_items * 4]


def main(data: str, max_items: int, budget_min: float, store_dir: str | None = None,
         out_dir: str | None = None, recompute: bool = False, wind: bool = False) -> int:
    from .card import build_card, write_card
    from .pipeline import DRAWING_TITLES, run
    from .queue import plan
    from .sync import build_index

    store = Path(store_dir).resolve() if store_dir else default_store()
    print(f"[locale] memoria in {store}")
    _ensure_memory(store)
    # wind: the census on the memory branch (Sicily, Sistema Puglia, Basilicata wind procedures), with its
    # own queue state (queue/wind_*.json) like the nightly wind cards; the SI-VVI daily cap is shared
    prefix = "wind_" if wind else ""
    if wind:
        data = str(store / "wind" / "census.json")
        print(f"[locale] eolico: censimento {data}")
    ocr = _ocr_available()
    print(f"[locale] OCR (Tesseract): {'disponibile' if ocr else 'NON disponibile: le tavole scansionate saranno saltate'}")
    # 'ricalcola': re-read the documents of projects that already have a card (new extraction rules)
    # queue a reserve: when a portal does not answer, its projects are skipped and the block is filled
    # with projects of the other portals, so a run is never wasted
    from .queue import record_attempts
    # the whole local queue, then the block (see _select)
    allq = plan(data, store, max_items=100000, mode="refresh" if recompute else "both", local=True,
                retry_days=0 if recompute else 14, record=False, state_prefix=prefix)
    items, reserve = _select(allq, store, max_items)
    record_attempts(store, [it["project_key"] for it in items], state_prefix=prefix)
    print(f"[locale] progetti in questo blocco: {len(items)}")
    done = 0
    out = Path(out_dir) if out_dir else store.parent / "dossier"
    out.mkdir(parents=True, exist_ok=True)
    from urllib.parse import urlparse
    down: dict[str, int] = {}                       # consecutive connection failures per portal
    queue_, n, worked = list(items), 0, 0
    while queue_ and worked < max_items:
        it = queue_.pop(0)
        n += 1
        print(f"\n[locale] {n} {it['url']}")
        host = urlparse(it["url"]).netloc
        if down.get(host, 0) >= 2:
            print(f"[locale]   saltato: {host} non risponde (riprovato al prossimo lancio)")
            _forget_attempt(store, it["project_key"], prefix)
            # replace it with a reserve project from a portal that answers
            nxt = next((r for r in reserve if down.get(urlparse(r["url"]).netloc, 0) < 2), None)
            if nxt:
                reserve.remove(nxt)
                queue_.append(nxt)
            continue
        left = _quota_left(store, host)
        if left == 0:
            print(f"[locale]   rimandato: limite giornaliero per {host} raggiunto (riprende domani)")
            _forget_attempt(store, it["project_key"], prefix)
            nxt = next((r for r in reserve if _quota_left(store, urlparse(r["url"]).netloc) != 0
                        and down.get(urlparse(r["url"]).netloc, 0) < 2), None)
            if nxt:
                reserve.remove(nxt)
                queue_.append(nxt)
            continue
        worked += 1
        limited = left is not None
        try:
            d = run(it["url"], str(store), data, ocr=ocr, ocr_max_pages=600,
                    workers=1 if limited else 3, registry="vies",
                    max_docs=left if limited else None,
                    skip_titles=DRAWING_TITLES if limited else None,
                    time_budget_s=budget_min * 60, reanalyse=recompute)
            if limited:
                # only real downloads count (documents already read are reused without any request)
                st = d["acquisition"].get("states") or {}
                _quota_add(store, host, sum(v for k, v in st.items() if k not in ("reused", "deferred")))
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
            if limited:
                _quota_add(store, host, left)                     # unknown downloads: count the whole quota
            if re.search(r"(?i)NameResolution|ConnectionError|Max retries|timed out|ConnectTimeout|Temporary failure", str(exc)):
                # network or portal down: not the project's fault, retry it at the next run
                down[host] = down.get(host, 0) + 1
                _forget_attempt(store, it["project_key"], prefix)
        else:
            down[host] = 0
        finally:
            for name in ("blobs", "text"):
                _rm(store / name)                                 # downloaded files are never kept
    idx = build_index(store / "schede")
    (store / "schede" / "live_index.json").write_text(json.dumps(idx, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    ok = _push(store, f"doc-intel memory: local batch{' wind' if wind else ''} ({done} cards)")
    print(f"\n[locale] schede create: {done}/{worked}; memoria {'salvata su GitHub' if ok else 'NON salvata (riprova piu tardi)'}")
    return 0 if ok else 1
