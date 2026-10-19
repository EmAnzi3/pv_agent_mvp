"""Persistent, content-addressed cache and per-project manifests.

Layout (outside Git; the CI persists it with actions/cache)::

    <root>/blobs/ab/abcdef...          original bytes, named by SHA-256
    <root>/text/abcdef....json         per-page text + method, keyed by SHA-256 + parser version
    <root>/manifests/<source>/<project>.json

Re-runs never re-download a URL already in the manifest unless ``refresh`` is
requested, and never re-parse bytes whose SHA-256 is already in the text cache.
If a refreshed URL returns different bytes, the previous SHA is kept in the
manifest ``history`` so revisions are detectable.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PARSER_CACHE_VERSION = "text-v1"


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class Store:
    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root)
        self.stats = {"blob_hits": 0, "blob_writes": 0, "text_hits": 0, "text_writes": 0}

    # ---- blobs -----------------------------------------------------------
    def blob_path(self, sha: str) -> Path:
        return self.root / "blobs" / sha[:2] / sha

    def has_blob(self, sha: str) -> bool:
        return self.blob_path(sha).is_file()

    def put_blob(self, raw: bytes) -> str:
        sha = sha256_bytes(raw)
        p = self.blob_path(sha)
        if p.is_file():
            self.stats["blob_hits"] += 1
        else:
            _atomic_write(p, raw)
            self.stats["blob_writes"] += 1
        return sha

    def get_blob(self, sha: str) -> bytes:
        raw = self.blob_path(sha).read_bytes()
        if sha256_bytes(raw) != sha:
            raise IOError(f"cache corruption for {sha}")
        return raw

    # ---- extracted text --------------------------------------------------
    def text_path(self, sha: str, member: str | None = None) -> Path:
        suffix = "" if not member else "-" + sha256_bytes(member.encode("utf-8"))[:12]
        return self.root / "text" / PARSER_CACHE_VERSION / f"{sha}{suffix}.json"

    def get_text(self, sha: str, member: str | None = None) -> dict[str, Any] | None:
        p = self.text_path(sha, member)
        if p.is_file():
            self.stats["text_hits"] += 1
            return json.loads(p.read_text(encoding="utf-8"))
        return None

    def put_text(self, sha: str, payload: dict[str, Any], member: str | None = None) -> None:
        _atomic_write(self.text_path(sha, member),
                      json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        self.stats["text_writes"] += 1

    # ---- manifests -------------------------------------------------------
    def manifest_path(self, source: str, project_id: str) -> Path:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in project_id)
        return self.root / "manifests" / source / f"{safe}.json"

    def load_manifest(self, source: str, project_id: str) -> dict[str, Any]:
        p = self.manifest_path(source, project_id)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
        return {"source": source, "project_id": project_id, "documents": {}, "runs": []}

    def save_manifest(self, manifest: dict[str, Any]) -> None:
        p = self.manifest_path(manifest["source"], manifest["project_id"])
        _atomic_write(p, json.dumps(manifest, ensure_ascii=False, indent=1).encode("utf-8"))


def record_download(manifest: dict[str, Any], url: str, sha: str, size: int, kind: str,
                    headers: dict[str, str] | None = None) -> str:
    """Register a download; returns 'new' | 'unchanged' | 'revised'."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    docs = manifest.setdefault("documents", {})
    entry = docs.get(url)
    if entry and entry.get("sha256") == sha:
        entry["last_checked"] = now
        return "unchanged"
    state = "revised" if entry and entry.get("sha256") else "new"
    history = (entry or {}).get("history", [])
    if entry and entry.get("sha256"):
        history.append({"sha256": entry["sha256"], "fetched_at": entry.get("fetched_at")})
    docs[url] = {**(entry or {}), "sha256": sha, "bytes": size, "kind": kind, "fetched_at": now,
                 "last_checked": now, "history": history,
                 "etag": (headers or {}).get("ETag"), "last_modified": (headers or {}).get("Last-Modified")}
    docs[url].pop("error", None)
    return state


def record_failure(manifest: dict[str, Any], url: str, error: str) -> None:
    docs = manifest.setdefault("documents", {})
    entry = docs.setdefault(url, {})
    entry["error"] = error[:300]
    entry["attempts"] = entry.get("attempts", 0) + 1
