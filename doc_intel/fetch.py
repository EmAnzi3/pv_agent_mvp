"""Layer C (part 1): download originals, hash them, detect format by signature, open archives."""
from __future__ import annotations

import io
import threading
import time
import zipfile
from dataclasses import dataclass, field
from typing import Callable

from .store import Store, record_download, record_failure

DEFAULT_UA = "PV-Agent-DocIntel/0.1 (+review-only research; contact via repository owner)"

SIGNATURES = [
    (b"%PDF", "pdf"),
    (b"PK\x03\x04", "zip"),            # also docx/xlsx/kmz -> refined below
    (b"\xd0\xcf\x11\xe0", "ole"),       # legacy doc/xls
    (b"\x30\x80", "p7m"),               # CAdES (signed) DER, indefinite length
    (b"\x30\x82", "p7m"),
    (b"AC10", "dwg"),
    (b"\x89PNG", "png"),
    (b"\xff\xd8\xff", "jpg"),
    (b"II*\x00", "tif"),
    (b"MM\x00*", "tif"),
]


def sniff(raw: bytes) -> str:
    head = raw[:8]
    for sig, kind in SIGNATURES:
        if head.startswith(sig):
            if kind == "zip":
                return _refine_zip(raw)
            return kind
    # Some servers prepend whitespace/BOM before %PDF
    if b"%PDF" in raw[:1024]:
        return "pdf"
    lowered = raw[:512].lower()
    if b"<html" in lowered or b"<!doctype html" in lowered:
        return "html"
    return "unknown"


def _refine_zip(raw: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            names = z.namelist()
    except zipfile.BadZipFile:
        return "unknown"
    if "word/document.xml" in names:
        return "docx"
    if "xl/workbook.xml" in names:
        return "xlsx"
    if any(n.lower().endswith(".kml") for n in names) and len(names) <= 3:
        return "kmz"
    return "zip"


@dataclass
class Member:
    name: str
    kind: str
    raw: bytes
    parent_sha256: str
    depth: int


@dataclass
class FetchResult:
    url: str
    sha256: str | None
    kind: str | None
    size: int = 0
    state: str = "failed"        # new | unchanged | revised | cached | failed
    error: str | None = None
    members: list[Member] = field(default_factory=list)
    skipped_members: list[dict] = field(default_factory=list)


def expand_archive(raw: bytes, parent_sha: str, cap: int, depth: int = 0,
                   max_depth: int = 2) -> tuple[list[Member], list[dict]]:
    """Inventory every ZIP entry; return processable members and an explicit skip list."""
    members: list[Member] = []
    skipped: list[dict] = []
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            if info.file_size > cap:
                skipped.append({"name": info.filename, "reason": "exceeds size cap", "size": info.file_size})
                continue
            try:
                data = z.read(info)
            except Exception as exc:  # encrypted, corrupt
                skipped.append({"name": info.filename, "reason": f"unreadable: {exc}"[:200]})
                continue
            kind = sniff(data)
            if kind == "zip" and depth < max_depth:
                sub, sub_skip = expand_archive(data, parent_sha, cap, depth + 1, max_depth)
                for m in sub:
                    m.name = f"{info.filename}/{m.name}"
                members.extend(sub)
                skipped.extend({**s, "name": f"{info.filename}/{s['name']}"} for s in sub_skip)
            elif kind == "pdf":
                members.append(Member(info.filename, kind, data, parent_sha, depth + 1))
            else:
                skipped.append({"name": info.filename, "reason": f"format {kind} not parsed", "size": len(data)})
    return members, skipped


class Fetcher:
    """HTTP downloader with byte cap, bounded retries and persistent cache."""

    def __init__(self, store: Store, session=None, max_bytes: int = 200 * 1024 * 1024,
                 retries: int = 2, backoff: float = 3.0, timeout: int = 120,
                 sleeper: Callable[[float], None] = time.sleep):
        if session is None:
            import requests
            session = requests.Session()
            session.headers["User-Agent"] = DEFAULT_UA
        self.session = session
        self.store = store
        self.max_bytes = max_bytes
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self.sleep = sleeper
        self._lock = threading.Lock()

    def _download(self, url: str, auth=None) -> tuple[bytes, dict[str, str]]:
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                kw = {"auth": tuple(auth)} if auth else {}
                with self.session.get(url, timeout=self.timeout, stream=True, **kw) as resp:
                    status = getattr(resp, "status_code", 200)
                    if status in (429, 500, 502, 503, 504):
                        raise IOError(f"HTTP {status}")
                    resp.raise_for_status()
                    blocks, size = [], 0
                    for chunk in resp.iter_content(1024 * 1024):
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > self.max_bytes:
                            raise ValueError(f"file exceeds byte cap {self.max_bytes}")
                        blocks.append(chunk)
                    return b"".join(blocks), dict(getattr(resp, "headers", {}) or {})
            except ValueError:
                raise  # size cap: retrying will not help
            except Exception as exc:  # transient network / server errors
                last = exc
                if attempt < self.retries:
                    self.sleep(self.backoff * (attempt + 1))
        raise IOError(f"download failed after {self.retries + 1} attempts: {last}")

    def fetch(self, url: str, manifest: dict, refresh: bool = False, auth=None) -> FetchResult:
        entry = manifest.get("documents", {}).get(url, {})
        sha = entry.get("sha256")
        if sha and not refresh and self.store.has_blob(sha):
            raw = self.store.get_blob(sha)
            result = FetchResult(url, sha, entry.get("kind"), len(raw), "cached")
        else:
            try:
                raw, headers = self._download(url, auth)
            except Exception as exc:
                with self._lock:
                    record_failure(manifest, url, str(exc))
                return FetchResult(url, None, None, 0, "failed", str(exc)[:300])
            sha = self.store.put_blob(raw)
            kind = sniff(raw)
            with self._lock:
                state = record_download(manifest, url, sha, len(raw), kind, headers)
            result = FetchResult(url, sha, kind, len(raw), state)
        kind = result.kind or sniff(raw)
        result.kind = kind
        if kind == "pdf":
            result.members = [Member(url.rsplit("/", 1)[-1] + ".pdf", "pdf", raw, sha, 0)]
        elif kind == "zip":
            result.members, result.skipped_members = expand_archive(raw, sha, self.max_bytes)
        else:
            result.skipped_members = [{"name": url, "reason": f"format {kind} not parsed", "size": len(raw)}]
        return result
