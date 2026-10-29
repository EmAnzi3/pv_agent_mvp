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
    if raw[:11] in (b"-----BEGIN ",) and (b"PKCS7" in raw[:40] or b"CMS" in raw[:40]) or (raw[:3] == b"MII" and b" " not in raw[:64]):
        return "p7m"                                        # signed file saved base64 (PEM)
    # Some servers prepend whitespace/BOM before %PDF
    if b"%PDF" in raw[:1024]:
        return "pdf"
    lowered = raw[:512].lower()
    if b"<html" in lowered or b"<!doctype html" in lowered:
        return "html"
    return "unknown"


def _tlv(buf: bytes, pos: int) -> tuple[int, bool, int, int, int]:
    """One DER/BER element at pos: (tag, constructed, content_start, content_end, next_pos).
    Indefinite lengths (BER, used by streaming signers) end at the 00 00 marker of their own level."""
    tag = buf[pos]
    constructed = bool(tag & 0x20)
    pos += 1
    if tag & 0x1F == 0x1F:                      # high tag number
        while buf[pos] & 0x80:
            pos += 1
        pos += 1
    first = buf[pos]
    pos += 1
    if first == 0x80:
        start, p = pos, pos
        while buf[p:p + 2] != b"\x00\x00":
            p = _tlv(buf, p)[4]
        return tag, constructed, start, p, p + 2
    if first & 0x80:
        n = first & 0x7F
        length = int.from_bytes(buf[pos:pos + n], "big")
        pos += n
    else:
        length = first
    return tag, constructed, pos, pos + length, pos + length


def _children(buf: bytes, start: int, end: int) -> list[tuple[int, bool, int, int, int]]:
    out, p = [], start
    while p < end and buf[p:p + 2] != b"\x00\x00":
        el = _tlv(buf, p)
        out.append(el)
        p = el[4]
    return out


def _octets(buf: bytes, el) -> bytes:
    tag, constructed, s, e, _ = el
    if not constructed:
        return buf[s:e]
    return b"".join(_octets(buf, c) for c in _children(buf, s, e))


def p7m_content(raw: bytes, depth: int = 0) -> bytes | None:
    """Document signed with CAdES (.p7m): the signed file inside (PDF, ZIP...), nested signatures unwrapped.
    ContentInfo -> [0] SignedData -> encapContentInfo -> [0] eContent (OCTET STRING, possibly chunked)."""
    import base64
    try:
        if raw[:3] in (b"MII", b"MIA") or raw[:1] == b"-":
            body = b"".join(l for l in raw.splitlines() if l and not l.startswith(b"-----"))
            raw = base64.b64decode(body)
        top = _tlv(raw, 0)
        ci = _children(raw, top[2], top[3])                  # contentType OID, [0] content
        sd_wrap = _children(raw, ci[1][2], ci[1][3])[0]       # SignedData SEQUENCE
        sd = _children(raw, sd_wrap[2], sd_wrap[3])
        encap = _children(raw, sd[2][2], sd[2][3])            # version, digestAlgorithms, encapContentInfo
        if len(encap) < 2:
            return None                                       # detached signature: no content inside
        econtent = _children(raw, encap[1][2], encap[1][3])[0]
        data = _octets(raw, econtent)
    except (IndexError, ValueError):
        return None
    if depth < 3 and data[:2] in (b"\x30\x80", b"\x30\x82"):
        return p7m_content(data, depth + 1) or data
    return data


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
            if kind == "p7m":
                inner = p7m_content(data)
                if inner:
                    data, kind = inner, sniff(inner)
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

    # small regional portals: one request at a time with a pause, so a batch never looks like an attack
    POLITE_HOSTS = {"si-vvi.regione.sicilia.it": 5.0, "www.sistema.puglia.it": 1.0, "valutazioneambientale.regione.basilicata.it": 1.0}
    _host_locks: dict = {}

    def _polite(self, url: str):
        import threading
        from urllib.parse import urlparse
        host = urlparse(url).netloc
        pause = self.POLITE_HOSTS.get(host)
        if not pause:
            return None
        lock = self._host_locks.setdefault(host, threading.Lock())
        lock.acquire()
        time.sleep(pause)
        return lock

    _primed: set = set()

    def _auth_kw(self, auth) -> dict:
        """auth is [user, password] (basic auth) or {"prime": url}: a public share link (SharePoint guest
        link) whose visit gives the session the cookie needed to download the files of the share."""
        if not auth:
            return {}
        if isinstance(auth, dict):
            prime = auth.get("prime")
            if prime and (id(self.session), prime) not in self._primed:
                try:
                    self.session.get(prime, timeout=60, allow_redirects=True)
                    self._primed.add((id(self.session), prime))
                except Exception:
                    pass
            return {}
        return {"auth": tuple(auth)}

    def _download(self, url: str, auth=None) -> tuple[bytes, dict[str, str]]:
        lock = self._polite(url)
        try:
            return self._download_inner(url, auth)
        finally:
            if lock:
                lock.release()

    def _download_inner(self, url: str, auth=None) -> tuple[bytes, dict[str, str]]:
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                kw = self._auth_kw(auth)
                with self.session.get(url, timeout=self.timeout, stream=True, **kw) as resp:
                    status = getattr(resp, "status_code", 200)
                    if status in (401, 403) and isinstance(auth, dict) and attempt < self.retries:
                        self._primed.discard((id(self.session), auth.get("prime")))   # cookie expired: visit the link again
                        raise IOError(f"HTTP {status}")
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

    def check_remote(self, url: str, entry: dict, auth=None) -> tuple[str, dict]:
        """Cheap change check without downloading: HEAD and compare validators.

        Returns ('unchanged' | 'changed' | 'unknown', observed headers)."""
        try:
            kw = self._auth_kw(auth)
            head = getattr(self.session, "head", None)
            if head is None:
                return "unknown", {}
            r = head(url, timeout=(15, 30), allow_redirects=True, **kw)
            if getattr(r, "status_code", 200) >= 400:
                return "unknown", {}
            h = {k.lower(): v for k, v in dict(getattr(r, "headers", {}) or {}).items()}
        except Exception:
            return "unknown", {}
        seen = {"etag": h.get("etag"), "last_modified": h.get("last-modified"),
                "length": int(h["content-length"]) if str(h.get("content-length", "")).isdigit() else None}
        compared = False
        if seen["etag"] and entry.get("etag"):
            compared = True
            if seen["etag"] != entry["etag"]:
                return "changed", seen
        if seen["last_modified"] and entry.get("last_modified"):
            compared = True
            if seen["last_modified"] != entry["last_modified"]:
                return "changed", seen
        if seen["length"] and entry.get("bytes") and "gzip" not in str(h.get("content-encoding", "")):
            compared = True
            if seen["length"] != entry["bytes"]:
                return "changed", seen
        return ("unchanged" if compared else "unknown"), seen

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
        if kind == "p7m":
            inner = p7m_content(raw)
            if inner:
                raw, kind = inner, sniff(inner)                # signed PDF/ZIP: analyse the signed file
        if kind == "pdf":
            result.members = [Member(url.rsplit("/", 1)[-1] + ".pdf", "pdf", raw, sha, 0)]
        elif kind == "zip":
            result.members, result.skipped_members = expand_archive(raw, sha, self.max_bytes)
        else:
            result.skipped_members = [{"name": url, "reason": f"format {kind} not parsed", "size": len(raw)}]
        return result
