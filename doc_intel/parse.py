"""Layer C (part 2): page-level text extraction with selective OCR.

Backends, in order of preference (first available wins, others are fallbacks
on error): PyMuPDF (``fitz``) -> poppler ``pdftotext`` -> ``pypdf``.
OCR renders only pages whose digital text is not useful, at a resolution that
is actually readable on engineering drawings (default 300 DPI; the PR #4 OCR
used ~94 DPI, which is too low for cartigli and coordinate tables).
"""
from __future__ import annotations

import io
import re
import shutil
import subprocess
import tempfile
import threading
import time
import os
from pathlib import Path
from typing import Any

from .store import Store

USEFUL_MIN_CHARS = 60
MAX_RENDER_PIXELS = 25_000_000   # ~A4 at 400 DPI / A0 at ~120 DPI: bounded time per page


def useful_chars(text: str) -> int:
    return len(re.findall(r"[A-Za-zÀ-ÿ0-9]", text or ""))


def _has_fitz() -> bool:
    try:
        import fitz  # noqa: F401
        return True
    except Exception:
        return False


class PdfBackend:
    name = "base"

    def __init__(self, raw: bytes):
        self.raw = raw

    def page_count(self) -> int: ...
    def page_text(self, n: int) -> str: ...
    def render_png(self, n: int, dpi: int) -> bytes: ...


class FitzBackend(PdfBackend):
    name = "pymupdf"

    def __init__(self, raw: bytes):
        super().__init__(raw)
        import fitz
        self._fitz = fitz
        self.doc = fitz.open(stream=raw, filetype="pdf")

    def page_count(self) -> int:
        return len(self.doc)

    def page_text(self, n: int) -> str:
        return self.doc[n - 1].get_text("text") or ""

    def render_png(self, n: int, dpi: int) -> bytes:
        rect = self.doc[n - 1].rect
        zoom = dpi / 72.0
        # large drawings (A0 at 300 DPI = 140 MP) are capped to keep memory and OCR time bounded
        pixels = rect.width * rect.height * zoom * zoom
        if pixels > MAX_RENDER_PIXELS:
            zoom *= (MAX_RENDER_PIXELS / pixels) ** 0.5
        pix = self.doc[n - 1].get_pixmap(matrix=self._fitz.Matrix(zoom, zoom), alpha=False)
        return pix.tobytes("png")


class PopplerBackend(PdfBackend):
    name = "poppler"

    def __init__(self, raw: bytes):
        super().__init__(raw)
        if not shutil.which("pdftotext"):
            raise RuntimeError("poppler not installed")
        self.tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        self.tmp.write(raw)
        self.tmp.flush()
        from pypdf import PdfReader
        self._count = len(PdfReader(io.BytesIO(raw), strict=False).pages)

    def page_count(self) -> int:
        return self._count

    def page_text(self, n: int) -> str:
        out = subprocess.run(["pdftotext", "-layout", "-f", str(n), "-l", str(n), self.tmp.name, "-"],
                             capture_output=True, timeout=120)
        if out.returncode:
            raise RuntimeError(out.stderr.decode(errors="replace")[:200])
        return out.stdout.decode("utf-8", errors="replace")

    def render_png(self, n: int, dpi: int) -> bytes:
        with tempfile.TemporaryDirectory() as d:
            prefix = str(Path(d) / "p")
            subprocess.run(["pdftoppm", "-r", str(dpi), "-f", str(n), "-l", str(n), "-png",
                            "-singlefile", self.tmp.name, prefix], check=True, capture_output=True, timeout=180)
            return Path(prefix + ".png").read_bytes()

    def __del__(self):
        try:
            Path(self.tmp.name).unlink(missing_ok=True)
        except Exception:
            pass


class PypdfBackend(PdfBackend):
    name = "pypdf"

    def __init__(self, raw: bytes):
        super().__init__(raw)
        from pypdf import PdfReader
        self.reader = PdfReader(io.BytesIO(raw), strict=False)

    def page_count(self) -> int:
        return len(self.reader.pages)

    def page_text(self, n: int) -> str:
        return self.reader.pages[n - 1].extract_text() or ""

    def render_png(self, n: int, dpi: int) -> bytes:
        raise RuntimeError("pypdf cannot render pages")


def open_backend(raw: bytes) -> tuple[PdfBackend, list[str]]:
    errors: list[str] = []
    candidates = []
    if _has_fitz():
        candidates.append(FitzBackend)
    candidates += [PopplerBackend, PypdfBackend]
    for cls in candidates:
        try:
            return cls(raw), errors
        except Exception as exc:
            errors.append(f"{cls.name}: {exc}"[:200])
    raise RuntimeError("no PDF backend could open the file: " + "; ".join(errors))


def ocr_png(png: bytes, langs: str = "ita+eng", timeout: int = 180) -> str:
    if not shutil.which("tesseract"):
        raise RuntimeError("tesseract not installed")
    available = subprocess.run(["tesseract", "--list-langs"], capture_output=True, text=True).stdout
    use = "+".join(l for l in langs.split("+") if l in available.split()) or "eng"
    env = {**os.environ, "OMP_THREAD_LIMIT": "1"}  # parallelism comes from workers, not tesseract threads
    proc = subprocess.run(["tesseract", "stdin", "stdout", "-l", use, "--psm", "3"],
                          input=png, capture_output=True, timeout=timeout, env=env)
    if proc.returncode:
        raise RuntimeError(proc.stderr.decode(errors="replace")[:200])
    return proc.stdout.decode("utf-8", errors="replace")


class OcrBudget:
    """Global page cap and wall-clock deadline: pages beyond either are deferred, never lost.

    Deferred pages stay in the text cache as ``ocr_pending`` and the next run resumes
    exactly those pages, so a long project converges over several bounded runs.
    """

    def __init__(self, max_pages: int = 400, deadline: float | None = None):
        self.max_pages = max_pages
        self.deadline = deadline
        self.used = 0
        self.deferred_by_time = 0
        self._lock = threading.Lock()

    def take(self) -> bool:
        with self._lock:
            if self.deadline is not None and time.time() >= self.deadline:
                self.deferred_by_time += 1
                return False
            if self.used >= self.max_pages:
                return False
            self.used += 1
            return True


def _ocr_page(backend: PdfBackend, n: int, text: str, dpi: int, langs: str) -> tuple[str, str, str | None]:
    try:
        ocr_text = ocr_png(backend.render_png(n, dpi), langs)
    except Exception as exc:
        return text, "ocr_failed", f"ocr: {exc}"[:200]
    if useful_chars(ocr_text) > useful_chars(text):
        return ocr_text, "ocr", None
    return text, "none_after_ocr", None


def extract_pages(raw: bytes, sha: str, store: Store | None = None, member: str | None = None,
                  ocr: bool = True, dpi: int = 300, budget: OcrBudget | None = None,
                  langs: str = "ita+eng") -> dict[str, Any]:
    """Return per-page text with the method used; cached by SHA-256 (+member).

    A cached result with ``ocr_pending`` pages is resumed: only those pages are OCR'd.
    """
    cached = store.get_text(sha, member) if store is not None else None
    if cached and ocr:
        # pages whose OCR failed (timeout, oversized render) are retried like deferred ones
        failed = [p["n"] for p in cached["pages"] if p["method"] == "ocr_failed"]
        if failed:
            cached["ocr_pending"] = sorted(set(cached.get("ocr_pending", [])) | set(failed))
            for p in cached["pages"]:
                if p["method"] == "ocr_failed":
                    p["method"] = "ocr_deferred"
    if cached and (not ocr or not cached.get("ocr_pending")):
        cached["from_cache"] = True
        return cached
    backend, errors = open_backend(raw)
    if cached:
        pages = cached["pages"]
        todo = set(cached["ocr_pending"])
    else:
        pages, todo = [], None
        for n in range(1, backend.page_count() + 1):
            text, method, err = "", "none", None
            try:
                text = backend.page_text(n)
            except Exception as exc:
                err = f"text: {exc}"[:200]
                try:  # page-wise fallback
                    text = PypdfBackend(raw).page_text(n)
                except Exception:
                    pass
            method = "digital" if useful_chars(text) >= USEFUL_MIN_CHARS else "none"
            pages.append({"n": n, "text": text, "method": method, "chars": useful_chars(text), "error": err})
    ocr_pending = []
    for p in pages:
        needs = p["method"] in ("none", "ocr_deferred") and (todo is None or p["n"] in todo)
        if not needs:
            continue
        if ocr and (budget is None or budget.take()):
            p["text"], p["method"], p["error"] = _ocr_page(backend, p["n"], p["text"], dpi, langs)
            p["chars"] = useful_chars(p["text"])
        else:
            p["method"] = "ocr_deferred" if ocr else "none"
            ocr_pending.append(p["n"])
    payload = {
        "sha256": sha, "member": member, "backend": backend.name, "backend_errors": errors,
        "page_count": len(pages), "pages": pages, "ocr_pending": ocr_pending, "dpi": dpi,
        "summary": summarize(pages), "resumed": bool(cached),
    }
    if store is not None:
        store.put_text(sha, payload, member)
    return payload


def summarize(pages: list[dict[str, Any]]) -> dict[str, int]:
    out = {"pages": len(pages), "digital": 0, "ocr": 0, "no_text": 0, "ocr_failed": 0, "ocr_deferred": 0}
    for p in pages:
        m = p["method"]
        if m == "digital":
            out["digital"] += 1
        elif m == "ocr":
            out["ocr"] += 1
        elif m == "ocr_failed":
            out["ocr_failed"] += 1
        elif m == "ocr_deferred":
            out["ocr_deferred"] += 1
        else:
            out["no_text"] += 1
    return out
