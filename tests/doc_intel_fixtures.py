"""Offline fixtures for doc_intel tests: tiny PDFs and fake HTTP."""
from __future__ import annotations

import io
import zipfile


def make_pdf(pages: list[str]) -> bytes:
    """Minimal valid PDF, one Helvetica text block per page ('' = image-like empty page)."""
    objs: list[bytes] = []
    n = len(pages)
    kids = " ".join(f"{3 + 2 * i} 0 R" for i in range(n))
    objs.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objs.append(f"<< /Type /Pages /Kids [{kids}] /Count {n} >>".encode())
    font_id = 3 + 2 * n
    for i, text in enumerate(pages):
        content_id = 4 + 2 * i
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 {font_id} 0 R >> >> "
                    f"/Contents {content_id} 0 R >>".encode())
        lines = []
        y = 800
        for line in text.splitlines():
            safe = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            lines.append(f"BT /F1 9 Tf 40 {y} Td ({safe}) Tj ET")
            y -= 12
        stream = "\n".join(lines).encode("latin-1", errors="replace")
        objs.append(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")
    objs.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = []
    for i, body in enumerate(objs, 1):
        offsets.append(out.tell())
        out.write(f"{i} 0 obj\n".encode() + body + b"\nendobj\n")
    xref = out.tell()
    out.write(f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode())
    for off in offsets:
        out.write(f"{off:010d} 00000 n \n".encode())
    out.write(f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    return out.getvalue()


def make_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    return buf.getvalue()


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200):
        self.body = body
        self.status_code = status
        self.headers = {"ETag": "x"}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise IOError(f"HTTP {self.status_code}")

    def iter_content(self, size):
        for i in range(0, len(self.body), size):
            yield self.body[i:i + size]


class FakeSession:
    def __init__(self, routes: dict[str, bytes | Exception]):
        self.routes = routes
        self.calls: list[str] = []

    def get(self, url, timeout=None, stream=None, **kw):
        self.calls.append(url)
        v = self.routes.get(url)
        if v is None:
            return FakeResponse(b"", 404)
        if isinstance(v, Exception):
            raise v
        return FakeResponse(v)
