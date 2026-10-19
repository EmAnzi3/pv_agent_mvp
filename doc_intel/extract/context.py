"""Page context and Italian number parsing shared by extractors."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..schema import Citation


@dataclass
class PageContext:
    text: str
    url: str
    page: int
    sha256: str | None = None
    member: str | None = None
    procedure_id: str | None = None
    document_title: str | None = None
    document_date: str | None = None
    revision: str | None = None
    method: str | None = None
    lines: list[str] = field(init=False)

    def __post_init__(self):
        self.lines = [re.sub(r"[ \t ]+", " ", l).strip() for l in (self.text or "").splitlines()]

    @property
    def lowered(self) -> str:
        return self.text.casefold()

    def cite(self, excerpt: str) -> Citation:
        return Citation(url=self.url, document_sha256=self.sha256, page=self.page,
                        excerpt=excerpt.strip()[:400], member=self.member,
                        procedure_id=self.procedure_id, document_title=self.document_title,
                        document_date=self.document_date, revision=self.revision,
                        text_method=self.method)

    def window(self, i: int, before: int = 1, after: int = 1) -> str:
        lo, hi = max(0, i - before), min(len(self.lines), i + after + 1)
        return " ".join(l for l in self.lines[lo:hi] if l)


NUM = r"\d{1,3}(?:[.\s]\d{3})+(?:,\d+)?|\d+(?:[.,]\d+)?"


def parse_number(token: str) -> float | None:
    """Parse Italian/English numbers: '54,5' '1.234,56' '60.000' '512 345.67' '0.710'."""
    t = token.strip().replace(" ", " ")
    if not t:
        return None
    t = re.sub(r"(?<=\d) (?=\d{3}\b)", "", t)
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    elif "," in t:
        t = t.replace(",", ".")
    elif t.count(".") > 1:
        t = t.replace(".", "")
    elif "." in t:
        head, tail = t.split(".")
        # '60.000' with exactly 3 decimals and no leading zero -> thousands separator
        if len(tail) == 3 and head and head != "0" and len(head) <= 3:
            t = head + tail
    try:
        return float(t)
    except ValueError:
        return None
