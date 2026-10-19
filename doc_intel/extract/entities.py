"""Companies, VAT numbers, contacts, persons and *documented* roles.

Rules enforced here (mandate §5.A):
* a role is assigned only from an explicit label next to the name
  ("Proponente:", "Progettista:", "Impresa esecutrice:"...);
* execution roles (EPC, BoP, contractor) additionally require a proper name and
  no conditional/template wording ("sarà individuata", "dovrà");
* persons are emitted only when attached to a role label, never because they
  appear near a company;
* VAT check-digit validity is recorded, but it never makes a VAT "verified":
  identity needs a registry check (Registro Imprese / VIES).
"""
from __future__ import annotations

import re

from ..schema import Fact
from .context import PageContext

LEGAL_FORM = (r"(?-i:S\.\s?N\.\s?C\.?|SNC)(?![A-Za-z])|(?i:S\.?\s?R\.?\s?L\.?(?:\s?S\.?)?(?![A-Za-z])|S\.?\s?P\.?\s?A\.?(?![A-Za-z])|S\.?\s?A\.?\s?S\.?(?![A-Za-z])|"
              r"S\.?\s?C\.?\s?A\.?\s?R\.?\s?L\.?|"
              r"SOCIET[AÀ]'?\s+A\s+RESPONSABILIT[AÀ]'?\s+LIMITATA|GMBH|S\.?A\.?U\.?|S\.?L\.?U?\.?(?![A-Za-z]))")
COMPANY_RE = re.compile(
    r"(?P<name>(?:[A-Z0-9][\w&'’.\-]*)(?:\s+[A-Za-z0-9&'’.\-]+){0,6}?)\s*,?\s+(?P<form>" + LEGAL_FORM + r")",
    re.UNICODE)
VAT_RE = re.compile(
    r"(?i)(?:P\.?\s?\.?\s?IVA|Partita\s+I\.?V\.?A\.?|P\.?\s?I\.(?!\w)|VAT(?:\s+(?:n(?:o|umber)?\.?))?|"
    r"C\.?\s?F\.?\s*(?:e|/|-)\s*P\.?\s?IVA|Cod(?:ice)?\.?\s*Fisc(?:ale)?\.?\s*(?:e|/)\s*P\.?\s?IVA)"
    r"\s*[:n°.\-]*\s*(?:IT\s?)?(?P<vat>\d{11})\b")
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
PEC_DOMAINS = ("pec", "legalmail", "postacert", "arubapec", "casellapec", "pecimprese", "cert.")
PHONE_RE = re.compile(r"(?i)\b(?:tel(?:efono)?|cell(?:ulare)?|mob(?:ile)?|ph(?:one)?)\.?\s*[:.]?\s*"
                      r"(?P<num>(?:\+39[\s.]?)?(?:\(?0\d{1,4}\)?|3\d{2})[\s./-]?\d{3,4}[\s./-]?\d{2,5})")
WEB_RE = re.compile(r"(?i)\b(?:www\.)[a-z0-9\-]+\.[a-z]{2,}(?:/[^\s]*)?")
TITLE = r"(?:Ing\.|Arch\.|Geol\.|Dott\.(?:\s?(?:Ing|Geol|Agr|For|Arch)\.)?|Dr\.|Geom\.|Per\.\s?Ind\.|Avv\.|Agr\.|P\.?\s?I\.)"
PERSON_RE = re.compile(TITLE + r"\s*(?P<name>[A-ZÀ-Ý][A-Za-zÀ-ÿ'’]+(?:\s+[A-ZÀ-Ý][A-Za-zÀ-ÿ'’]+){1,3})")

# label regex -> (role, is_execution_role)
ROLE_LABELS: list[tuple[re.Pattern, str, bool]] = [
    (re.compile(r"(?i)\b(?:soggetto\s+)?proponent[ei]\b|\brichiedente\b|\bcommittente\b"), "proponent", False),
    (re.compile(r"(?i)\bprogettist[ai](?:\s+(?:incaricat[oa]|responsabile))?\b|\bprogetto\s+a\s+cura\s+d[ie]\b|\bprogettazione\b"), "designer", False),
    (re.compile(r"(?i)\bgeolog[oia]\b|\brelazione\s+geologica\b"), "geologist", False),
    (re.compile(r"(?i)\bdirett(?:ore|ion[ei])\s+(?:dei\s+)?lavori\b|\bD\.\s?L\.(?=\s|:)"), "works_director", False),
    (re.compile(r"(?i)\bcoordinatore\s+(?:per\s+la\s+)?sicurezza\b|\bC\.?S\.?[PE]\.?\s*:"), "safety_coordinator", False),
    (re.compile(r"(?i)\bimpresa\s+(?:esecutrice|appaltatrice|affidataria)\b|\bappaltator[ei]\b|\bgeneral\s+contractor\b|\bcontraente\s+generale\b"), "contractor", True),
    (re.compile(r"(?i)\bEPC(?:\s+contractor)?\b"), "epc", True),
    (re.compile(r"(?i)\b(?:civil|electrical|elettrico|civile)?\s*BoP\b|\bbalance\s+of\s+plant\b"), "bop", True),
    (re.compile(r"(?i)\bfornitor[ei]\s+(?:dei\s+)?(?:moduli|inverter|tracker|aerogeneratori|turbine|BESS)\b"), "supplier", True),
    (re.compile(r"(?i)\bO\s?&\s?M\b|\bmanutenzione\s+e\s+gestione\b"), "om", True),
    (re.compile(r"(?i)\bgestore\s+(?:della\s+|di\s+)?rete\b"), "grid_operator", False),
]
CONDITIONAL = re.compile(
    r"(?i)\b(?:sar[aà]\s+(?:individuat|scelt|selezionat|affidat)|dovr[aà]|verr[aà]|da\s+individuare|"
    r"da\s+definire|in\s+fase\s+di\s+(?:gara|affidamento)|eventual[ei]|non\s+ancora|potr[aà])")


def _clean_company(name: str, form: str) -> str:
    name = name.strip()
    while True:
        new = re.sub(r"^(?:la\s+|il\s+|dalla\s+|della\s+|societ[aà]\s+|soc\.\s*|ditta\s+|ragione\s+sociale\s*:?\s*)",
                     "", name, flags=re.I)
        if new == name:
            break
        name = new
    name = re.sub(r"^.*?\b(?:proponente|committente|richiedente|progettista|appaltatrice|esecutrice|contractor|"
                  r"per\s+conto\s+d(?:i|ella|el)|dell'impresa|della\s+societ[aà]|spett\.?le|EPC|BoP)\s*:?\s*",
                  "", name, flags=re.I)
    name = name.strip(" ,:;-")
    if ADDRESS_WORDS.search(name):
        return ""
    form_n = re.sub(r"[\s.]", "", form).upper()
    form_n = {"SRL": "S.r.l.", "SPA": "S.p.A.", "SAS": "S.a.s.", "SNC": "S.n.c.", "SRLS": "S.r.l.s.",
              "SCARL": "S.c.a r.l."}.get(form_n, form.strip())
    if re.match(r"(?i)SOCIET", form):
        form_n = "S.r.l."
    return f"{name} {form_n}".strip()


ADDRESS_WORDS = re.compile(r"(?i)\b(?:via|viale|piazza|piazzale|corso|strada|località|loc\.|caserma|largo|vicolo|frazione|c\.so|p\.zza)\b")


def company_key(name: str) -> str:
    n = name.casefold()
    n = re.sub(r"societ[aà]'?\s+a\s+responsabilit[aà]'?\s+limitata", "srl", n)
    n = re.sub(r"[^a-z0-9]", "", n)
    return n


def find_companies(text: str) -> list[tuple[int, str]]:
    out = []
    for m in COMPANY_RE.finditer(text):
        name = _clean_company(m.group("name"), m.group("form"))
        if not name or len(re.sub(r"[^A-Za-z]", "", name)) < 4:
            continue
        out.append((m.start(), name))
    return out


def valid_vat_it(value: str) -> bool:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) != 11:
        return False
    total = 0
    for i, ch in enumerate(digits[:10]):
        d = int(ch)
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return (10 - total % 10) % 10 == int(digits[-1])


def _nearest_company(ctx: PageContext, i: int, pos: int) -> tuple[str | None, str]:
    """Company on the same line before the match, else in the 4 previous lines (title-block
    layout: name / address / VAT), else on the next line. The distance is recorded and only
    ``same_line`` is accepted by the proposal gate."""
    same = [c for c in find_companies(ctx.lines[i]) if c[0] < pos]
    if same:
        return same[-1][1], "same_line"
    for back in (1, 2, 3, 4):
        if i - back >= 0:
            prev = find_companies(ctx.lines[i - back])
            if prev:
                return prev[-1][1], f"previous_line_{back}"
    if i + 1 < len(ctx.lines):
        nxt = find_companies(ctx.lines[i + 1])
        if nxt:
            return nxt[0][1], "next_line_1"
    return None, "none"


def extract(ctx: PageContext) -> list[Fact]:
    facts: list[Fact] = []
    for i, line in enumerate(ctx.lines):
        if not line:
            continue
        for m in VAT_RE.finditer(line):
            vat = m.group("vat")
            subject, how = _nearest_company(ctx, i, m.start())
            ok = valid_vat_it(vat)
            facts.append(Fact(
                field="company.vat", value=vat, subject=subject, citation=ctx.cite(ctx.window(i, 1, 0)),
                status="candidate" if ok else "rejected",
                qualifiers={"checksum_ok": ok, "subject_association": how},
                limits="Check digit is not a registry check; association with the company is positional"
                       + ("" if subject else " and no company name was found nearby"),
                extractor="entities.vat"))
        for m in EMAIL_RE.finditer(line):
            mail = m.group(0).rstrip(".").lower()
            subject, how = _nearest_company(ctx, i, m.start())
            is_pec = any(d in mail.split("@", 1)[1] for d in PEC_DOMAINS)
            facts.append(Fact(field="contact.pec" if is_pec else "contact.email", value=mail, subject=subject,
                              citation=ctx.cite(ctx.window(i, 1, 0)), qualifiers={"subject_association": how},
                              limits="Contact as printed in the document; currency not verified",
                              extractor="entities.email"))
        for m in PHONE_RE.finditer(line):
            num = re.sub(r"[^\d+]", "", m.group("num"))
            if len(num.lstrip("+")) < 8:
                continue
            subject, how = _nearest_company(ctx, i, m.start())
            facts.append(Fact(field="contact.phone", value=num, subject=subject, citation=ctx.cite(line),
                              qualifiers={"subject_association": how},
                              limits="Phone as printed; owner and currency not verified", extractor="entities.phone"))
        for m in WEB_RE.finditer(line):
            subject, how = _nearest_company(ctx, i, m.start())
            facts.append(Fact(field="contact.website", value=m.group(0).lower(), subject=subject,
                              citation=ctx.cite(line), qualifiers={"subject_association": how},
                              extractor="entities.web"))
        facts.extend(_roles(ctx, i, line))
    return facts


def _role_value(ctx: PageContext, i: int, label_end: int) -> tuple[str | None, str | None, str]:
    """Name written after the label on the same line, or on the next non-empty line."""
    tail = ctx.lines[i][label_end:]
    tail = re.sub(r"^\s*(?:[:\-–]|è|e'|risulta|:?\s*la\s+societ[aà])\s*", "", tail, flags=re.I)
    candidates = [tail]
    j = i + 1
    while j < len(ctx.lines) and not ctx.lines[j]:
        j += 1
    if j < len(ctx.lines) and len(tail.strip()) < 3:
        candidates.append(ctx.lines[j])
    for text in candidates:
        comp = find_companies(text[:160])
        pers = PERSON_RE.search(text[:160])
        if comp and (not pers or comp[0][0] <= pers.start()):
            return "company", comp[0][1], text
        if pers:
            return "person", pers.group(0).strip(), text
    return None, None, tail


def _roles(ctx: PageContext, i: int, line: str) -> list[Fact]:
    out: list[Fact] = []
    for rx, role, execution in ROLE_LABELS:
        m = rx.search(line)
        if not m:
            continue
        kind, value, raw = _role_value(ctx, i, m.end())
        if not value:
            continue
        if role in ("proponent", "contractor", "epc", "bop", "supplier", "om", "grid_operator") and kind != "company":
            continue  # these roles are held by legal entities; a person here is a signature, not the role
        excerpt = ctx.window(i, 0, 1)
        if execution:
            if CONDITIONAL.search(excerpt):
                # template/conditional wording: keep as rejected evidence, never as a role
                out.append(Fact(field=f"role.{role}", value=value, role=role, citation=ctx.cite(excerpt),
                                status="rejected", temporal="template_reference",
                                limits="Conditional or template wording: no award is documented",
                                extractor="entities.role"))
                continue
        out.append(Fact(
            field=f"role.{role}", value=value, role=role, subject=value, citation=ctx.cite(excerpt),
            qualifiers={"entity_kind": kind, "execution_role": execution, "label": m.group(0)},
            limits=("Explicit label in the document; an award/contract is not proven by a project document"
                    if execution else "Role as labelled in this document/revision"),
            extractor="entities.role"))
    return out
