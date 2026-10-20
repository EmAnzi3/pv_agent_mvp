"""PV Agent Document Intelligence Toolbox (review-only).

Five strictly separated layers:

A. Discovery           -> existing collectors in ``app/collectors`` (untouched)
B. Document discovery  -> ``doc_intel.adapters``   (procedures, indices, pagination, manifest)
C. Document processing -> ``doc_intel.fetch`` / ``doc_intel.parse`` (bytes, SHA-256, ZIP, text, OCR)
D. Semantic layer      -> ``doc_intel.extract`` + ``doc_intel.reconcile``
E. Evidence validation -> ``doc_intel.gates`` + ``doc_intel.canonical`` + ``doc_intel.dossier``

Nothing in this package writes to ``docs/data.json`` or to the SQLite database.
"""

SCHEMA_VERSION = "pv-dossier/1.0"
TOOLBOX_VERSION = "0.1.0"


def _extractor_version() -> str:
    """Hash of the extractor sources: facts stored with another value come from older rules."""
    import hashlib
    from pathlib import Path
    h = hashlib.sha256()
    for f in sorted((Path(__file__).parent / "extract").glob("*.py")):
        h.update(f.read_bytes())
    return h.hexdigest()[:12]


EXTRACTOR_VERSION = _extractor_version()
