# Crawl4AI pilot: PV Agent, existing MASE record

This optional experiment reads `docs/data.json`, deterministically selects an **existing record with an HTTPS MASE detail URL**, and analyzes its landing page and up to three discoverable attached PDFs. It never creates or edits projects and never alters the SQLite DB or published dashboard.

Run from the repository root in an **isolated environment**:

```bash
python -m venv .venv-crawl
.venv-crawl/bin/pip install crawl4ai pypdf requests
.venv-crawl/bin/crawl4ai-setup
.venv-crawl/bin/python -m unittest discover -s tests -p "test_crawl4ai_mase_record.py"
.venv-crawl/bin/python experiments/crawl4ai_mase_record.py --max-pdfs 3
```

On Windows use `.venv-crawl\Scripts\python.exe` and `python -m crawl4ai-setup` if the entrypoint is available. Dependencies are **not** added to the production requirements.

The default report is `reports/crawl4ai/mase-record-pilot.json`, with the selected dataset record, MASE URL, discovered links, failures, PDF byte SHA256, page numbers, and review-only field mentions. Set `--title-contains "fragment"` to pin an existing record. The first selected record is sorted lexicographically to be reproducible, not chosen by project relevance.

## Gate
Compare with the original source and the existing MASE collector. Verify that identified links actually belong to the selected project; PDF mentions are only unverified candidates. The prototype deliberately does **not** infer EPC, suppliers, permits or authorization outcomes from proximity of words. It does not follow pagination or JavaScript-triggered downloads outside extracted hrefs. PDF scan/OCR and indirect download endpoints are not supported. Any missing reference remains missing.

**Status:** code prepared; live crawl has not been run or benchmarked yet. Do not merge until real-source validation.

## Pinned real-world test — MASE provvedimento 6479

The supplied public URL is:
`https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6479`

Run:
```bash
python experiments/crawl4ai_mase_record.py --url 'https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6479' --max-pdfs 3
```

**Fail-closed requirement:** the supplied URL must be present in one record in `docs/data.json` (including its nested sources); if not found the experiment stops and reports the missing association instead of crawling a different project. Current public MASE pages mention a security-related temporary suspension; no live document extraction has been verified for this URL. A provvedimento page is not necessarily the full document index for the underlying project. Do not infer annex coverage.
