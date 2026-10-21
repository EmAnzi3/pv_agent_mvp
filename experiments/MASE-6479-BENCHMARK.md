# MASE 6479 — Crawl4AI pilot benchmark (2026-10-19)

**Repository:** `pv_agent_mvp`, isolated Draft PR #4. **Project:** Troia Sun, agrivoltaico 54.5 MW and separate 10 MW BESS, MASE provvedimento 6479, project page `/it-IT/Oggetti/Info/11109`, documents `/it-IT/Oggetti/Documentazione/11109/16669`.

## Evidence from live run #39177892707

- GitHub Actions: SUCCESS, installation and 4 isolated unit tests passed.
- Crawl4AI HTML: MASE provvedimento -> project page -> documentation index; **11 document download links discovered**.
- First 10 links attempted: **9 PDF parsed**, 1 HTTP 200 `application/octet-stream` response rejected as non-PDF; one link not attempted due to cap.
- Parsed 9 PDFs: **387 total pages**, **117 pages read** (40-page cap/file), **270 pages unexamined**. Original bytes SHA256 recorded for each parsed PDF.
- 979 regex/keyword hits **from PDFs**, not verified structured findings: power 179, proponent 186, designer 6, grid 122, storage 85, authorization 337, works 64.
- Example documented page 1 of `/File/Documento/1438129`: `Proponente TROIA SUN S.r.l.`; same document shows ID 12863 on page 2.
- Example page 1 of `/File/Documento/1126265`: 54.5 MWp + storage 10 MW, RTN works in Troia.
- **No EPC, BoP, award, COD, contractors or authorization-to-build verified.** No new canonical records written.

## Decision

**Proof of feasibility for source discovery: YES. Production enrichment: NOT YET.** Crawl4AI successfully navigates actual MASE project/document pages. PDF extraction is performed by **pypdf**, not Crawl4AI. The keyword matcher is noisy, not an analyst: repeated/legal references must not become field values. The test does not measure recall against an independently verified full MASE attachment inventory.

## Remaining acceptance gates

1. Resolve the `/File/Provvedimento/13107` octet-stream safely (inspect magic bytes/actual format, never assume PDF).
2. Enumerate paged/tabbed document indexes and compare observed 11 links to MASE published inventory. Avoid an assertion of full index coverage.
3. Process full pages or selective target sections with explicit page coverage, OCR only where needed.
4. Extract *project-specific structured fields* (proponent, grid works, equipment, EPC/BoP, relevant schedule), attach page, quote, date and status; suppress false positives and duplicates.
5. Benchmark verified new facts against baseline `docs/data.json` and manual reference, classify correct/incorrect/missing and measure time/cost.
6. Keep all results review-only and external to published dataset until gate is passed.

## Run
[Actions #39177892707](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39177892707). Artifact `crawl4ai-mase-6479` includes raw JSON diagnostics (retained for 7 days). Never judge from workflow SUCCESS alone.
