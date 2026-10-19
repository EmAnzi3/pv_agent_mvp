# doc_intel — Document Intelligence Toolbox (review-only)

Dato un record già scoperto da un collector, costruisce un dossier documentale verificabile. **Non scrive mai** in `docs/data.json` né nel database.

```
record URL ─► adapters (B) ─► fetch + store (C) ─► parse/OCR (C) ─► extract (D) ─► reconcile (D) ─► canonical + gates (E) ─► dossier JSON + MD
```

## Uso

```bash
python -m doc_intel run --url https://va.mite.gov.it/it-IT/Oggetti/Info/<id> \
    --store .doc_intel_store --data docs/data.json --out reports/doc_intel [--gold doc_intel/gold/<file>.json]
python -m doc_intel evaluate --dossier reports/doc_intel/mase-<id>.json --gold doc_intel/gold/mase-<id>.json
python -m unittest tests.test_doc_intel          # offline, nessuna rete
```

In CI: `doc-intel-tests.yml` (offline, con e senza PyMuPDF) e `doc-intel-run.yml` (benchmark live su PR, o qualunque URL via *Run workflow*).

## Regole non negoziabili (implementate e testate)

- Ruoli solo da etichetta esplicita; EPC/BoP/appaltatori con formula condizionale → `rejected`; qualunque ruolo esecutivo in proposta → gate bloccante.
- P. IVA: check digit registrato ma **mai** sufficiente; doppia P. IVA per la stessa società o stessa P. IVA su società diverse → conflitto.
- Coordinate: conversione UTM→WGS84 solo con fuso dichiarato in pagina; oggetto della coordinata sempre classificato; cabine/SSE/PVGIS mai "posizione impianto"; poligoni solo da vertici di area.
- Cronoprogramma: durate ≠ date; date classificate `event_occurred` / `planned` / `template_reference` / `scenario` / `conditional` / `unknown`; nessuna data dedotta da una durata.
- Conflitti mai risolti scegliendo il più recente; `preference_hint` solo se un unico valore compare in un atto, sempre `binding: false`.
- Potenze FV DC, FV AC, BESS MW/MWh ed eolico in campi distinti, mai sommati.

## Persistenza e costi

Store content-addressed (`blobs/`, `text/`, `manifests/`) fuori da Git, persistito con `actions/cache`. Un URL già in manifest non viene riscaricato; un contenuto già parsato non viene rielaborato; `--refresh` rileva revisioni (SHA diverso → `history`). OCR solo su pagine senza testo utile, con budget (`--ocr-max-pages`) e cap pixel per le tavole grandi. Nessun servizio a pagamento.

## Schema

`pv-dossier/1.0`: `project`, `acquisition`, `inventory`, `facts`, `groups` (versioni/conflitti), `canonical`, `canonical_record`, `delta` (5 classi), `proposal`, `gates`, `review`, `efficiency`, opzionale `evaluation`.
