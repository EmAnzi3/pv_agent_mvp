# CHANGELOG — PV Agent MVP

Formato consigliato: voci brevi, orientate a cosa cambia per il progetto.

## Unreleased

- 2026-10-20 — `doc_intel`: i documenti originali vengono eliminati dopo l'estrazione; memoria persistente per progetto (manifest, fatti per SHA-256) con rilevamento di documenti nuovi, modificati o rimossi; adapter regionali Emilia-Romagna, Lazio (Box), Veneto (Nextcloud), regole Basilicata e Sistema Puglia.
- 2026-10-19 — `doc_intel`: run resumibili entro i limiti CI, controllo anagrafico P. IVA su VIES, correzioni di precisione dai primi run reali; dossier Tortona 2 chiuso per review umana.
- 2026-10-19 — Aggiunta toolbox documentale `doc_intel` (review-only): adapter MASE generico senza ID hardcoded, cache SHA-256 incrementale, OCR selettivo a 300 DPI, estrattori di società/P. IVA/ruoli, configurazione tecnica, coordinate con oggetto e CRS, cronoprogrammi con natura temporale, conflitti, confronto col canonico per URL esatto, gate bloccanti, dossier JSON/MD, gold set Troia Sun e Tortona 2, test offline e workflow CI. Nessuna modifica al dataset pubblicato.
- 2026-10-15 — Impostata a 19,8 MW la potenza del progetto Porto Viro FEV (FOOD ENERGY VALLEY), su indicazione dell'utente: correzione persistente associata all'URL esatto e applicata dal batch prima del ricalcolo dei riepiloghi e dei report.
- 2026-10-05 — Rafforzato il collector Piemonte SKVIA: ricerca tramite l'evento reale del pulsante `Ricerca`, rimosso il filtro esclusivo `REGIONE PIEMONTE` per acquisire anche `SOGGETTO GESTORE RN2000`, ed esclusione dei procedimenti conclusi quando il dettaglio contiene un esito esplicitamente negativo. Aggiunto regression check sui record mancanti rilevati manualmente.
- 2026-10-02 — Aggiunta correzione documentata della potenza per il nuovo progetto AG 31 di Castagnaro: 21,13886 MWp (21.138,86 kWp), mantenendo l'override AG 31 esistente.
- 2026-09-10 — Corretta e protetta la localizzazione di Sesto al Reghena: PN / Friuli-Venezia Giulia indipendentemente dall'URL MASE; la correzione viene applicata prima del gate bloccante sia nella pipeline standalone sia nel batch completo.
- 2026-06-02 — Aggiunti file minimi di manutenzione repository: `CURRENT_STATE.md`, `AGENTS.md`, `CHANGELOG.md` e `scripts/check_before_publish.ps1`.
