# Wind Project & Contractor Radar

MVP operativo del radar eolico nazionale, isolato in `docs/wind/` e costruito per leggere la pipeline dal punto di vista commerciale e della supply chain esecutiva.

## Portafoglio canonico

Il Radar pubblico corrente contiene:

- **51 progetti / 11.202,52 MW eolici**;
- **17 seed originari / 1.496,9 MW**;
- **34 progetti promossi dal Discovery v0.4 / 9.705,62 MW**, più **311 MW BESS** mantenuti separati;
- **441 MW BESS** complessivi nel canonico, sempre separati dai MW wind;
- maturità osservabile `E0–E8`;
- evidence grading `A1/A2/B/C/D`;
- contractor esecutivo conteggiato nei KPI solo con ruolo esecutivo `confirmed` e confidenza `A1/A2`.

## Discovery interno

Discovery è una coda tecnica di ricerca, **non una sezione pubblica della dashboard**. I candidati vengono:

- promossi nel canonico quando identità, attività corrente, configurazione e stage sono sufficientemente verificati;
- mantenuti internamente se reali ma ancora incompleti;
- rimossi dalla coda attiva se falsi, duplicati o non più validi, conservando soltanto le guardie negative utili a evitare reintroduzioni errate.

La triage corrente è in `data/discovery-triage-v06.json`.

## Dataset

Il manifest canonico è `data/projects.json`, con metadati in `data/meta.json` e progetti suddivisi in chunk. GlobalData è esclusivamente enrichment/lead source e non prevale su atti, developer, contractor o documentazione di cantiere.

Ogni progetto conserva, dove disponibili:

- anagrafica e potenza wind/BESS;
- developer/SPV e identificativi;
- fase E0–E8;
- timing e milestone;
- relazioni progetto ↔ azienda con ruolo, stato, fonte e confidenza;
- contractor gap;
- storico delle configurazioni MW/WTG;
- fonti/evidenze.

## Mappa

La vista principale usa una **choropleth ECharts per provincia**. Ogni progetto viene aggregato una sola volta sulla provincia canonica principale per evitare duplicazioni dei MW; per l’offshore la provincia è un riferimento amministrativo/territoriale e non rappresenta il footprint delle WTG in mare. I MW BESS restano separati.

## Contractor view

La vista inversa mostra azienda → progetti → MW → ruolo → stato → timing, separando relazioni A1/A2 confermate dai segnali B/C.

## Motore di intelligence

Player & Network Watch e Institutional & Source Watch alimentano il Radar ma non vengono mostrati come sezioni a piena pagina. La loro copertura è riportata nella metodologia della dashboard.

## Regole di attribuzione

Non assegnare mai un ruolo esecutivo per deduzione. Segnali B/C restano intelligence e non diventano affidamenti. GlobalData non è fonte canonica.

## Avvio locale

Doppio click su `aggiorna_wind_radar.bat` dalla radice del repository: aggiorna i watch dovuti, genera il digest review-only, valida il dataset e apre la dashboard via HTTP locale. Il riquadro **Ultimo run del BAT** mostra l'esito dell'aggiornamento.

Varianti: `all` forza tutti i watch; `offline` valida/apre senza interrogare fonti esterne. Il canonico non viene mai modificato automaticamente dal launcher.

## Stato

MVP in Draft PR per preview/revisione. Nessun merge e nessuna pubblicazione senza approvazione esplicita.
