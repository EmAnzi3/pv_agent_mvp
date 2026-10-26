# Wind Radar v0.6 — Institutional & Source Network

Data: 2026-09-05

## Perché serve un terzo layer

Un network commerciale ampio non basta. Il Radar deve lavorare su tre grafi distinti ma collegabili:

1. **Project graph** — progetto, identità, configurazione, stage, timing, scope e gap.
2. **Company / commercial graph** — developer, OEM, EPC/BoP, civili, elettrico, grid, logistica, engineering, O&M e target adiacenti.
3. **Institutional / source graph** — MASE, Terna, Regioni, portali VIA/PAUR, atti autorizzativi e dataset FER che possono anticipare cambi di progetto e nominare nuovi player.

Il terzo layer è necessario per non dipendere solo da comunicati aziendali o da progetti già noti.

## Riutilizzo di `pv_agent_mvp`

La repository `EmAnzi3/pv_agent_mvp` contiene già un patrimonio utile di endpoint e meccaniche di raccolta. Nel suo `app/main.py` risultano attivi collector regionali per Veneto, Emilia-Romagna, Lombardia, Sicilia, Sistema Puglia Energia, Lazio, Sardegna, Toscana, Toscana ATOS, Piemonte, Campania, Calabria e Umbria, oltre ai collector nazionali MASE, MASE provvedimenti e Terna Econnextion.

Sono inoltre presenti:
- `app/collectors/basilicata.py`, disponibile ma non cablato nel main corrente;
- `app/collectors/puglia.py`, presente ma disabilitato, mentre `sistema_puglia_energia.py` è attivo.

Questo patrimonio **non viene copiato meccanicamente**. Molti collector PV hanno filtri che escludono esplicitamente `eolico`: nel Wind Radar riutilizziamo endpoint, request mechanics, parsing, GIS e normalizzazione, ma sostituiamo i filtri di dominio.

## Fonti ad alta priorità già identificate

### Nazionali
- MASE VIA/PUA/scoping/documentazione;
- MASE provvedimenti e decreti;
- Terna Econnextion come intelligence aggregata sulla connessione, mai come elenco progetti.

### Regionali A
- Puglia — Sistema Puglia / Transizione Energetica;
- Puglia — dataset open VIA FER;
- Sardegna — SIRA ricerca progetti;
- Sicilia — SI-VVI + layer GIS;
- Basilicata — portale VIA regionale / Screening;
- Calabria — avvisi VIA/VAS/PAUR;
- Campania — ricerca VIA/PAUR;
- Toscana — GeA/STAR;
- Toscana — ATOS ARRR, particolarmente interessante perché espone mappa FER, stato autorizzativo e ultimo atto;
- Lazio — collector già attivo in `pv_agent_mvp`, endpoint da auditare per l'adattamento wind.

## Gap espliciti

Il registry non finge copertura nazionale completa. I gap regionali attuali sono:
- Abruzzo;
- Friuli-Venezia Giulia;
- Liguria — **priorità A** per rilevanza wind/repowering;
- Marche;
- Molise — **priorità A** per presenza di progetti canonici;
- Trentino-Alto Adige;
- Valle d'Aosta.

Questi gap devono essere chiusi con endpoint ufficiali verificati, non con scraping commerciale generico.

## Cadenza

- MASE core: giornaliera;
- fonti regionali priorità A: ogni 3 giorni;
- altre regionali: ogni 7 giorni;
- audit copertura nazionale: ogni 30 giorni.

## Regola probatoria

Il fatto che un portale sia ufficiale non trasforma automaticamente ogni dato in una relazione esecutiva. Il canale è istituzionale; l'evidenza A1 nasce solo da uno specifico atto/documento riferito al progetto.

Esempi:
- un decreto VIA può sostenere stage/configurazione ma non necessariamente l'Autorizzazione Unica;
- una tavola di cantierizzazione può nominare un progettista ma non provare il Civil BoP;
- un atto regionale che nomina esplicitamente un affidatario può invece diventare evidenza project-specific A1.

## Obiettivo operativo v0.6

La v0.6 deve quindi evolvere da "Radar progetti + contractor" a un sistema di intelligence a tre reti:

**Progetti ↔ Aziende ↔ Fonti/Enti**

con refresh indipendenti, provenance e regole probatorie separate.
