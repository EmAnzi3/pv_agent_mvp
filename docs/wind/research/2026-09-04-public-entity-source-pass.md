# Wind Radar — Public entity source pass

Data audit: 2026-09-04

## Scopo

Estendere le fonti del Wind Radar sui **17 progetti già censiti**, senza usare questo pass per aggiungere nuovi progetti. L'obiettivo è intercettare segnali di costruzione e supply chain che spesso non emergono dal fascicolo MASE: avvio lavori reale, espropri/occupazioni, viabilità, trasporti eccezionali, attraversamenti, ordinanze, opere di connessione e soggetti esecutori eventualmente nominati negli atti.

## Gerarchia fonti aggiuntiva

1. **Regione** — VIA/AU regionali, trasparenza, pubblicità legale/albo, BUR/BURP, determinazioni e pareri;
2. **Provincia / Città metropolitana** — strade provinciali, concessioni e attraversamenti, occupazioni, trasporti eccezionali, autorizzazioni ambientali di competenza;
3. **Comune** — Albo Pretorio, delibere/determine/ordinanze, SUAP/urbanistica, espropri, occupazioni, viabilità, Polizia Locale, autorizzazioni temporanee e comunicazioni tecniche;
4. **Enti infrastrutturali** — Terna, ANAS e gestori delle strade/reticoli interferiti;
5. **Proponente / contractor** — press room, portfolio, referenze, job posting e pagine corporate, sempre subordinate alle fonti A1 quando si attribuisce uno scope esecutivo.

## Perché gli enti locali sono ad alto valore

Gli elaborati tecnici stessi indicano che una parte delle informazioni operative passa necessariamente dagli enti territoriali. Fenice prevede attraversamenti e posa lungo strade secondo le indicazioni delle amministrazioni comunali e/o provinciali; Andretta-Bisaccia prevede autorizzazioni agli itinerari, coordinamento con Comuni, enti gestori e Polizia Locale per trasporti eccezionali e picchi di traffico. Di conseguenza, gli albi e i portali di trasparenza non sono una fonte accessoria: diventano una classe primaria per validare il passaggio dal permitting al cantiere reale.

## Primo pass: ALAS

### Documentazione acquisita

`PEALAS_PE_00016_01_00` è un **Progetto Esecutivo Opere Civili** di RWE Renewables Italia con Hydro Engineering indicata come progettista. Il cronoprogramma è prima emissione agosto 2024 e contiene baseline pianificate per campo base, fondazioni/piazzole, SSE RWE, cavidotto, ampliamento SSE Terna, anchor cage, getti, arrivo main components/erection, commissioning e COD.

Regola radar: il documento è A1 per il ruolo di progettazione di Hydro e per la sequenza pianificata, ma il calendario 2024–2026 non sostituisce lo stato reale successivo comunicato da RWE. Non attribuire a Hydro il Civil BoP esecutivo per deduzione.

### Fonte pubblica locale già trovata

Comune di Ittiri — comunicato del 20/05/2026 “Chiarimenti sui progetti Bess e Alas”:
https://www.comune.ittiri.ss.it/it/news/comunicato-stampa-chiarimenti-sui-progetti-bess-e-alas

La pagina comunale rinvia a documentazione che comprende la lettera del Sindaco alla Regione Sardegna del luglio 2024 sulla sospensione delle attività ALAS e documenti comunali sulle aree idonee/non idonee. È una prova concreta che il portale comunale contiene una traccia amministrativa del progetto non limitata al MASE.

### Coda enti

- Regione Sardegna: AU, eventuali volture/varianti, avvio lavori, espropri/occupazioni;
- Comune di Ittiri;
- Comune di Villanova Monteleone;
- Provincia/Città metropolitana competente per viabilità e trasporti;
- Terna per ampliamenti/commissioning della SSE.

## Primo pass: Toritto

### Documentazione acquisita

`C24PU001WP010R00`, progetto definitivo 28/03/2025, conferma 108 MW wind + 50 MW storage e un programma di **503 giorni relativi**:

- apertura/allestimento: giorni 1–10;
- strade/piazzole: fino a circa giorno 214;
- fondazioni: circa giorni 170–266;
- erection: circa giorni 237–306;
- cavidotti: giorni 71–300;
- SSE civili/elettriche/meccaniche: giorni 301–450;
- commissioning SSE: giorni 450–489;
- smobilizzo: giorni 489–503.

Regola radar: non convertire questi giorni in date finché non esiste un anchor verificato di avvio cantiere.

### Fonte regionale già trovata

Regione Puglia / BURP — Determinazione VIA/VIncA n. 340 del 01/08/2025, ID VIP 13821:
https://burp.regione.puglia.it/rss-burp/-/asset_publisher/6xyRm0hhUqeb/document/id/2697760

Il fascicolo MASE contiene inoltre una Relazione Tecnica Estimativa integrata nel maggio 2026, utile per particelle, espropri e indennità.

### Coda enti

- Regione Puglia: Trasparenza, Albo Telematico/pubblicità legale, BURP;
- Città Metropolitana di Bari;
- Comuni di Toritto, Bitonto, Binetto e Ruvo di Puglia;
- Terna/opere RTN.

## Primo pass: Fenice

### Documentazione acquisita

REL101 e REL102 integrate ad aprile 2026 confermano NVA Fenice, ATS Engineering come soggetto tecnico/progettista, 51 WTG e 367,2 MW. Descrivono strade, piazzole, fondazioni WTG, cavidotti 36 kV, stazione di elevazione nel Comune di Torremaggiore e connessione alla RTN Foggia–San Severo in località Palmori.

Le relazioni non nominano un contractor esecutivo e non contengono un cronoprogramma di costruzione. ATS Engineering può quindi essere registrata A1 come engineering/design, ma non chiude alcuno scope esecutivo.

### Fonte regionale già trovata

Regione Puglia / BURP — Determinazione VIA/VIncA n. 870 del 27/12/2024, ID VIP 11260:
https://burp.regione.puglia.it/rss-burp/-/asset_publisher/6xyRm0hhUqeb/document/id/2617190

### Coda enti

- Regione Puglia: Trasparenza, pubblicità legale, BURP;
- Provincia di Foggia: soprattutto viabilità/attraversamenti;
- Comuni di San Severo, Lucera, Pietramontecorvino, Torremaggiore, Castelnuovo della Daunia e Foggia;
- priorità specifica Torremaggiore per stazione di elevazione e Palmori/Foggia-San Severo per connessione.

## Primo pass: Lama Cupa / SE Casamassima

### Chiarimento importante sul 74402A

Il file `74402A` è presente nelle integrazioni MASE di Lama Cupa, ma il frontespizio identifica **FLYNIS PV 34 Srl** come committente e **Brulli Trasmissione** come engineering & construction della **SE 380/150/36 kV Casamassima**. Il cronoprogramma contiene approvvigionamenti, “RdO e subappalto opere civili”, OOCC, montaggi elettromeccanici, collaudi e MIS.

Regola radar: **Brulli non va attribuita come contractor di Lama Cupa**. Il documento è evidence A1 di un'infrastruttura di connessione condivisa/relata al dossier, non di uno scope direttamente affidato da Acciona per il parco eolico.

Il dato interessante da seguire è invece la filiera della SE Casamassima: voltura a Terna, aggiudicazione/subappalto OOCC, montaggi e messa in servizio.

### Fonte regionale già trovata

Regione Puglia / BURP — Determinazione VIA/VIncA n. 820 del 04/12/2024, ID VIP 12978:
https://burp.regione.puglia.it/en/rss-burp/-/asset_publisher/6xyRm0hhUqeb/document/id/2614177

MASE — integrazioni Lama Cupa, dove compare anche 74402A:
https://va.mite.gov.it/it-IT/Oggetti/Documentazione/11203/16822?pagina=10

### Coda enti

- Regione Puglia: Trasparenza, pubblicità legale, BURP;
- Comuni di Acquaviva delle Fonti, Gioia del Colle, Sammichele di Bari e Casamassima;
- particolare attenzione a Casamassima per SE, acque meteoriche, viabilità e atti sulle opere RTN;
- Terna per voltura/ampliamento/messa in servizio della SE.

## Regione Puglia: tre superfici da interrogare in parallelo

Gli atti regionali indicano espressamente una tripla pubblicazione:

1. `https://trasparenza.regione.puglia.it/` — Provvedimenti dirigenti amministrativi;
2. `https://www.regione.puglia.it/pubblicita-legale` — Albo pretorio on-line;
3. `https://burp.regione.puglia.it/` — BURP.

Per il radar le tre superfici vanno trattate come fonti complementari, perché hanno finestre di pubblicazione e indicizzazione differenti.

## Estensione agli altri 13 progetti del seed

Il metodo va ora applicato ai restanti progetti già monitorati, con priorità decrescente:

### Priorità 1 — costruzione / finestra commerciale attiva

- Andretta-Bisaccia;
- Serra Giannina;
- Serra Palino;
- Venusia;
- Greci-Montaguto;
- Carlentini;
- Tricarico;
- Tarsia Ovest;
- Castelfranco in Miscano / CER.

Query/atti target: `inizio lavori`, `ordinanza`, `trasporto eccezionale`, `occupazione`, `esproprio`, `attraversamento`, `manomissione suolo pubblico`, `cavidotto`, `sottostazione`, `strada provinciale`, `piazzola`, `fondazione`, `impresa`, `appaltatore`, `subappalto`, SPV e nome progetto.

### Priorità 2 — procurement/permitting

- Alia-Sclafani per gli scope residui;
- Nulvi-Ploaghe;
- Fenice;
- Sava-Maruggio;
- Toritto;
- Volturino;
- Lama Cupa.

Qui il valore principale è anticipare procurement e assegnazioni prima dell'apertura cantiere.

## Regole di attribuzione

- Un atto locale può confermare **stato/timing** anche senza nominare l'impresa.
- Un'impresa entra come contractor A1/A2 solo se l'atto o una fonte diretta associa chiaramente **azienda + progetto + scope**.
- Un'autorizzazione a un trasporto può confermare logistics solo se identifica il soggetto incaricato, non il semplice proprietario/committente.
- Un progettista, DL, CSE o advisor resta separato dagli scope esecutivi.
- Un'opera di connessione condivisa non trasferisce automaticamente il proprio contractor a tutti i progetti che la utilizzano.

## Prossimo pass

1. interrogazione sistematica degli enti locali/regionali per i progetti Priority 1;
2. normalizzazione delle fonti pubbliche nel dataset per progetto;
3. uso degli atti per cercare nomi di imprese, date reali e viabilità/trasporti;
4. mantenimento del seed a 17 progetti fino a completamento di questa fase.
