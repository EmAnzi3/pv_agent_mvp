# Wind Radar v0.4 — Discovery & Refresh Engine

Data: 2026-09-05
Branch: `feat/wind-radar-v0.4-discovery-refresh`
Baseline: v0.3 pubblicata su `master` al merge commit `5cb4f00c3d9206e3b08eb224dd8751928ac44022`.

## Obiettivo

Trasformare il Wind Radar da seed curato di 17 progetti a radar aggiornabile e riproducibile, capace di:

1. scoprire nuovi progetti;
2. riconciliare identità e varianti;
3. rilevare cambi di MW, WTG, developer/SPV, stage, date e supply chain;
4. aggiornare i record senza perdere storico e provenienza;
5. separare discovery, verifica e pubblicazione;
6. includere **sia onshore sia offshore** senza escludere l'offshore a priori.

## Onshore / offshore

Il Radar adotta un campo strutturale `site_type` con valori canonici:

- `onshore`
- `offshore`

I 17 progetti v0.3 sono onshore e vengono trattati come `onshore` anche nei record legacy privi del campo esplicito. Ogni nuovo record v0.4 dovrà invece dichiarare `site_type`.

La UI deve offrire un selettore dedicato **Onshore + offshore / Onshore / Offshore**. Il campo `type` continua a descrivere Greenfield, Repowering, Greenfield + BESS ecc. e non va usato per distinguere terra/mare.

Per l'offshore il modello dovrà poter estendere in seguito gli attributi specifici senza alterare il nucleo comune, ad esempio tecnologia `fixed-bottom` / `floating`, area marina, porto/logistica, landfall e connessione a terra. Questi campi non sono obbligatori nel primo incremento.

## Principio di discovery

Un progetto entra nel discovery layer quando esiste almeno un'identità verificabile minima (nome/alias + soggetto o procedura + localizzazione/area + fonte). Non entra automaticamente nel dataset canonico.

Pipeline proposta:

`discovered -> identity_checked -> evidence_enriched -> canonical_candidate -> accepted`

I casi con collisioni o configurazioni incompatibili vanno in `needs_reconciliation` e non sovrascrivono il record canonico.

## Fonti

### Canoniche / ad alta affidabilità
- MASE / portale VIA-VAS e documentazione collegata;
- Regioni, BUR/BURAS e procedimenti autorizzativi;
- Terna / codici e pratiche di connessione quando pubblicamente accessibili;
- atti provinciali/comunali e concessioni;
- developer/SPV;
- OEM;
- contractor ed EPC con dichiarazione nominale del progetto.

### Discovery / enrichment
- pv magazine Italia;
- Rinnovabili.it;
- QualEnergia;
- Energia Oltre;
- Staffetta Quotidiana / Quotidiano Energia;
- Recharge, Renewables Now, Windpower Monthly, WindEurope;
- GlobalData come lead source, mai canonica.

La stampa di settore può generare un lead o rafforzare timing/market intelligence, ma non chiude automaticamente uno scope A1/A2.

## Change detection

Per ogni pass di aggiornamento devono essere confrontati almeno:
- progetto / alias;
- `site_type`;
- MW wind e BESS separati;
- numero e taglia WTG;
- developer e SPV;
- comuni / area / mare interessato;
- IDs MASE / MYTERNA / regionali;
- stage E0-E8;
- milestone e finestre temporali;
- relazioni azienda-progetto;
- contractor gap;
- stato della fonte e data ultimo controllo.

Ogni variazione significativa genera un evento di changelog prima di modificare il canonico.

## Stage E0-E8

La scala v0.3 resta comune a onshore e offshore:
E0 Pre-sviluppo -> E1 Sviluppo -> E2 Iter autorizzativo -> E3 Iter autorizzativo avanzato -> E4 Autorizzato -> E5 FID / investimento impegnato -> E6 Procurement / affidamenti -> E7 Costruzione -> E8 In esercizio.

La soglia resta: **fase più avanzata direttamente osservata**.

## Scope

Gli attuali 7 core scope sono il profilo onshore. Non devono essere applicati meccanicamente all'offshore. In v0.4 verrà definito un profilo di applicabilità per `site_type`, mantenendo separati:
- scope comuni;
- scope onshore;
- scope offshore.

Fino a quel pass, nessun progetto offshore dovrà essere penalizzato nel KPI 8/108 usando scope che non gli sono applicabili.

## Primo incremento v0.4

1. rendere il frontend non dipendente dal vincolo esatto `17 progetti`;
2. aggiungere `site_type` con fallback legacy `onshore`;
3. aggiungere selettore Onshore/Offshore;
4. includere `site_type` in drawer ed export CSV;
5. introdurre un registro discovery separato dal dataset canonico;
6. progettare il primo sweep nazionale onshore + offshore e le identity guard.

## Vincoli

- nessuna stima di contractor;
- nessuna attribuzione per vicinanza geografica o semplice presenza societaria;
- nessuna sovrascrittura di una configurazione senza identity reconciliation;
- nessuna esclusione preventiva dei progetti offshore per probabilità o distanza dalla domanda commerciale attuale;
- BESS sempre separato dai MW eolici;
- merge/pubblicazione solo dopo review della v0.4.