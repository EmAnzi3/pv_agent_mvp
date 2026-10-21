# Troia Sun / MASE 6479 — semantic closeout review (bounded pilot)

**Ambito:** progetto 11109, procedimento VIP 12863, fonte 6479. **Esito:** acquisizione completata, dossier di evidenze controllate predisposto; non equivale alla capacità generalizzata già validata su altri portali. Non promuovere automaticamente dati nel canonico.

## 1. Copertura verificabile
- Documentazione MASE dichiarata: **112**, link distinti enumerati: **112**, 12 pagine di indice.
- **112 documenti + ZIP del provvedimento = 113 elementi** acquisiti/aperti. Primo parser 101, recupero PyMuPDF di 12, **0 errori residui di apertura**.
- PDF digitali, ZIP e scansioni: OCR mirato su **131 pagine** di due documenti, senza errori del motore. Non significa comprensione automatica delle tavole o precisione OCR perfetta.
- Provenienza: SHA dei download, URL, pagine/estratti e stato di verifica. Run acquisizione #39213237747, recovery #39213677698, OCR #39214236980.
- Baseline confrontata con dataset pubblicato: 21 corrispondenze campo/fonte/pagina, 4 valori già presenti letteralmente nel record e 17 candidati incrementali (run #39217556506). Questo conteggio è *case-based*: non misura precision/recall universale.

## 2. Risultati semantici utilizzabili
**Proponente:** Troia Sun S.r.l., P. IVA **10148261216** (checksum italiano valido), PEC **troiasunsrl@legalmail.it**. Evidenza [1126265 p.1](https://va.mite.gov.it/File/Documento/1126265).

**Progettazione:** IPROJECT S.r.l., PEC **I-project@legalmail.it**, P. IVA **11092870960** (checksum valido su frontespizi, [1126228 p.1](https://va.mite.gov.it/File/Documento/1126228) e [1126229 p.1](https://va.mite.gov.it/File/Documento/1126229)). **Anomalia:** numerosi cartigli in pagine successive riportano **1109287960** (10 cifre, non P. IVA italiana valida) e talvolta indirizzi diversi. Questo è un errore/incoerenza documentale, non una seconda entità. Da confermare con anagrafe ufficiale prima di diffusione commerciale come VAT certa. Arch. Antonio Manco (progettista), geol. Fabio Di Feo (tecnico); email e recapiti nei documenti [1126229](https://va.mite.gov.it/File/Documento/1126229) e [1126165](https://va.mite.gov.it/File/Documento/1126165). Recapiti professionali documentali, aggiornamento corrente non attestato.

**Tecnologia:** agrivoltaico **54,5 MWp** (configurazione VIA 2026, non sovrascrivere con 54,9 MWp presente in versioni precedenti); accumulo **10 MW** mantenuto separato. Tracker monoassiali, moduli da 710 Wp, indicazione di 168 inverter da 350 kW, opere di connessione con riferimenti a 36 kV. Numeri tecnici relativi a documenti progettuali: evitare di spacciarli per installazioni/forniture definitive.

**Localizzazione:** Comune Troia (FG), località Vaccareccia. Il documento [1126263, p.4](https://va.mite.gov.it/File/Documento/1126263) dichiara coordinate **UTM WGS84 fuso 33** per le **cabine di trasformazione**; esempio CT1 **531826,30 E / 4582292,20 N**. Questa è una coordinata di cabina, **NON il centroide o il perimetro del parco**. La coppia lat/lon **41.394, 15.389** nel documento [1126166 p.15–16](https://va.mite.gov.it/File/Documento/1126166) è un *input PVGIS di stima della resa*: non è prova dell'esatto posizionamento dell'impianto. Il valore corretto per localizzazione generale resta *Comune + località*, fino a validazione di GIS georeferenziato e relativa entità spaziale.

**Timeline, conflitto contestualizzato:**
- [1126259 p.173](https://va.mite.gov.it/File/Documento/1126259): **16 mesi di attività di cantiere**, **entrata commerciale prevista dopo 12 mesi dall'apertura**, subordinata a commissioning e test (~2 mesi): dichiarazioni progettuali, non eventi avvenuti.
- [1126259 p.188](https://va.mite.gov.it/File/Documento/1126259) e [1126229 p.3](https://va.mite.gov.it/File/Documento/1126229): **circa 24 mesi per il cronoprogramma di realizzazione complessiva**, a decorrere dalle autorizzazioni necessarie ed escluso approvvigionamento materiali. La durata di 24 mesi non sostituisce automaticamente le durate di singole fasi.
- Dismissione/ripristino: **~16 mesi**, fase diversa dalla costruzione ([1126259 pagg.197–198](https://va.mite.gov.it/File/Documento/1126259)).
- **Nessuna data calendariale verificata per apertura cantiere, fine lavori o COD; non generarne una da una VIA positiva**.

**Esecuzione:** EPC, BoP civile/elettrico, installatore, fornitore WTG/PV e affidamenti *non assegnati* sulla base delle evidenze validate. La presenza della società di progettazione non prova un award.

## 3. Risultato dei due gate richiesti

- **Validazione semantica mirata: PASS con qualificazioni.** Sono stati controllati riferimenti agli originali e chiariti i falsi collegamenti più significativi (PVGIS vs coordinate reali; design vs EPC; fasi di cronoprogramma; P. IVA breve). Non asserire recall completo di ogni nome e cellulare presente nei 112 allegati.
- **Confronto canonico: PASS per i 21 campi testati**, con elenco di 17 candidati nuovi e 4 già presenti letteralmente. Il canonico non contiene ancora la struttura necessaria per contatti e ruoli.
- **Gate di generalizzazione: NON PROVATO.** Prima di dichiarare l'agente semanticamente autonomo servono gold set indipendente e test su altri progetti/portali, con misure di precisione/recall, controllo delle revisioni e validazione grafica/cartografica.

## 4. Chiusura del caso e prossimo campione

**Si può chiudere Troia Sun come benchmark di fattibilità e dossier umano verificabile con campi non determinabili espliciti.** Non si può certificare che ogni informazione utile sia stata trovata o che la toolbox sia pronta a importare campi senza review.

Verdetto Crawl4AI per il caso: **utile per scoperta, navigazione e repertorio documentale** se inserito nella pipeline Crawl4AI + Playwright + ZIP + pypdf/PyMuPDF + OCR + evidence gate. Prossimo test raccomandato: altra procedura MASE con disegni/coordinate ed enti/soggetti diversi, confrontata con una scheda di controllo indipendente.

**PR #4 resta Draft** per separare chiusura benchmark da merge/abilitazione in produzione.
