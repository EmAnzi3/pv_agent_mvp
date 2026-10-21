# Secondo benchmark MASE — Tortona 2 (progetto 8788)

**Stato:** acquisizione completa, validazione semantica NON completata. Nessuna scrittura nel canonico. Review-only.

## Procedura e inventario

- Scheda: https://va.mite.gov.it/it-IT/Oggetti/Info/8788
- Impianto **Tortona 2**, fotovoltaico/agrivoltaico da **60 MW** con opere RTN presso **Tortona, Pozzolo Formigaro, Frugarolo (AL)**.
- Archivio procedura [19438](https://va.mite.gov.it/it-IT/Oggetti/Documentazione/8788/19438): **87/87** documenti.
- Archivio procedura [12932](https://va.mite.gov.it/it-IT/Oggetti/Documentazione/8788/12932): **124/124** documenti.
- Totale **211/211 allegati aperti**, **0 errori** in 8 lotti. **189 contenuti SHA distinti**; 22 riferimenti hanno contenuti duplicati.
- **4.356 pagine PDF**, **3.576 con testo digitale**, **780 prive di testo digitale** in 35 file. Le ultime richiedono OCR/valutazione grafica prima di dichiarare copertura semantica integrale.
- [Run dei lotti](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39221207051): 8/8 SUCCESS. Un workflow verde indica acquisizione funzionante, non validazione dei fatti.

## Evidenze candidate — NON promuovere automaticamente

| Categoria | Valore e livello di prova | Fonte |
|---|---|---|
| Potenza FV | 60 MW lato DC e 50 MW lato AC negli elaborati; distinguere da titolo sintetico 60 MW | [805858, p.1](https://va.mite.gov.it/File/Documento/805858) |
| Tecnologia | Elaborati identificano un progetto **agrivoltaico** | [805858, p.1](https://va.mite.gov.it/File/Documento/805858) |
| Proponente | **MARGISOLAR S.r.l.** riportata quale richiedente, da riconciliare fra versioni | [706412, p.4](https://va.mite.gov.it/File/Documento/706412) |
| P. IVA | **03920651209** associata a MARGISOLAR in frontespizi; altri cartigli presentano **03920631201**: conflitto da verificare prima di attribuzione definitiva | [805858, p.1](https://va.mite.gov.it/File/Documento/805858), [805857, p.1](https://va.mite.gov.it/File/Documento/805857) |
| PEC | **margisolar@legalmail.it** citata nei documenti | [805873, p.9](https://va.mite.gov.it/File/Documento/805873) |
| Società tecniche | **INTEGRA s.r.l.**, email **integra@integraingegneria.it** su frontespizi; presenza come società di ingegneria non equivale a EPC | [805857, p.1](https://va.mite.gov.it/File/Documento/805857) |
| Altri attori | **SHELTER S.r.l.** incaricata di redigere un piano di monitoraggio; non impresa esecutrice confermata | [1439019, p.7](https://va.mite.gov.it/File/Documento/1439019) |
| Georeferenziazione | Coordinate dei **vertici delle aree** in UTM WGS84 32N dichiarate negli allegati; richiedono verifica delle righe numeriche, sistema di riferimento e identità delle aree | [805873, pp.60–61,109](https://va.mite.gov.it/File/Documento/805873) |
| Località | Cascina Ponzana, Ponzanina, Baronina fra le località indicate. I tre Comuni sono nella scheda ufficiale. Non usare un solo punto indicativo come perimetro dell'impianto | [706408, p.1](https://va.mite.gov.it/File/Documento/706408) |
| Cronoprogramma | Elaborati riportano stime divergenti: 10–12 mesi (scenario con squadre parallele), 12 mesi per specifiche attività, 18 mesi complessivi in altra relazione, 24 mesi in ulteriore documentazione; rispettare versione e ambito | [1439042, p.15](https://va.mite.gov.it/File/Documento/1439042), [1161323, p.15](https://va.mite.gov.it/File/Documento/1161323), [1439043, p.39](https://va.mite.gov.it/File/Documento/1439043) |

**Attenzione a una data:** [1439043, p.133](https://va.mite.gov.it/File/Documento/1439043) contiene «Inizio lavori: 01/09/2026», ma non abbiamo ancora verificato se è data effettiva, previsione del Gantt o esempio nella documentazione di sicurezza. Non registrarla come avvio cantiere confermato.

## Gate ancora aperti

1. OCR e lettura grafica delle 780 pagine senza testo digitale, prioritizzate per tavole di ubicazione, cantieri e cronoprogrammi.
2. Riconciliare persone/società, recapiti/P.IVA e versioni fra procedure 19438/12932; tenere EPC/BoP non determinati senza evidenza di award.
3. Estrarre le coordinate dei vertici delle aree e validarne entità, formato e CRS prima di offrire geometrie GIS.
4. Verificare il cronoprogramma più recente e distinguere obblighi/documenti di sicurezza da attività effettivamente avviate.
5. Confrontare i campi documentati con il record già presente in PV Agent, classificando conferme, nuovi fatti, conflitti e non determinabile.
6. Test finale con dossier pronto per review; non confondere 8/8 SUCCESS di acquisizione con completezza informativa.

**Verdetto intermedio:** toolbox tecnicamente valida per acquisire anche due fascicoli MASE articolati; non ancora chiudibile il dossier semantico Tortona 2.
