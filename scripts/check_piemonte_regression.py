from __future__ import annotations

from bs4 import BeautifulSoup

from app.collectors.piemonte import PiemonteCollector


EXPECTED_ACTIVE_CODES = {
    "2024-20/VI",
    "2025-140/VI",
    "2025-144/VI",
    "2026-118/VI",
    "2025-87/VI",
}


def main() -> int:
    collector = object.__new__(PiemonteCollector)

    html = """
    <table id="row_tElencoProgetti">
      <tr>
        <th></th>
        <th>Autorità competente</th>
        <th>Codice pratica</th>
        <th>Denominazione</th>
        <th>Localizzazione</th>
        <th>Scadenza Osservazioni</th>
        <th>Stato</th>
      </tr>
      <tr><td></td><td>SOGGETTO GESTORE RN2000</td><td>2024-20/VI</td><td>Progetto di impianto fotovoltaico a terra</td><td>PECETTO DI VALENZA, VALENZA</td><td></td><td>IN CORSO</td></tr>
      <tr><td></td><td>SOGGETTO GESTORE RN2000</td><td>2025-140/VI</td><td>PROGETTO DEFINITIVO PER LA REALIZZAZIONE DI UN IMPIANTO FOTOVOLTAICO A TERRA</td><td>CANDELO</td><td></td><td>IN CORSO</td></tr>
      <tr><td></td><td>SOGGETTO GESTORE RN2000</td><td>2025-144/VI</td><td>Realizzazione impianto fotovoltaico a terra di potenza installata 4,7170 MWp denominato "Trino Nord" e potenza di immissione pari a 4,3 MW</td><td>TRINO</td><td></td><td>IN CORSO</td></tr>
      <tr><td></td><td>SOGGETTO GESTORE RN2000</td><td>2026-118/VI</td><td>realizzazione impianto fotovoltaico a terra</td><td>CASALE MONFERRATO</td><td></td><td>IN CORSO</td></tr>
      <tr><td></td><td>SOGGETTO GESTORE RN2000</td><td>2025-87/VI</td><td>IMPIANTO AGRIVOLTAICO POTENZA NOMINALE (DC) 3,45 MWp - POTENZA IN IMMISSIONE (AC) 2,575 MW</td><td>ISOLA SANT'ANTONIO</td><td></td><td>IN CORSO</td></tr>
      <tr><td></td><td>REGIONE PIEMONTE</td><td>2026-20/VI</td><td>Progetto di Impianto Fotovoltaico e opere di Urbanizzazione</td><td>SANTA VITTORIA D'ALBA</td><td></td><td>CONCLUSA</td></tr>
    </table>
    """

    soup = BeautifulSoup(html, "html.parser")
    rows = collector._extract_result_rows(soup, "http://example.test/search")
    by_code = {row["code"]: row for row in rows}

    missing = EXPECTED_ACTIVE_CODES - set(by_code)
    if missing:
        raise SystemExit(
            "Piemonte regression: record RN2000 non acquisiti: "
            + ", ".join(sorted(missing))
        )

    if by_code["2025-87/VI"]["power"] != "3,45 MWp":
        raise SystemExit(
            "Piemonte regression: parsing potenza agrivoltaico 2025-87/VI errato"
        )

    if collector._negative_outcome_marker("Procedimento concluso con esito negativo") is None:
        raise SystemExit("Piemonte regression: esito negativo non riconosciuto")

    if collector._negative_outcome_marker(
        "Non emergono effetti negativi significativi sul sito"
    ) is not None:
        raise SystemExit("Piemonte regression: falso positivo su testo non negativo")

    print(
        "[piemonte-regression] OK:",
        len(EXPECTED_ACTIVE_CODES),
        "record attivi di riferimento RN2000 acquisibili",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
