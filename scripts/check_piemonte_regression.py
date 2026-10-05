from __future__ import annotations

from bs4 import BeautifulSoup

from app.collectors.piemonte import PiemonteCollector, START_URL
from app.pipeline import build_project_key


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

    external_ids = {
        collector._build_external_id(by_code[code])
        for code in EXPECTED_ACTIVE_CODES
    }
    if len(external_ids) != len(EXPECTED_ACTIVE_CODES):
        raise SystemExit(
            "Piemonte regression: external_id non distingue tutte le pratiche"
        )

    status_variant = dict(by_code["2025-144/VI"])
    status_variant["status"] = "CONCLUSA"
    if collector._build_external_id(status_variant) != collector._build_external_id(
        by_code["2025-144/VI"]
    ):
        raise SystemExit(
            "Piemonte regression: external_id cambia al variare dello stato"
        )

    if collector._extract_proponent(
        "IMPIANTO AGRIVOLTAICO POTENZA NOMINALE (DC) 3,45 MWp - "
        "POTENZA IN IMMISSIONE (AC) 2,575 MW"
    ) is not None:
        raise SystemExit(
            "Piemonte regression: una potenza è stata interpretata come proponente"
        )

    project_keys = {
        build_project_key(
            project_name=by_code[code]["title"],
            proponent=by_code[code]["proponent"],
            region="Piemonte",
            municipalities=by_code[code]["municipality"],
            power_mw=None,
            source_url=None,
            external_id=collector._build_external_id(by_code[code]),
        )
        for code in EXPECTED_ACTIVE_CODES
    }
    if len(project_keys) != len(EXPECTED_ACTIVE_CODES):
        raise SystemExit(
            "Piemonte regression: project_key esterna non distingue le pratiche"
        )

    legacy_url_key = build_project_key(
        project_name=None,
        proponent=None,
        region=None,
        municipalities=None,
        power_mw=None,
        source_url=START_URL,
        external_id=None,
    )
    if legacy_url_key in project_keys:
        raise SystemExit(
            "Piemonte regression: project_key corretto coincide col legacy URL key"
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
