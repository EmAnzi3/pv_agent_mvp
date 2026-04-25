from __future__ import annotations

import argparse
import logging
import shutil
import time
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

from app.collectors.veneto import VenetoCollector
from app.collectors.emilia_romagna import EmiliaRomagnaCollector
from app.collectors.lombardia import LombardiaCollector
from app.collectors.sicilia import SiciliaCollector
from app.collectors.puglia import PugliaCollector
from app.collectors.lazio import LazioCollector
from app.collectors.sardegna import SardegnaCollector
from app.collectors.toscana import ToscanaCollector
from app.collectors.piemonte import PiemonteCollector
from app.collectors.campania import CampaniaCollector
from app.config import settings
from app.db import SessionLocal, engine
from app.models import Base
from app.pipeline import IngestionPipeline
from app.reporting import ReportBuilder

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


COLLECTORS = [
    VenetoCollector,
    EmiliaRomagnaCollector,
    LombardiaCollector,
    SiciliaCollector,
    PugliaCollector,
    LazioCollector,
    SardegnaCollector,
    ToscanaCollector,
    PiemonteCollector,
    CampaniaCollector,
]


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def clean_reports_dir() -> None:
    """
    Svuota la cartella reports prima di ogni esecuzione.

    Serve a evitare l'accumulo progressivo di file debug/report generati dai collector.
    La cartella viene mantenuta, ma il contenuto viene rimosso.
    """
    reports_dir = Path(settings.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    for item in reports_dir.iterdir():
        if item.name == ".gitkeep":
            continue

        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning("Impossibile eliminare %s: %s", item, exc)


def clean_csv_reports_only() -> None:
    """
    Elimina eventuali CSV già generati nella run corrente.

    Utile se il ReportBuilder viene richiamato più volte nello stesso ciclo
    o se un'esecuzione ravvicinata lascia più terne di report.
    """
    reports_dir = Path(settings.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    patterns = [
        "new_projects_*.csv",
        "projects_snapshot_*.csv",
        "status_changes_*.csv",
    ]

    for pattern in patterns:
        for file_path in reports_dir.glob(pattern):
            try:
                file_path.unlink(missing_ok=True)
            except Exception as exc:
                logger.warning("Impossibile eliminare report CSV %s: %s", file_path, exc)


def clean_debug_dirs_only() -> None:
    """
    Elimina le cartelle debug generate dai collector a fine esecuzione.

    I debug restano disponibili durante la run, ma non vengono mantenuti
    nella cartella reports finale. In questo modo l'output resta leggero:
    solo i CSV finali.
    """
    reports_dir = Path(settings.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    for item in reports_dir.glob("debug_*"):
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
        except Exception as exc:
            logger.warning("Impossibile eliminare cartella debug %s: %s", item, exc)


def run_once() -> None:
    clean_reports_dir()
    init_db()

    logger.info("Avvio run giornaliero")

    db = SessionLocal()
    try:
        pipeline = IngestionPipeline(db)

        for collector_cls in COLLECTORS:
            collector = collector_cls()
            logger.info("Esecuzione collector: %s", collector.source_name)

            results = collector.fetch()
            summary = pipeline.process_collector_results(
                collector.source_name,
                collector.base_url,
                results,
            )

            logger.info("Summary %s", summary)

        # Mantiene una sola terna CSV finale anche se il builder viene richiamato più volte.
        clean_csv_reports_only()

        reports = ReportBuilder(db).build_daily_reports()
        for report in reports:
            logger.info("Creato report: %s", report)

        # Rimuove i debug a fine run: output finale leggero, solo CSV.
        clean_debug_dirs_only()

    finally:
        db.close()


def run_scheduler() -> None:
    if not settings.enable_scheduler:
        logger.warning("Scheduler disabilitato da configurazione")
        return

    scheduler = BlockingScheduler(timezone="Europe/Rome")
    scheduler.add_job(
        run_once,
        trigger="cron",
        hour=settings.daily_run_hour,
        minute=settings.daily_run_minute,
        id="daily_run",
        replace_existing=True,
    )

    logger.info(
        "Scheduler avviato. Run giornaliero alle %02d:%02d Europe/Rome",
        settings.daily_run_hour,
        settings.daily_run_minute,
    )

    # Esecuzione iniziale breve per testare il container appena avviato.
    try:
        run_once()
    except Exception as exc:
        logger.exception("Errore nel run iniziale: %s", exc)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler arrestato")


def main() -> None:
    parser = argparse.ArgumentParser(description="PV Agent MVP")
    parser.add_argument(
        "command",
        choices=["run-once", "scheduler"],
        help="Comando da eseguire",
    )
    args = parser.parse_args()

    start = time.time()

    if args.command == "run-once":
        run_once()
    elif args.command == "scheduler":
        run_scheduler()

    logger.info("Completato in %.2f secondi", time.time() - start)


if __name__ == "__main__":
    main()