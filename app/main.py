from __future__ import annotations

import argparse
import logging
import time

from apscheduler.schedulers.blocking import BlockingScheduler

from app.collectors.mase import MASECollector
from app.collectors.veneto import VenetoCollector
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
    MASECollector,
    VenetoCollector,
]


def init_db() -> None:
    Base.metadata.create_all(bind=engine)



def run_once() -> None:
    init_db()
    logger.info("Avvio run giornaliero")
    db = SessionLocal()
    try:
        pipeline = IngestionPipeline(db)
        for collector_cls in COLLECTORS:
            collector = collector_cls()
            logger.info("Esecuzione collector: %s", collector.source_name)
            results = collector.fetch()
            summary = pipeline.process_collector_results(collector.source_name, collector.base_url, results)
            logger.info("Summary %s", summary)

        reports = ReportBuilder(db).build_daily_reports()
        for report in reports:
            logger.info("Creato report: %s", report)
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

    # esecuzione iniziale breve per testare il container appena avviato
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
    parser.add_argument("command", choices=["run-once", "scheduler"], help="Comando da eseguire")
    args = parser.parse_args()

    start = time.time()
    if args.command == "run-once":
        run_once()
    elif args.command == "scheduler":
        run_scheduler()
    logger.info("Completato in %.2f secondi", time.time() - start)


if __name__ == "__main__":
    main()
