import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def run_pipeline_job():
    """Full ETL pipeline job — called by scheduler."""
    log.info(f"Scheduled run started at {datetime.now()}")
    try:
        from pipeline.extract   import extract_all
        from pipeline.validate  import validate_all
        from pipeline.transform import transform_all
        from pipeline.load      import load_all
        from pipeline.anomaly   import analyse_all
        from pipeline.report    import generate_report
        import time

        start       = time.time()
        extracted   = extract_all()
        validations = validate_all(extracted)
        transforms  = transform_all(extracted)
        loads       = load_all(transforms)
        anomalies   = analyse_all()
        duration    = time.time() - start

        report_path = generate_report(
            validations, anomalies, transforms, loads, duration
        )
        log.info(f"Scheduled run complete — {round(duration,2)}s — {report_path}")
    except Exception as e:
        log.error(f"Scheduled run failed: {e}")


def start_scheduler(mode="demo"):
    """
    mode='demo'  — runs every 2 minutes so you can see it working
    mode='daily' — runs every day at 6:00 AM
    mode='hourly'— runs every hour
    """
    scheduler = BlockingScheduler()

    if mode == "demo":
        scheduler.add_job(
            run_pipeline_job,
            trigger="interval",
            minutes=2,
            id="etl_demo",
            name="ETL Pipeline (demo — every 2 min)",
            next_run_time=datetime.now()   # run immediately on start
        )
        log.info("Scheduler started — running every 2 minutes")

    elif mode == "daily":
        scheduler.add_job(
            run_pipeline_job,
            trigger=CronTrigger(hour=6, minute=0),
            id="etl_daily",
            name="ETL Pipeline (daily 6am)"
        )
        log.info("Scheduler started — running daily at 6:00 AM")

    elif mode == "hourly":
        scheduler.add_job(
            run_pipeline_job,
            trigger=CronTrigger(minute=0),
            id="etl_hourly",
            name="ETL Pipeline (hourly)"
        )
        log.info("Scheduler started — running every hour")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user")
        scheduler.shutdown()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"
    log.info(f"Starting scheduler in '{mode}' mode")
    start_scheduler(mode)