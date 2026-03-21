import logging
import time
from datetime import datetime
from pipeline.extract   import extract_all
from pipeline.validate  import validate_all
from pipeline.transform import transform_all
from pipeline.load      import load_all
from pipeline.anomaly   import analyse_all
from pipeline.report    import generate_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

def run_pipeline():
    log.info("=" * 60)
    log.info("ETL PIPELINE RUN STARTED")
    log.info("=" * 60)
    start = time.time()

    extracted   = extract_all()
    validations = validate_all(extracted)
    transforms  = transform_all(extracted)
    loads       = load_all(transforms)
    anomalies   = analyse_all()

    duration = time.time() - start
    report_path = generate_report(
        validations, anomalies, transforms, loads, duration
    )

    log.info("=" * 60)
    log.info(f"PIPELINE COMPLETE in {round(duration, 2)}s")
    log.info(f"Report: {report_path}")
    log.info("=" * 60)
    return report_path

if __name__ == "__main__":
    run_pipeline()