import pandas as pd
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

class ExtractionResult:
    def __init__(self, name, df, source_path, row_count, extracted_at):
        self.name         = name
        self.df           = df
        self.source_path  = source_path
        self.row_count    = row_count
        self.extracted_at = extracted_at

    def __repr__(self):
        return f"ExtractionResult({self.name}, rows={self.row_count})"


def extract_sales(path="data/raw/sales.csv") -> ExtractionResult:
    log.info(f"Extracting sales from {path}")
    df = pd.read_csv(path)
    log.info(f"Sales extracted — {len(df)} rows")
    return ExtractionResult(
        name="sales",
        df=df,
        source_path=path,
        row_count=len(df),
        extracted_at=datetime.now()
    )


def extract_inventory(path="data/raw/inventory.csv") -> ExtractionResult:
    log.info(f"Extracting inventory from {path}")
    df = pd.read_csv(path)
    log.info(f"Inventory extracted — {len(df)} rows")
    return ExtractionResult(
        name="inventory",
        df=df,
        source_path=path,
        row_count=len(df),
        extracted_at=datetime.now()
    )


def extract_customers(path="data/raw/customers.csv") -> ExtractionResult:
    log.info(f"Extracting customers from {path}")
    df = pd.read_csv(path)
    log.info(f"Customers extracted — {len(df)} rows")
    return ExtractionResult(
        name="customers",
        df=df,
        source_path=path,
        row_count=len(df),
        extracted_at=datetime.now()
    )


def extract_all() -> dict:
    log.info("=== EXTRACT STAGE START ===")
    results = {
        "sales":     extract_sales(),
        "inventory": extract_inventory(),
        "customers": extract_customers(),
    }
    total = sum(r.row_count for r in results.values())
    log.info(f"=== EXTRACT STAGE DONE — {total} total rows ===")
    return results


if __name__ == "__main__":
    results = extract_all()
    for name, result in results.items():
        print(f"{name}: {result.row_count} rows from {result.source_path}")