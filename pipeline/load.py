import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import List
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os

load_dotenv()
log = logging.getLogger(__name__)

def get_engine():
    password = quote_plus(os.getenv("DB_PASSWORD"))
    url = f"postgresql://postgres:{password}@localhost:5432/etl_pipeline"
    return create_engine(url)

@dataclass
class LoadResult:
    dataset:     str
    rows_loaded: int
    rows_updated: int
    loaded_at:   datetime = field(default_factory=datetime.now)
    status:      str = "success"
    error:       str = ""


def create_tables(engine):
    """Create destination tables if they don't exist."""
    log.info("Creating tables if not exist")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sales (
                order_id         TEXT PRIMARY KEY,
                customer_name    TEXT,
                product          TEXT,
                quantity         INTEGER,
                unit_price       NUMERIC(12,2),
                region           TEXT,
                status           TEXT,
                order_date       DATE,
                sales_rep        TEXT,
                revenue          NUMERIC(12,2),
                discount_applied BOOLEAN,
                _loaded_at       TIMESTAMP,
                _source          TEXT
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory (
                product           TEXT PRIMARY KEY,
                stock_quantity    INTEGER,
                reorder_level     INTEGER,
                warehouse         TEXT,
                last_updated      DATE,
                unit_cost         NUMERIC(12,2),
                low_stock         BOOLEAN,
                stock_status      TEXT,
                inventory_value   NUMERIC(12,2),
                _loaded_at        TIMESTAMP,
                _source           TEXT
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id   TEXT PRIMARY KEY,
                name          TEXT,
                email         TEXT,
                city          TEXT,
                age           INTEGER,
                customer_tier TEXT,
                join_date     DATE,
                total_spent   NUMERIC(12,2),
                age_group     TEXT,
                tenure_days   INTEGER,
                _loaded_at    TIMESTAMP,
                _source       TEXT
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id           SERIAL PRIMARY KEY,
                run_at       TIMESTAMP DEFAULT NOW(),
                status       TEXT,
                sales_rows   INTEGER,
                inventory_rows INTEGER,
                customers_rows INTEGER,
                duration_sec NUMERIC(8,2),
                error        TEXT
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS data_lineage (
                id           SERIAL PRIMARY KEY,
                run_at       TIMESTAMP DEFAULT NOW(),
                dataset      TEXT,
                column_name  TEXT,
                operation    TEXT,
                before_value TEXT,
                after_value  TEXT,
                rows_affected INTEGER
            )
        """))

        conn.commit()
    log.info("Tables ready")


def upsert_sales(df: pd.DataFrame, engine) -> LoadResult:
    log.info(f"Loading {len(df)} sales rows into PostgreSQL")
    try:
        # write to staging first
        df.to_sql("sales_staging", engine,
                  if_exists="replace", index=False)

        with engine.connect() as conn:
            # upsert from staging to main
            conn.execute(text("""
                INSERT INTO sales
                SELECT * FROM sales_staging
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_name    = EXCLUDED.customer_name,
                    product          = EXCLUDED.product,
                    quantity         = EXCLUDED.quantity,
                    unit_price       = EXCLUDED.unit_price,
                    region           = EXCLUDED.region,
                    status           = EXCLUDED.status,
                    order_date       = EXCLUDED.order_date,
                    sales_rep        = EXCLUDED.sales_rep,
                    revenue          = EXCLUDED.revenue,
                    discount_applied = EXCLUDED.discount_applied,
                    _loaded_at       = EXCLUDED._loaded_at,
                    _source          = EXCLUDED._source
            """))
            conn.execute(text("DROP TABLE IF EXISTS sales_staging"))
            conn.commit()

        log.info(f"Sales upsert complete — {len(df)} rows")
        return LoadResult(dataset="sales", rows_loaded=len(df), rows_updated=0)

    except Exception as e:
        log.error(f"Sales load failed: {e}")
        return LoadResult(dataset="sales", rows_loaded=0,
                         rows_updated=0, status="failed", error=str(e))


def upsert_inventory(df: pd.DataFrame, engine) -> LoadResult:
    log.info(f"Loading {len(df)} inventory rows")
    try:
        df.to_sql("inventory_staging", engine,
                  if_exists="replace", index=False)

        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventory
                SELECT * FROM inventory_staging
                ON CONFLICT (product) DO UPDATE SET
                    stock_quantity  = EXCLUDED.stock_quantity,
                    reorder_level   = EXCLUDED.reorder_level,
                    warehouse       = EXCLUDED.warehouse,
                    last_updated    = EXCLUDED.last_updated,
                    unit_cost       = EXCLUDED.unit_cost,
                    low_stock       = EXCLUDED.low_stock,
                    stock_status    = EXCLUDED.stock_status,
                    inventory_value = EXCLUDED.inventory_value,
                    _loaded_at      = EXCLUDED._loaded_at,
                    _source         = EXCLUDED._source
            """))
            conn.execute(text("DROP TABLE IF EXISTS inventory_staging"))
            conn.commit()

        log.info(f"Inventory upsert complete — {len(df)} rows")
        return LoadResult(dataset="inventory", rows_loaded=len(df), rows_updated=0)

    except Exception as e:
        log.error(f"Inventory load failed: {e}")
        return LoadResult(dataset="inventory", rows_loaded=0,
                         rows_updated=0, status="failed", error=str(e))


def upsert_customers(df: pd.DataFrame, engine) -> LoadResult:
    log.info(f"Loading {len(df)} customer rows")
    try:
        df.to_sql("customers_staging", engine,
                  if_exists="replace", index=False)

        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO customers
                SELECT * FROM customers_staging
                ON CONFLICT (customer_id) DO UPDATE SET
                    name          = EXCLUDED.name,
                    email         = EXCLUDED.email,
                    city          = EXCLUDED.city,
                    age           = EXCLUDED.age,
                    customer_tier = EXCLUDED.customer_tier,
                    join_date     = EXCLUDED.join_date,
                    total_spent   = EXCLUDED.total_spent,
                    age_group     = EXCLUDED.age_group,
                    tenure_days   = EXCLUDED.tenure_days,
                    _loaded_at    = EXCLUDED._loaded_at,
                    _source       = EXCLUDED._source
            """))
            conn.execute(text("DROP TABLE IF EXISTS customers_staging"))
            conn.commit()

        log.info(f"Customers upsert complete — {len(df)} rows")
        return LoadResult(dataset="customers", rows_loaded=len(df), rows_updated=0)

    except Exception as e:
        log.error(f"Customers load failed: {e}")
        return LoadResult(dataset="customers", rows_loaded=0,
                         rows_updated=0, status="failed", error=str(e))


def save_lineage(transform_results: dict, engine):
    """Save data lineage steps to PostgreSQL."""
    rows = []
    for dataset, result in transform_results.items():
        for step in result.steps:
            rows.append({
                "run_at":        datetime.now(),
                "dataset":       dataset,
                "column_name":   step.column,
                "operation":     step.operation,
                "before_value":  step.before,
                "after_value":   step.after,
                "rows_affected": step.rows_affected
            })
    if rows:
        pd.DataFrame(rows).to_sql(
            "data_lineage", engine,
            if_exists="append", index=False
        )
        log.info(f"Saved {len(rows)} lineage records")


def load_all(transform_results: dict) -> dict:
    log.info("=== LOAD STAGE START ===")
    engine = get_engine()
    create_tables(engine)

    results = {
        "sales":     upsert_sales(transform_results["sales"].df, engine),
        "inventory": upsert_inventory(transform_results["inventory"].df, engine),
        "customers": upsert_customers(transform_results["customers"].df, engine),
    }

    save_lineage(transform_results, engine)

    for name, r in results.items():
        log.info(f"{name}: {r.rows_loaded} rows — {r.status}")

    log.info("=== LOAD STAGE DONE ===")
    return results


if __name__ == "__main__":
    from pipeline.extract import extract_all
    from pipeline.transform import transform_all

    extracted   = extract_all()
    transformed = transform_all(extracted)
    loaded      = load_all(transformed)

    print()
    for name, r in loaded.items():
        print(f"{name}: {r.rows_loaded} rows loaded — {r.status}")