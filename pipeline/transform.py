import pandas as pd
import numpy as np
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import List

log = logging.getLogger(__name__)

@dataclass
class TransformStep:
    column:      str
    operation:   str
    before:      str
    after:       str
    rows_affected: int

@dataclass
class TransformResult:
    dataset: str
    df:      pd.DataFrame
    steps:   List[TransformStep] = field(default_factory=list)
    transformed_at: datetime = field(default_factory=datetime.now)

    def log_step(self, column, operation, before, after, rows_affected=0):
        self.steps.append(TransformStep(
            column=column,
            operation=operation,
            before=str(before),
            after=str(after),
            rows_affected=rows_affected
        ))


def transform_sales(df: pd.DataFrame) -> TransformResult:
    log.info("Transforming sales dataset")
    result = TransformResult(dataset="sales", df=df.copy())
    df = result.df

    # 1 — remove duplicates
    before = len(df)
    df.drop_duplicates(subset=["order_id"], keep="first", inplace=True)
    removed = before - len(df)
    result.log_step("order_id", "drop_duplicates",
                    f"{before} rows", f"{len(df)} rows", removed)
    log.info(f"Removed {removed} duplicate order IDs")

    # 2 — fill null customer names
    nulls = df["customer_name"].isna().sum()
    df["customer_name"] = df["customer_name"].fillna("Unknown Customer")
    result.log_step("customer_name", "fillna",
                    "null", "Unknown Customer", nulls)

    # 3 — fix negative prices — set to median
    median_price = df.loc[df["unit_price"] > 0, "unit_price"].median()
    neg_mask = df["unit_price"] <= 0
    neg_count = neg_mask.sum()
    df.loc[neg_mask, "unit_price"] = median_price
    result.log_step("unit_price", "fix_negative",
                    "<=0", f"median={round(median_price, 2)}", neg_count)

    # 4 — cap extreme prices at 99.5th percentile
    cap = df["unit_price"].quantile(0.995)
    spike_mask = df["unit_price"] > cap
    spike_count = spike_mask.sum()
    df.loc[spike_mask, "unit_price"] = cap
    result.log_step("unit_price", "cap_outliers",
                    f">{round(cap,2)}", f"capped at {round(cap,2)}", spike_count)

    # 5 — cap extreme quantities
    qty_cap = df["quantity"].quantile(0.995)
    qty_mask = df["quantity"] > qty_cap
    qty_count = qty_mask.sum()
    df.loc[qty_mask, "quantity"] = int(qty_cap)
    result.log_step("quantity", "cap_outliers",
                    f">{int(qty_cap)}", f"capped at {int(qty_cap)}", qty_count)

    # 6 — derive revenue column
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(2)
    result.log_step("revenue", "derive",
                    "not present", "quantity × unit_price", len(df))

    # 7 — derive discount flag (orders with quantity > 5 get discount)
    df["discount_applied"] = df["quantity"] > 5
    result.log_step("discount_applied", "derive",
                    "not present", "quantity > 5", len(df))

    # 8 — normalise status to lowercase
    df["status"] = df["status"].str.lower().str.strip()
    result.log_step("status", "normalise", "mixed case", "lowercase", len(df))

    # 9 — parse order_date to datetime
    df["order_date"] = pd.to_datetime(df["order_date"])
    result.log_step("order_date", "parse_datetime",
                    "string", "datetime", len(df))

    # 10 — add pipeline metadata
    df["_loaded_at"]  = datetime.now()
    df["_source"]     = "sales_csv"
    result.log_step("_loaded_at/_source", "add_metadata",
                    "not present", "pipeline metadata", len(df))

    result.df = df
    log.info(f"Sales transform done — {len(df)} rows, "
             f"{len(df.columns)} columns, {len(result.steps)} steps")
    return result


def transform_inventory(df: pd.DataFrame) -> TransformResult:
    log.info("Transforming inventory dataset")
    result = TransformResult(dataset="inventory", df=df.copy())
    df = result.df

    # 1 — add low stock flag
    df["low_stock"] = df["stock_quantity"] < df["reorder_level"]
    result.log_step("low_stock", "derive",
                    "not present", "stock < reorder_level", len(df))

    # 2 — add stock status label
    def stock_status(row):
        if row["stock_quantity"] == 0:
            return "OUT_OF_STOCK"
        elif row["stock_quantity"] < row["reorder_level"]:
            return "LOW"
        elif row["stock_quantity"] < row["reorder_level"] * 2:
            return "MEDIUM"
        return "HEALTHY"

    df["stock_status"] = df.apply(stock_status, axis=1)
    result.log_step("stock_status", "derive",
                    "not present", "OUT_OF_STOCK/LOW/MEDIUM/HEALTHY", len(df))

    # 3 — derive inventory value
    df["inventory_value"] = (
        df["stock_quantity"] * df["unit_cost"]
    ).round(2)
    result.log_step("inventory_value", "derive",
                    "not present", "stock_quantity × unit_cost", len(df))

    # 4 — parse last_updated
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    result.log_step("last_updated", "parse_datetime",
                    "string", "datetime", len(df))

    # 5 — metadata
    df["_loaded_at"] = datetime.now()
    df["_source"]    = "inventory_csv"
    result.log_step("_loaded_at/_source", "add_metadata",
                    "not present", "pipeline metadata", len(df))

    result.df = df
    log.info(f"Inventory transform done — {len(df)} rows")
    return result


def transform_customers(df: pd.DataFrame) -> TransformResult:
    log.info("Transforming customers dataset")
    result = TransformResult(dataset="customers", df=df.copy())
    df = result.df

    # 1 — fix invalid ages (>120 set to median)
    median_age = df.loc[df["age"].between(0, 120), "age"].median()
    invalid_mask = ~df["age"].between(0, 120)
    invalid_count = invalid_mask.sum()
    df.loc[invalid_mask, "age"] = int(median_age)
    result.log_step("age", "fix_invalid",
                    ">120 or <0", f"median={int(median_age)}", invalid_count)

    # 2 — derive age group
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 25, 35, 45, 55, 120],
        labels=["18-25","26-35","36-45","46-55","55+"]
    ).astype(str)
    result.log_step("age_group", "derive",
                    "not present", "binned age", len(df))

    # 3 — normalise city
    df["city"] = df["city"].str.strip().str.title()
    result.log_step("city", "normalise", "mixed", "title case", len(df))

    # 4 — parse join_date
    df["join_date"] = pd.to_datetime(df["join_date"])
    result.log_step("join_date", "parse_datetime",
                    "string", "datetime", len(df))

    # 5 — derive customer tenure in days
    df["tenure_days"] = (
        datetime.now() - df["join_date"]
    ).dt.days
    result.log_step("tenure_days", "derive",
                    "not present", "today - join_date", len(df))

    # 6 — metadata
    df["_loaded_at"] = datetime.now()
    df["_source"]    = "customers_csv"
    result.log_step("_loaded_at/_source", "add_metadata",
                    "not present", "pipeline metadata", len(df))

    result.df = df
    log.info(f"Customers transform done — {len(df)} rows")
    return result


def transform_all(extracted: dict) -> dict:
    log.info("=== TRANSFORM STAGE START ===")
    results = {
        "sales":     transform_sales(extracted["sales"].df),
        "inventory": transform_inventory(extracted["inventory"].df),
        "customers": transform_customers(extracted["customers"].df),
    }
    for name, r in results.items():
        log.info(f"{name}: {len(r.steps)} transformations applied")
    log.info("=== TRANSFORM STAGE DONE ===")
    return results


if __name__ == "__main__":
    from pipeline.extract import extract_all
    extracted  = extract_all()
    transformed = transform_all(extracted)

    for name, result in transformed.items():
        print(f"\n{name.upper()} — {len(result.df)} rows, "
              f"{len(result.df.columns)} columns")
        print("  Transformations applied:")
        for step in result.steps:
            print(f"    [{step.operation}] {step.column} "
                  f"— {step.rows_affected} rows affected")
        print(f"  New columns: {[c for c in result.df.columns if c.startswith('_') or c in ['revenue','discount_applied','low_stock','stock_status','inventory_value','age_group','tenure_days']]}")