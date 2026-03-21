import pandas as pd
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import List

log = logging.getLogger(__name__)

@dataclass
class ValidationCheck:
    name:    str
    passed:  bool
    level:   str        # "critical" or "warning"
    message: str

@dataclass
class ValidationResult:
    dataset:  str
    checks:   List[ValidationCheck] = field(default_factory=list)
    passed_at: datetime = field(default_factory=datetime.now)

    @property
    def passed(self):
        return all(c.passed for c in self.checks if c.level == "critical")

    @property
    def critical_failures(self):
        return [c for c in self.checks if not c.passed and c.level == "critical"]

    @property
    def warnings(self):
        return [c for c in self.checks if not c.passed and c.level == "warning"]

    def summary(self):
        total    = len(self.checks)
        passed   = sum(1 for c in self.checks if c.passed)
        score    = round((passed / total) * 100, 1) if total else 0
        return {
            "dataset":  self.dataset,
            "total":    total,
            "passed":   passed,
            "failed":   total - passed,
            "score":    score,
            "status":   "PASS" if self.passed else "FAIL"
        }


def _check(result: ValidationResult, name: str, condition: bool,
           level: str, pass_msg: str, fail_msg: str):
    result.checks.append(ValidationCheck(
        name=name,
        passed=condition,
        level=level,
        message=pass_msg if condition else fail_msg
    ))


def validate_sales(df: pd.DataFrame) -> ValidationResult:
    log.info("Validating sales dataset")
    result = ValidationResult(dataset="sales")

    # critical checks
    _check(result, "required_columns",
           all(c in df.columns for c in
               ["order_id","product","quantity","unit_price","region","status"]),
           "critical",
           "All required columns present",
           "Missing required columns")

    _check(result, "no_null_order_id",
           df["order_id"].notna().all(),
           "critical",
           "No null order IDs",
           f"{df['order_id'].isna().sum()} null order IDs found")

    _check(result, "positive_quantity",
           (df["quantity"] > 0).all(),
           "critical",
           "All quantities are positive",
           f"{(df['quantity'] <= 0).sum()} non-positive quantities")

    _check(result, "positive_unit_price",
           (df["unit_price"] > 0).all(),
           "critical",
           "All unit prices are positive",
           f"{(df['unit_price'] <= 0).sum()} non-positive prices found")

    _check(result, "unique_order_ids",
           df["order_id"].nunique() == len(df),
           "critical",
           "All order IDs are unique",
           f"{len(df) - df['order_id'].nunique()} duplicate order IDs")

    # warning checks
    null_names = df["customer_name"].isna().sum()
    _check(result, "no_null_customer_name",
           null_names == 0,
           "warning",
           "No null customer names",
           f"{null_names} null customer names")

    price_99 = df["unit_price"].quantile(0.99)
    spikes   = (df["unit_price"] > price_99 * 3).sum()
    _check(result, "no_price_spikes",
           spikes == 0,
           "warning",
           "No extreme price spikes",
           f"{spikes} extreme price values detected")

    qty_99 = df["quantity"].quantile(0.99)
    outliers = (df["quantity"] > qty_99 * 3).sum()
    _check(result, "no_quantity_outliers",
           outliers == 0,
           "warning",
           "No quantity outliers",
           f"{outliers} extreme quantity values")

    log.info(f"Sales validation: {result.summary()}")
    return result


def validate_inventory(df: pd.DataFrame) -> ValidationResult:
    log.info("Validating inventory dataset")
    result = ValidationResult(dataset="inventory")

    _check(result, "required_columns",
           all(c in df.columns for c in
               ["product","stock_quantity","reorder_level","unit_cost"]),
           "critical",
           "All required columns present",
           "Missing required columns")

    _check(result, "non_negative_stock",
           (df["stock_quantity"] >= 0).all(),
           "critical",
           "All stock quantities are non-negative",
           f"{(df['stock_quantity'] < 0).sum()} negative stock values")

    _check(result, "positive_unit_cost",
           (df["unit_cost"] > 0).all(),
           "critical",
           "All unit costs are positive",
           f"{(df['unit_cost'] <= 0).sum()} non-positive costs")

    below_reorder = (df["stock_quantity"] < df["reorder_level"]).sum()
    _check(result, "stock_above_reorder",
           below_reorder == 0,
           "warning",
           "All products above reorder level",
           f"{below_reorder} products below reorder level")

    log.info(f"Inventory validation: {result.summary()}")
    return result


def validate_customers(df: pd.DataFrame) -> ValidationResult:
    log.info("Validating customers dataset")
    result = ValidationResult(dataset="customers")

    _check(result, "required_columns",
           all(c in df.columns for c in
               ["customer_id","name","email","city","age"]),
           "critical",
           "All required columns present",
           "Missing required columns")

    _check(result, "unique_customer_ids",
           df["customer_id"].nunique() == len(df),
           "critical",
           "All customer IDs are unique",
           f"{len(df) - df['customer_id'].nunique()} duplicate customer IDs")

    _check(result, "valid_age_range",
           df["age"].between(0, 120).all(),
           "critical",
           "All ages are in valid range",
           f"{(~df['age'].between(0, 120)).sum()} invalid ages detected")

    _check(result, "no_null_email",
           df["email"].notna().all(),
           "warning",
           "No null emails",
           f"{df['email'].isna().sum()} null emails")

    log.info(f"Customers validation: {result.summary()}")
    return result


def validate_all(extracted: dict) -> dict:
    log.info("=== VALIDATE STAGE START ===")
    results = {
        "sales":     validate_sales(extracted["sales"].df),
        "inventory": validate_inventory(extracted["inventory"].df),
        "customers": validate_customers(extracted["customers"].df),
    }

    for name, r in results.items():
        s = r.summary()
        log.info(f"{name}: score={s['score']}% status={s['status']}")
        if r.critical_failures:
            for f in r.critical_failures:
                log.error(f"CRITICAL — {f.name}: {f.message}")
        if r.warnings:
            for w in r.warnings:
                log.warning(f"WARNING  — {w.name}: {w.message}")

    log.info("=== VALIDATE STAGE DONE ===")
    return results


if __name__ == "__main__":
    from pipeline.extract import extract_all
    extracted  = extract_all()
    validations = validate_all(extracted)
    print()
    for name, r in validations.items():
        s = r.summary()
        print(f"{name}: {s['score']}% — {s['status']} "
              f"({s['passed']}/{s['total']} checks passed)")