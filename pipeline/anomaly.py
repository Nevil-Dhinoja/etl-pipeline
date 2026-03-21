import pandas as pd
import numpy as np
import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import List
from sqlalchemy import create_engine, text
from langchain_groq import ChatGroq
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os

load_dotenv()
log = logging.getLogger(__name__)


@dataclass
class Anomaly:
    dataset:  str
    column:   str
    type:     str
    value:    float
    threshold: float
    row_index: int = -1
    severity: str = "warning"


@dataclass
class AnomalyResult:
    dataset:   str
    anomalies: List[Anomaly] = field(default_factory=list)
    narrative: str = ""
    checked_at: datetime = field(default_factory=datetime.now)

    @property
    def count(self):
        return len(self.anomalies)


def get_engine():
    password = quote_plus(os.getenv("DB_PASSWORD"))
    return create_engine(
        f"postgresql://postgres:{password}@localhost:5432/etl_pipeline"
    )


def detect_iqr_outliers(df: pd.DataFrame, column: str,
                         dataset: str) -> List[Anomaly]:
    anomalies = []
    if column not in df.columns:
        return anomalies

    series = pd.to_numeric(df[column], errors="coerce").dropna()
    Q1, Q3 = series.quantile(0.25), series.quantile(0.75)
    IQR    = Q3 - Q1
    lower  = Q1 - 1.5 * IQR
    upper  = Q3 + 1.5 * IQR

    outlier_mask = (series < lower) | (series > upper)
    for idx in series[outlier_mask].index:
        anomalies.append(Anomaly(
            dataset=dataset,
            column=column,
            type="iqr_outlier",
            value=float(series[idx]),
            threshold=float(upper) if series[idx] > upper else float(lower),
            row_index=int(idx),
            severity="warning"
        ))
    return anomalies


def detect_zscore_outliers(df: pd.DataFrame, column: str,
                            dataset: str, threshold=3.0) -> List[Anomaly]:
    anomalies = []
    if column not in df.columns:
        return anomalies

    series = pd.to_numeric(df[column], errors="coerce").dropna()
    if series.std() == 0:
        return anomalies

    zscores = np.abs((series - series.mean()) / series.std())
    for idx in series[zscores > threshold].index:
        anomalies.append(Anomaly(
            dataset=dataset,
            column=column,
            type="zscore_outlier",
            value=float(series[idx]),
            threshold=threshold,
            row_index=int(idx),
            severity="critical" if zscores[idx] > 5 else "warning"
        ))
    return anomalies


def groq_narrative(dataset: str, anomalies: List[Anomaly],
                   stats: dict) -> str:
    if not anomalies:
        return f"No anomalies detected in {dataset} dataset. Data looks healthy."

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        api_key=os.getenv("GROQ_API_KEY"),
        temperature=0,
        max_tokens=512
    )

    anomaly_summary = "\n".join([
        f"- {a.column}: {a.type} — value={round(a.value,2)}, "
        f"threshold={round(a.threshold,2)}, severity={a.severity}"
        for a in anomalies[:10]
    ])

    stats_summary = "\n".join([
        f"- {k}: {round(v,2) if isinstance(v, float) else v}"
        for k, v in stats.items()
    ])

    prompt = f"""You are a senior data analyst reviewing an ETL pipeline report.

Dataset: {dataset}
Key statistics:
{stats_summary}

Anomalies detected:
{anomaly_summary}

Write a concise 3-4 sentence business narrative explaining:
1. What the anomalies mean in plain English
2. Which ones are most concerning and why
3. What action the business should take

Be specific with numbers. No markdown, no bullet points. Plain paragraph only."""

    try:
        response = llm.invoke(prompt)
        return response.content.strip()
    except Exception as e:
        log.warning(f"Groq narrative failed: {e}")
        return f"{len(anomalies)} anomalies detected in {dataset}. Manual review recommended."


def analyse_sales(engine) -> AnomalyResult:
    log.info("Running anomaly detection on sales")
    result = AnomalyResult(dataset="sales")

    df = pd.read_sql("SELECT * FROM sales", engine)

    result.anomalies += detect_iqr_outliers(df, "unit_price", "sales")
    result.anomalies += detect_iqr_outliers(df, "quantity",   "sales")
    result.anomalies += detect_iqr_outliers(df, "revenue",    "sales")
    result.anomalies += detect_zscore_outliers(df, "unit_price", "sales")
    result.anomalies += detect_zscore_outliers(df, "revenue",    "sales")

    stats = {
        "total_rows":    len(df),
        "total_revenue": df["revenue"].sum(),
        "avg_order_val": df["revenue"].mean(),
        "max_price":     df["unit_price"].max(),
        "cancelled_pct": round((df["status"]=="cancelled").mean()*100, 1),
        "returned_pct":  round((df["status"]=="returned").mean()*100, 1),
    }

    result.narrative = groq_narrative("sales", result.anomalies, stats)
    log.info(f"Sales: {result.count} anomalies detected")
    return result


def analyse_inventory(engine) -> AnomalyResult:
    log.info("Running anomaly detection on inventory")
    result = AnomalyResult(dataset="inventory")

    df = pd.read_sql("SELECT * FROM inventory", engine)

    result.anomalies += detect_iqr_outliers(df, "stock_quantity", "inventory")
    result.anomalies += detect_iqr_outliers(df, "unit_cost",      "inventory")

    low_stock = df[df["low_stock"] == True]
    for _, row in low_stock.iterrows():
        result.anomalies.append(Anomaly(
            dataset="inventory",
            column="stock_quantity",
            type="below_reorder_level",
            value=float(row["stock_quantity"]),
            threshold=float(row["reorder_level"]),
            severity="critical"
        ))

    stats = {
        "total_products":        len(df),
        "out_of_stock":          int((df["stock_quantity"]==0).sum()),
        "below_reorder":         int((df["low_stock"]==True).sum()),
        "total_inventory_value": df["inventory_value"].sum(),
        "avg_stock":             df["stock_quantity"].mean(),
    }

    result.narrative = groq_narrative("inventory", result.anomalies, stats)
    log.info(f"Inventory: {result.count} anomalies detected")
    return result


def analyse_customers(engine) -> AnomalyResult:
    log.info("Running anomaly detection on customers")
    result = AnomalyResult(dataset="customers")

    df = pd.read_sql("SELECT * FROM customers", engine)

    result.anomalies += detect_iqr_outliers(df, "total_spent", "customers")
    result.anomalies += detect_zscore_outliers(df, "total_spent", "customers")
    result.anomalies += detect_iqr_outliers(df, "tenure_days", "customers")

    stats = {
        "total_customers":  len(df),
        "avg_total_spent":  df["total_spent"].mean(),
        "max_total_spent":  df["total_spent"].max(),
        "avg_tenure_days":  df["tenure_days"].mean(),
        "platinum_count":   int((df["customer_tier"]=="Platinum").sum()),
    }

    result.narrative = groq_narrative("customers", result.anomalies, stats)
    log.info(f"Customers: {result.count} anomalies detected")
    return result


def analyse_all() -> dict:
    log.info("=== ANOMALY STAGE START ===")
    engine = get_engine()

    results = {
        "sales":     analyse_sales(engine),
        "inventory": analyse_inventory(engine),
        "customers": analyse_customers(engine),
    }

    for name, r in results.items():
        log.info(f"{name}: {r.count} anomalies — narrative ready")

    log.info("=== ANOMALY STAGE DONE ===")
    return results


if __name__ == "__main__":
    results = analyse_all()
    for name, r in results.items():
        print(f"\n{'='*50}")
        print(f"{name.upper()} — {r.count} anomalies")
        print(f"Narrative: {r.narrative}")
        for a in r.anomalies[:5]:
            print(f"  [{a.severity}] {a.column}: {a.type} "
                  f"value={round(a.value,2)}")