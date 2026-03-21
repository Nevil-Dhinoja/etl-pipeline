import streamlit as st
import pandas as pd
import glob
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
load_dotenv()

st.set_page_config(
    page_title="ETL Pipeline Monitor",
    page_icon="📊",
    layout="wide"
)

def get_engine():
    password = quote_plus(os.getenv("DB_PASSWORD"))
    return create_engine(
        f"postgresql://postgres:{password}@localhost:5432/etl_pipeline"
    )

@st.cache_data(ttl=30)
def get_pipeline_stats(engine_url):
    engine = get_engine()
    with engine.connect() as conn:
        sales_count = conn.execute(
            text("SELECT COUNT(*) FROM sales")).fetchone()[0]
        inv_count = conn.execute(
            text("SELECT COUNT(*) FROM inventory")).fetchone()[0]
        cust_count = conn.execute(
            text("SELECT COUNT(*) FROM customers")).fetchone()[0]
        lineage_count = conn.execute(
            text("SELECT COUNT(*) FROM data_lineage")).fetchone()[0]
        total_rev = conn.execute(
            text("SELECT SUM(revenue) FROM sales")).fetchone()[0]
        cancelled = conn.execute(
            text("SELECT COUNT(*) FROM sales WHERE status='cancelled'")
        ).fetchone()[0]
    return {
        "sales": sales_count,
        "inventory": inv_count,
        "customers": cust_count,
        "lineage": lineage_count,
        "revenue": total_rev,
        "cancelled": cancelled
    }

@st.cache_data(ttl=30)
def get_revenue_by_region(engine_url):
    engine = get_engine()
    return pd.read_sql(
        "SELECT region, SUM(revenue) as revenue FROM sales GROUP BY region ORDER BY revenue DESC",
        engine
    )

@st.cache_data(ttl=30)
def get_revenue_by_product(engine_url):
    engine = get_engine()
    return pd.read_sql(
        "SELECT product, SUM(revenue) as revenue FROM sales GROUP BY product ORDER BY revenue DESC LIMIT 5",
        engine
    )

@st.cache_data(ttl=30)
def get_status_breakdown(engine_url):
    engine = get_engine()
    return pd.read_sql(
        "SELECT status, COUNT(*) as count FROM sales GROUP BY status",
        engine
    )

@st.cache_data(ttl=30)
def get_lineage(engine_url):
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM data_lineage ORDER BY run_at DESC LIMIT 50",
        engine
    )

@st.cache_data(ttl=30)
def get_inventory(engine_url):
    engine = get_engine()
    return pd.read_sql(
        "SELECT product, stock_quantity, reorder_level, stock_status, inventory_value FROM inventory ORDER BY stock_quantity ASC",
        engine
    )

@st.cache_data(ttl=30)
def get_customer_tiers(engine_url):
    engine = get_engine()
    return pd.read_sql(
        "SELECT customer_tier, COUNT(*) as count, AVG(total_spent) as avg_spent FROM customers GROUP BY customer_tier ORDER BY avg_spent DESC",
        engine
    )

def get_reports():
    files = sorted(glob.glob("reports/*.html"), reverse=True)
    return files

engine_url = os.getenv("DB_URL", "")

st.title("ETL Pipeline Monitor")
st.caption("Live view of your PostgreSQL data + pipeline history")

col1, col2 = st.columns([3, 1])
with col2:
    if st.button("Run Pipeline Now", type="primary"):
        with st.spinner("Running full ETL pipeline..."):
            import subprocess
            result = subprocess.run(
                ["py", "main.py"],
                capture_output=True, text=True,
                cwd=os.path.dirname(os.path.dirname(__file__))
            )
            st.cache_data.clear()
            if result.returncode == 0:
                st.success("Pipeline completed successfully")
            else:
                st.error(f"Pipeline failed: {result.stderr[-500:]}")

try:
    stats = get_pipeline_stats(engine_url)

    st.markdown("### Live database stats")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Sales rows",     f"{stats['sales']:,}")
    c2.metric("Inventory rows", stats['inventory'])
    c3.metric("Customers",      f"{stats['customers']:,}")
    c4.metric("Lineage steps",  f"{stats['lineage']:,}")
    c5.metric("Total revenue",  f"Rs.{stats['revenue']:,.0f}")
    c6.metric("Cancelled orders", stats['cancelled'])

    st.divider()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Sales analytics", "Inventory", "Customers",
        "Data lineage", "Reports"
    ])

    with tab1:
        st.markdown("#### Revenue by region")
        rev_region = get_revenue_by_region(engine_url)
        st.bar_chart(rev_region.set_index("region"))

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Top 5 products by revenue")
            rev_product = get_revenue_by_product(engine_url)
            st.bar_chart(rev_product.set_index("product"))
        with col2:
            st.markdown("#### Order status breakdown")
            status_df = get_status_breakdown(engine_url)
            st.bar_chart(status_df.set_index("status"))

    with tab2:
        st.markdown("#### Inventory health")
        inv_df = get_inventory(engine_url)
        def colour_status(val):
            if val == "OUT_OF_STOCK": return "background-color: #FFCDD2"
            if val == "LOW":          return "background-color: #FFE0B2"
            if val == "MEDIUM":       return "background-color: #FFF9C4"
            return "background-color: #C8E6C9"
        st.dataframe(
            inv_df.style.map(colour_status, subset=["stock_status"]),
            use_container_width=True
            )

    with tab3:
        st.markdown("#### Customer tier breakdown")
        tier_df = get_customer_tiers(engine_url)
        tier_df["avg_spent"] = tier_df["avg_spent"].round(2)
        st.dataframe(tier_df, use_container_width=True)
        st.bar_chart(tier_df.set_index("customer_tier")["count"])

    with tab4:
        st.markdown("#### Last 50 lineage records")
        lineage_df = get_lineage(engine_url)
        st.dataframe(lineage_df, use_container_width=True)

    with tab5:
        st.markdown("#### Pipeline reports")
        reports = get_reports()
        if reports:
            for r in reports:
                name = os.path.basename(r)
                col1, col2 = st.columns([4, 1])
                col1.write(name)
                with open(r, "r", encoding="utf-8") as f:
                    col2.download_button(
                        "Download",
                        data=f.read(),
                        file_name=name,
                        mime="text/html",
                        key=name
                    )
        else:
            st.info("No reports yet. Run the pipeline first.")

except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.info("Make sure PostgreSQL is running and .env is configured correctly.")