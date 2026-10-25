import os
import json
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data/raw"
RAG_APP = ROOT / "src/rag/app.py"

CUSTOMERS = DATA / "olist_customers_dataset.csv"
ORDERS = DATA / "olist_orders_dataset.csv"
PRODUCTS = DATA / "olist_products_dataset.csv"
ALERTS = ROOT / "data/logs/cart_abandoned_alerts.jsonl"

SEGMENT_PATH = (
    "/ecommerce_customer360/ml/customer_segmentation_business_labels/"
    "run_20261023_074500"
)

st.set_page_config(
    page_title="Customer 360 Intelligence",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

:root {
    --bg: #080d1b;
    --panel: #111a2e;
    --panel2: #17233b;
    --border: #273653;
    --purple: #a78bfa;
    --cyan: #22d3ee;
    --text: #f1f5f9;
    --muted: #94a3b8;
}

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: radial-gradient(ellipse at top left, #17213b 0%, #080d1b 55%);
    color: var(--text);
}

[data-testid="stHeader"] {
    background: transparent;
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1527, #080d1b);
    border-right: 1px solid var(--border);
}

[data-testid="stSidebar"] * {
    color: #dbeafe;
}

[data-testid="stSidebar"] label {
    font-size: 15px !important;
    font-weight: 600 !important;
}

.block-container {
    max-width: 1500px;
    padding-top: 2rem;
    padding-bottom: 3rem;
}

h1 {
    font-size: 2.45rem !important;
    font-weight: 800 !important;
    letter-spacing: -1.2px;
    color: #f8fafc !important;
}

h2 {
    font-size: 1.65rem !important;
    font-weight: 700 !important;
    color: #e2e8f0 !important;
}

h3 {
    font-size: 1.25rem !important;
    font-weight: 700 !important;
    color: #c4b5fd !important;
}

p, label, li {
    font-size: 15px;
    line-height: 1.75;
}

[data-testid="stMetric"] {
    background: linear-gradient(145deg, #18243b, #10192b);
    border: 1px solid #344563;
    border-radius: 18px;
    padding: 22px 24px;
    box-shadow: 0 8px 28px rgba(0,0,0,.18);
}

[data-testid="stMetricLabel"] {
    color: #a5b4cc !important;
    font-size: 14px !important;
    font-weight: 600 !important;
}

[data-testid="stMetricValue"] {
    color: #c4b5fd !important;
    font-size: 2.1rem !important;
    font-weight: 800 !important;
}

[data-testid="stMetricDelta"] {
    font-size: 13px !important;
}

[data-testid="stDataFrame"],
[data-testid="stTable"] {
    border: 1px solid #2b3b55;
    border-radius: 12px;
    overflow: hidden;
}

[data-testid="stAlert"] {
    border-radius: 12px;
}

div[data-testid="stPlotlyChart"] {
    background: #111a2e;
    border: 1px solid #273653;
    border-radius: 16px;
    padding: 12px;
}

.stButton > button {
    background: linear-gradient(110deg, #7c3aed, #4f46e5);
    color: white;
    border: 0;
    border-radius: 10px;
    padding: 10px 22px;
    font-weight: 700;
    transition: .2s ease;
}

.stButton > button:hover {
    background: linear-gradient(110deg, #8b5cf6, #6366f1);
    border: 1px solid #a78bfa;
    transform: translateY(-1px);
}

hr {
    border-color: #273653;
}

small, .stCaption {
    color: #94a3b8 !important;
}

</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    customers = pd.read_csv(CUSTOMERS)
    orders = pd.read_csv(ORDERS)
    products = pd.read_csv(PRODUCTS)

    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )

    return customers, orders, products


@st.cache_data
def load_segments():
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-getmerge", SEGMENT_PATH, "-"],
            capture_output=True,
            text=False,
            timeout=90
        )

        if result.returncode != 0:
            return pd.DataFrame()

        from io import BytesIO
        return pd.read_parquet(BytesIO(result.stdout))

    except Exception:
        return pd.DataFrame()


@st.cache_data
def load_alerts():
    if not ALERTS.exists():
        return pd.DataFrame()

    records = []

    with open(ALERTS, encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    return pd.DataFrame(records)


try:
    customers, orders, products = load_data()
except Exception as e:
    st.error(f"Dataset loading failed: {e}")
    st.stop()


st.sidebar.title("🛍️ Customer 360")
st.sidebar.caption("E-Commerce Intelligence Platform")

page = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "Sales Funnel",
        "Cohort Retention",
        "Customer Segmentation",
        "Purchase Propensity",
        "Abandoned Carts",
        "Data Pipeline",
        "AI Chatbot"
    ]
)

st.sidebar.divider()
st.sidebar.caption("Data-driven analytics")
st.sidebar.caption("Spark • HDFS • ML • RAG")


# ========================= OVERVIEW =========================

if page == "Overview":

    st.title("🛍️ Customer 360 Intelligence")
    st.caption("Unified customer analytics and machine learning dashboard")

    total_customers = len(customers)
    unique_customers = customers["customer_unique_id"].nunique()
    total_orders = len(orders)
    total_products = len(products)

    completed = orders[
        orders["order_status"] == "delivered"
    ]

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Customer Records", f"{total_customers:,}")
    c2.metric("Unique Customers", f"{unique_customers:,}")
    c3.metric("Total Orders", f"{total_orders:,}")
    c4.metric("Products", f"{total_products:,}")

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Order Status Distribution")

        status = orders["order_status"].value_counts().reset_index()
        status.columns = ["Status", "Orders"]

        fig = px.pie(
            status,
            names="Status",
            values="Orders",
            hole=0.55
        )

        fig.update_layout(
            template="plotly_dark",
            font=dict(family="Inter", size=14, color="#e2e8f0"),
            paper_bgcolor="#111a2e",
            plot_bgcolor="#111a2e"
        )

        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Monthly Order Trend")

        monthly = (
            orders.dropna(subset=["order_purchase_timestamp"])
            .assign(
                month=lambda x:
                x["order_purchase_timestamp"].dt.to_period("M").astype(str)
            )
            .groupby("month")
            .size()
            .reset_index(name="Orders")
        )

        fig = px.line(
            monthly,
            x="month",
            y="Orders",
            markers=True
        )

        fig.update_layout(
            template="plotly_dark", font=dict(family="Inter", size=14, color="#e2e8f0"), paper_bgcolor="#111a2e", plot_bgcolor="#111a2e",
        )

        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Dataset Summary")

    summary = pd.DataFrame({
        "Dataset": ["Customers", "Orders", "Products"],
        "Records": [
            len(customers),
            len(orders),
            len(products)
        ]
    })

    st.dataframe(summary, use_container_width=True)


# ========================= SALES FUNNEL =========================

elif page == "Sales Funnel":

    st.title("📊 Sales Funnel")

    status = orders["order_status"].value_counts().reset_index()
    status.columns = ["Order Status", "Orders"]

    fig = px.funnel(
        status,
        x="Orders",
        y="Order Status",
        title="Order Status Funnel"
    )

    fig.update_layout(template="plotly_dark", font=dict(family="Inter", size=14, color="#e2e8f0"), paper_bgcolor="#111a2e", plot_bgcolor="#111a2e")

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Order Status Details")
    st.dataframe(status, use_container_width=True)

    st.info(
        "Order-status funnel is based on recorded order statuses. "
        "It is not a website visit-to-purchase conversion funnel."
    )


# ========================= COHORT RETENTION =========================

elif page == "Cohort Retention":

    st.title("📈 Cohort Retention")

    delivered = orders[
        orders["order_status"] == "delivered"
    ].copy()

    delivered = delivered.dropna(
        subset=["customer_id", "order_purchase_timestamp"]
    )

    delivered["order_month"] = (
        delivered["order_purchase_timestamp"].dt.to_period("M")
    )

    first_purchase = (
        delivered.groupby("customer_id")["order_month"]
        .min()
        .rename("cohort_month")
    )

    delivered = delivered.join(
        first_purchase,
        on="customer_id"
    )

    delivered["cohort_age"] = (
        delivered["order_month"].astype("int64")
        - delivered["cohort_month"].astype("int64")
    )

    cohort_counts = delivered.groupby(
        ["cohort_month", "cohort_age"]
    )["customer_id"].nunique().unstack(fill_value=0)

    if not cohort_counts.empty:
        retention = cohort_counts.div(
            cohort_counts.iloc[:, 0], axis=0
        )

        retention.index = retention.index.astype(str)

        fig = px.imshow(
            retention,
            text_auto=".0%",
            aspect="auto",
            color_continuous_scale="Purples",
            labels={
                "x": "Months Since First Purchase",
                "y": "Cohort Month",
                "color": "Retention"
            }
        )

        fig.update_layout(template="plotly_dark", font=dict(family="Inter", size=14, color="#e2e8f0"), paper_bgcolor="#111a2e", plot_bgcolor="#111a2e")

        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            "Retention uses customers with delivered orders. "
            "Month 0 is the first delivered-order cohort month."
        )
    else:
        st.warning("No delivered-order cohort data available.")


# ========================= SEGMENTATION =========================

elif page == "Customer Segmentation":

    st.title("👥 Customer Segmentation")
    st.caption("K-Means business segment labels from HDFS")

    segments = load_segments()

    if segments.empty:
        st.warning(
            "Unable to load segmentation output from HDFS. "
            "Check HDFS availability and permissions."
        )
    else:
        st.metric("Segmented Customers", f"{len(segments):,}")

        if "business_segment" in segments.columns:
            counts = (
                segments["business_segment"]
                .value_counts()
                .reset_index()
            )

            counts.columns = ["Segment", "Customers"]

            fig = px.bar(
                counts,
                x="Segment",
                y="Customers",
                color="Segment",
                title="Customer Segment Distribution"
            )

            fig.update_layout(template="plotly_dark", font=dict(family="Inter", size=14, color="#e2e8f0"), paper_bgcolor="#111a2e", plot_bgcolor="#111a2e")

            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(counts, use_container_width=True)

        if {"recency_days", "frequency"}.issubset(segments.columns):
            fig = px.scatter(
                segments,
                x="recency_days",
                y="frequency",
                color="business_segment",
                title="Recency vs Frequency"
            )

            fig.update_layout(template="plotly_dark", font=dict(family="Inter", size=14, color="#e2e8f0"), paper_bgcolor="#111a2e", plot_bgcolor="#111a2e")

            st.plotly_chart(fig, use_container_width=True)


# ========================= PROPENSITY =========================

elif page == "Purchase Propensity":

    st.title("🤖 Purchase Propensity")

    st.metric("Model", "Random Forest")
    st.metric("Best Existing Test ROC-AUC", "0.6761")
    st.metric("Best Existing Test PR-AUC", "0.0084")
    st.metric("Test F1 at threshold 0.5", "0.01645")

    st.info(
        "Metrics are from the existing tuned model evaluation. "
        "They are not live prediction results."
    )

    st.markdown("""
    **Existing model status**

    - Test ROC-AUC: 0.6761
    - Test PR-AUC: 0.0084
    - F1: 0.01645
    - Threshold: 0.5
    - True positives: 16
    - False positives: 1,888
    - False negatives: 25

    The positive class is rare, so accuracy alone is not a suitable
    evaluation metric.
    """)


# ========================= CART MONITORING =========================

elif page == "Abandoned Carts":

    st.title("🛒 Abandoned Cart Monitoring")

    alerts = load_alerts()

    if alerts.empty:
        st.info("No cart alert records found.")
    else:
        st.metric("Recorded Alerts", len(alerts))

        if "is_synthetic" in alerts.columns:
            synthetic = alerts["is_synthetic"].fillna(False).astype(bool)

            st.metric("Synthetic Test Alerts", int(synthetic.sum()))

            st.warning(
                "Synthetic test alerts are displayed separately "
                "and are not real customer abandonment."
            )

        st.dataframe(alerts, use_container_width=True)

    st.subheader("Streaming Components")

    st.markdown("""
    - Cart Event Producer
    - Cart Events Stream
    - Window Aggregation
    - Abandoned Cart Alerts
    - HBase Cart State
    """)


# ========================= PIPELINE =========================

elif page == "Data Pipeline":

    st.title("⚙️ Data Pipeline Architecture")

    stages = [
        "Olist CSV Datasets",
        "Spark Batch Processing",
        "HDFS Storage",
        "Customer Segmentation",
        "Purchase Propensity",
        "Kafka Cart Events",
        "Spark Streaming",
        "HBase Cart State",
        "RAG Knowledge Base",
        "Streamlit Dashboard"
    ]

    for i, stage in enumerate(stages, 1):
        st.markdown(f"**{i}.** {stage}")

    st.subheader("Project Components")

    components = pd.DataFrame({
        "Component": [
            "Apache Spark",
            "HDFS",
            "Kafka",
            "HBase",
            "Airflow",
            "Machine Learning",
            "ChromaDB",
            "Streamlit"
        ],
        "Purpose": [
            "Batch and streaming processing",
            "Distributed data storage",
            "Event streaming",
            "Cart state storage",
            "Workflow orchestration",
            "Segmentation and propensity",
            "RAG vector database",
            "Analytics dashboard"
        ]
    })

    st.dataframe(components, use_container_width=True)



# ========================= RAG CHATBOT =========================

elif page == "AI Chatbot":

    st.title("🧠 Customer 360 AI Assistant")
    st.caption("Ask questions about customers, orders, products, and datasets.")

    st.markdown("""
    <style>
    iframe {
        border: 1px solid #334155;
        border-radius: 14px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.info(
        "Powered by the existing Customer 360 RAG knowledge base."
    )

    st.iframe(
        "http://localhost:8503",
        height=850
    )

