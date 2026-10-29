import glob
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Retail Sales Dashboard",
    page_icon="📊",
    layout="wide",
)

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data/processed"))


@st.cache_data(ttl=30)
def load_csv(directory_name):
    folder = DATA_DIR / directory_name
    files = sorted(glob.glob(str(folder / "part-*.csv")))

    if not files:
        return pd.DataFrame()

    frames = [pd.read_csv(file) for file in files]
    return pd.concat(frames, ignore_index=True)


st.title("📊 Retail Sales Dashboard")
st.caption("Sales performance and data quality insights from the Spark pipeline.")

sales = load_csv("retail_sales")
category_sales = load_csv("sales_by_category")

if sales.empty or category_sales.empty:
    st.warning(
        "Processed sales data was not found. Run the retail_sales_pipeline "
        "in Airflow, then refresh this dashboard."
    )
    st.stop()

required_columns = {
    "transaction_id",
    "transaction_date",
    "product",
    "category",
    "quantity",
    "unit_price",
    "store",
    "total_sales",
}

missing_columns = required_columns - set(sales.columns)

if missing_columns:
    st.error(f"Processed sales data is missing columns: {sorted(missing_columns)}")
    st.stop()

sales["transaction_date"] = pd.to_datetime(
    sales["transaction_date"], errors="coerce"
)
sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce")
sales["unit_price"] = pd.to_numeric(sales["unit_price"], errors="coerce")
sales["total_sales"] = pd.to_numeric(sales["total_sales"], errors="coerce")

sales = sales.dropna(
    subset=["transaction_date", "quantity", "unit_price", "total_sales"]
)

if sales.empty:
    st.error("No valid sales records are available to display.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

min_date = sales["transaction_date"].min().date()
max_date = sales["transaction_date"].max().date()

date_range = st.sidebar.date_input(
    "Transaction date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

categories = sorted(sales["category"].dropna().astype(str).unique())
selected_categories = st.sidebar.multiselect(
    "Categories",
    options=categories,
    default=categories,
)

stores = sorted(sales["store"].fillna("Unknown").astype(str).unique())
selected_stores = st.sidebar.multiselect(
    "Stores",
    options=stores,
    default=stores,
)

filtered = sales.copy()

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        filtered["transaction_date"].dt.date.between(start_date, end_date)
    ]

if selected_categories:
    filtered = filtered[
        filtered["category"].astype(str).isin(selected_categories)
    ]
else:
    filtered = filtered.iloc[0:0]

if selected_stores:
    filtered = filtered[
        filtered["store"].fillna("Unknown").astype(str).isin(selected_stores)
    ]
else:
    filtered = filtered.iloc[0:0]

if filtered.empty:
    st.info("No transactions match the selected filters.")
    st.stop()

# KPI cards
total_revenue = filtered["total_sales"].sum()
transaction_count = filtered["transaction_id"].nunique()
units_sold = filtered["quantity"].sum()
average_transaction = (
    total_revenue / transaction_count if transaction_count else 0
)

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Revenue", f"{total_revenue:,.2f}")
col2.metric("Transactions", f"{transaction_count:,}")
col3.metric("Units Sold", f"{units_sold:,.0f}")
col4.metric("Average Transaction", f"{average_transaction:,.2f}")

st.divider()

# Revenue by category and category share
left, right = st.columns(2)

category_summary = (
    filtered.groupby("category", as_index=False)
    .agg(
        total_revenue=("total_sales", "sum"),
        transaction_count=("transaction_id", "nunique"),
        units_sold=("quantity", "sum"),
    )
    .sort_values("total_revenue", ascending=False)
)

with left:
    st.subheader("Revenue by Category")
    fig = px.bar(
        category_summary,
        x="category",
        y="total_revenue",
        color="category",
        labels={
            "category": "Category",
            "total_revenue": "Revenue",
        },
        text_auto=".2s",
    )
    fig.update_layout(showlegend=False, yaxis_title="Revenue")
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Revenue Share by Category")
    fig = px.pie(
        category_summary,
        names="category",
        values="total_revenue",
        hole=0.45,
    )
    st.plotly_chart(fig, use_container_width=True)

# Revenue trend
st.subheader("Revenue Over Time")

daily_sales = (
    filtered.assign(
        transaction_day=filtered["transaction_date"].dt.date
    )
    .groupby("transaction_day", as_index=False)["total_sales"]
    .sum()
)

fig = px.line(
    daily_sales,
    x="transaction_day",
    y="total_sales",
    markers=True,
    labels={
        "transaction_day": "Date",
        "total_sales": "Revenue",
    },
)
st.plotly_chart(fig, use_container_width=True)

# Store performance
st.subheader("Store Performance")

store_summary = (
    filtered.assign(store=filtered["store"].fillna("Unknown"))
    .groupby("store", as_index=False)
    .agg(
        total_revenue=("total_sales", "sum"),
        transaction_count=("transaction_id", "nunique"),
        units_sold=("quantity", "sum"),
    )
    .sort_values("total_revenue", ascending=False)
)

fig = px.bar(
    store_summary,
    x="store",
    y="total_revenue",
    color="store",
    labels={"store": "Store", "total_revenue": "Revenue"},
    text_auto=".2s",
)
fig.update_layout(showlegend=False, yaxis_title="Revenue")
st.plotly_chart(fig, use_container_width=True)

# Transaction details
st.subheader("Transaction Details")

display_sales = filtered.sort_values(
    "transaction_date", ascending=False
).copy()

display_sales["transaction_date"] = display_sales[
    "transaction_date"
].dt.strftime("%Y-%m-%d")

st.dataframe(
    display_sales[
        [
            "transaction_id",
            "transaction_date",
            "product",
            "category",
            "quantity",
            "unit_price",
            "store",
            "total_sales",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

csv_data = display_sales.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download Filtered Transactions (CSV)",
    data=csv_data,
    file_name="filtered_retail_sales.csv",
    mime="text/csv",
)

st.caption(
    f"Showing {len(filtered):,} records from the processed Spark output. "
    "Figures update when the dashboard reloads after new pipeline results."
)

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
