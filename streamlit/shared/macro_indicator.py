"""
Macro Indicator Dashboard.

Module này chứa toàn bộ logic dashboard kinh tế vĩ mô:
    - Truy vấn fact_macro_indicator từ Spark Gold layer.
    - Join fact với dim_time và dim_indicator.
    - Tập trung vào CPI, inflation, macro trend.
    - Tính toán metric bằng Spark:
        Previous value, MoM/QoQ change, MoM/QoQ growth,
        previous year value, YoY change, YoY growth,
        rolling average, rolling volatility.
    - Vẽ biểu đồ chính:
        Indicator Trend, YoY Growth Bar, MoM/QoQ Growth Bar,
        Combined Value + Growth, Rolling Average, Drill-down.
    - Giao diện đồng bộ với các dashboard khác.

app.py và tabs/macro_indicator.py không nên chứa logic xử lý dữ liệu;
toàn bộ nằm trong file này, expose qua hàm render_dashboard().
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.spark import get_spark_session


# ====================================================================
# THEME / COLOR CONSTANTS
# ====================================================================

COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#102B55"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_POSITIVE = "#2ECC71"
COLOR_NEGATIVE = "#E74C3C"
COLOR_TEXT = "#FFFFFF"
COLOR_TEXT_MUTED = "#CFE3FB"

DISCRETE_PALETTE = [
    "#3FA9F5",
    "#2ECC71",
    "#E74C3C",
    "#F5B041",
    "#9B59B6",
    "#1ABC9C",
    "#E67E22",
    "#5DADE2",
    "#F1948A",
    "#48C9B0",
]

CHART_PLOT_BG = "#090A44"
CHART_PAPER_BG = "#090A44"
CHART_FONT_COLOR = "#FFFFFF"


# ====================================================================
# CSS
# ====================================================================

def inject_custom_css() -> None:
    """Inject CSS cho Macro Indicator Dashboard."""
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: #FFFFFF;
        }}

        .macro-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .macro-header h1 {{
            color: #FFFFFF !important;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .macro-header p {{
            color: #FFFFFF !important;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .macro-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
            color: #FFFFFF !important;
        }}

        .macro-section-title {{
            color: #FFFFFF !important;
            font-size: 18px;
            font-weight: 600;
            margin: 6px 0 12px 4px;
            border-left: 4px solid {COLOR_ACCENT};
            padding-left: 10px;
        }}

        .kpi-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 16px 14px;
            text-align: center;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            height: 100%;
            color: #FFFFFF !important;
        }}

        .kpi-label {{
            color: #FFFFFF !important;
            font-size: 13px;
            font-weight: 500;
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
        }}

        .kpi-value {{
            color: #FFFFFF !important;
            font-size: 24px;
            font-weight: 700;
        }}

        .kpi-positive {{
            color: {COLOR_POSITIVE} !important;
        }}

        .kpi-negative {{
            color: {COLOR_NEGATIVE} !important;
        }}

        .macro-chart-wrapper {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 10px 14px 4px 14px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 16px;
            color: #FFFFFF !important;
        }}

        .empty-state {{
            color: #FFFFFF !important;
            text-align: center;
            padding: 40px 0;
            font-size: 15px;
        }}

        label,
        .stMultiSelect label,
        .stSelectbox label {{
            color: #FFFFFF !important;
            font-weight: 600 !important;
        }}

        div[data-baseweb="select"] {{
            color: #FFFFFF !important;
        }}

        div[data-baseweb="select"] > div {{
            background-color: {COLOR_CARD} !important;
            border-color: {COLOR_BORDER} !important;
            color: #FFFFFF !important;
        }}

        div[data-baseweb="select"] span,
        div[data-baseweb="select"] input {{
            color: #FFFFFF !important;
        }}

        div[data-baseweb="tag"] {{
            background-color: {COLOR_HEADER} !important;
            color: #FFFFFF !important;
        }}

        div[data-baseweb="tag"] span {{
            color: #FFFFFF !important;
        }}

        ul[role="listbox"] {{
            background-color: {COLOR_CARD} !important;
            color: #FFFFFF !important;
        }}

        li[role="option"] {{
            background-color: {COLOR_CARD} !important;
            color: #FFFFFF !important;
        }}

        li[role="option"]:hover {{
            background-color: {COLOR_HEADER} !important;
            color: #FFFFFF !important;
        }}

        .stMarkdown,
        .stMarkdown p,
        .stMarkdown span,
        .stMarkdown div {{
            color: #FFFFFF !important;
        }}

        div[data-testid="stDataFrame"] {{
            color: #FFFFFF !important;
        }}

        div[data-testid="stAlert"] {{
            color: #FFFFFF !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    """Render header."""
    st.markdown(
        """
        <div class="macro-header">
            <h1>Macro Indicator Dashboard</h1>
            <p>Theo dõi CPI, lạm phát, tăng trưởng và các chỉ tiêu kinh tế vĩ mô theo thời gian</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING - SPARK FIRST
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu Macro Indicator...")
def load_data() -> pd.DataFrame:
    """
    Đọc dữ liệu macro indicator từ Gold layer.

    Spark xử lý toàn bộ metric:
        - prev_value
        - mom_qoq_change
        - mom_qoq_growth_pct
        - prev_year_value
        - yoy_change
        - yoy_growth_pct
        - rolling_avg_3p
        - rolling_avg_12p
        - rolling_volatility_12p

    Pandas chỉ dùng ở bước cuối để render Plotly/Streamlit.
    """
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold_gold.fact_macro_indicator")
    dim_time: SparkDataFrame = spark.table("gold_gold.dim_time")
    dim_indicator: SparkDataFrame = spark.table("gold_gold.dim_indicator")

    base_df = (
        fact.alias("f")
        .join(
            dim_time.alias("t"),
            F.col("f.time_key") == F.col("t.time_key"),
            "left",
        )
        .join(
            dim_indicator.alias("i"),
            F.col("f.indicator_key") == F.col("i.indicator_key"),
            "left",
        )
        .select(
            F.to_date(F.col("t.full_date")).alias("date"),
            F.col("t.year").cast("int").alias("year"),
            F.col("t.quarter").cast("int").alias("quarter"),
            F.col("t.month").cast("int").alias("month"),
            F.upper(F.trim(F.col("i.indicator_name"))).alias("indicator_name"),
            F.upper(F.trim(F.col("f.unit_name"))).alias("unit_name"),
            F.upper(F.trim(F.col("f.period_grain"))).alias("period_grain"),
            F.col("f.value").cast("double").alias("value"),
        )
        .where(
            F.col("date").isNotNull()
            & F.col("indicator_name").isNotNull()
            & F.col("period_grain").isNotNull()
            & F.col("unit_name").isNotNull()
            & F.col("value").isNotNull()
        )
    )

    group_cols = ["indicator_name", "period_grain", "unit_name"]

    w_ordered = (
        Window
        .partitionBy(*group_cols)
        .orderBy("date")
    )

    metric_df = (
       base_df
        .withColumn(
            "prev_value",
            F.lag("value", 1).over(w_ordered),
        )
        .withColumn(
            "mom_qoq_change",
            F.col("value") - F.col("prev_value"),
        )
        .withColumn(
            "mom_qoq_growth_pct",
            F.when(
                F.col("prev_value").isNotNull()
                & (F.col("prev_value") != F.lit(0.0)),
                (F.col("mom_qoq_change") / F.col("prev_value")) * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    metric_df = (
        metric_df
        .withColumn(
            "prev_year_value",
            F.when(
                F.col("period_grain") == F.lit("MONTHLY"),
                F.lag("value", 12).over(w_ordered),
            )
            .when(
                F.col("period_grain") == F.lit("QUARTERLY"),
                F.lag("value", 4).over(w_ordered),
            )
            .when(
                F.col("period_grain") == F.lit("YEARLY"),
                F.lag("value", 1).over(w_ordered),
            )
            .otherwise(
                F.lag("value", 12).over(w_ordered)
            ),
        )
        .withColumn(
            "yoy_change",
            F.col("value") - F.col("prev_year_value"),
        )
        .withColumn(
            "yoy_growth_pct",
            F.when(
                F.col("prev_year_value").isNotNull()
                & (F.col("prev_year_value") != F.lit(0.0)),
                (F.col("yoy_change") / F.col("prev_year_value")) * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    w_3p = (
        Window
        .partitionBy(*group_cols)
        .orderBy("date")
        .rowsBetween(-2, 0)
    )

    w_12p = (
        Window
        .partitionBy(*group_cols)
        .orderBy("date")
        .rowsBetween(-11, 0)
    )

    metric_df = (
        metric_df
        .withColumn(
            "rolling_avg_3p",
            F.avg("value").over(w_3p),
        )
           .withColumn(
            "rolling_avg_12p",
            F.avg("value").over(w_12p),
        )
        .withColumn(
            "rolling_volatility_12p",
            F.stddev_samp("mom_qoq_growth_pct").over(w_12p),
        )
        .withColumn(
            "date_label",
            F.date_format(F.col("date"), "yyyy-MM-dd"),
        )
    )

    final_df = (
        metric_df
        .select(
            "date",
            "date_label",
            "year",
            "quarter",
            "month",
            "indicator_name",
            "unit_name",
            "period_grain",
            "value",
            "prev_value",
            "mom_qoq_change",
            "mom_qoq_growth_pct",
            "prev_year_value",
            "yoy_change",
            "yoy_growth_pct",
            "rolling_avg_3p",
            "rolling_avg_12p",
            "rolling_volatility_12p",
        )
        .orderBy(
            "indicator_name",
            "period_grain",
            "unit_name",
            "date",
        )
    )

    pdf = final_df.toPandas()

    if pdf.empty:
        return pdf

    pdf["date"] = pd.to_datetime(pdf["date"], errors="coerce")

    numeric_cols = [
        "value",
        "prev_value",
        "mom_qoq_change",
        "mom_qoq_growth_pct",
        "prev_year_value",
        "yoy_change",
        "yoy_growth_pct",
        "rolling_avg_3p",
        "rolling_avg_12p",
        "rolling_volatility_12p",
    ]

    for col in numeric_cols:
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

    pdf = pdf.dropna(subset=["date", "indicator_name", "value"])
    pdf = pdf.sort_values(
        ["indicator_name", "period_grain", "unit_name", "date"]
    ).reset_index(drop=True)

    return pdf


# ====================================================================
# FILTERS
# ====================================================================

def get_filter_options(
    df: pd.DataFrame,
    selected_years: list[Any] | None = None,
    selected_period_grains: list[Any] | None = None,
    selected_indicators: list[Any] | None = None,
) -> dict[str, list[Any]]:
    """Lấy option filter theo hierarchy Year -> Period Grain -> Indicator."""
    if df.empty:
        return {
            "year": [],
            "period_grain": [],
            "indicator_name": [],
            "unit_name": [],
        }

    pool = df.copy()

    if selected_years:
        pool = pool[pool["year"].isin(selected_years)]

    if selected_period_grains:
        pool = pool[pool["period_grain"].isin(selected_period_grains)]

    if selected_indicators:
        pool = pool[pool["indicator_name"].isin(selected_indicators)]

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "period_grain": sorted(pool["period_grain"].dropna().unique().tolist()),
        "indicator_name": sorted(pool["indicator_name"].dropna().unique().tolist()),
        "unit_name": sorted(pool["unit_name"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    period_grains: list[Any],
    indicators: list[Any],
    units: list[Any],
) -> pd.DataFrame:
    """Áp dụng filter toàn cục."""
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]

    if period_grains:
        filtered = filtered[filtered["period_grain"].isin(period_grains)]

    if indicators:
        filtered = filtered[filtered["indicator_name"].isin(indicators)]

    if units:
        filtered = filtered[filtered["unit_name"].isin(units)]

    return filtered


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render filter hierarchy."""
    base_options = get_filter_options(df)

    st.markdown('<div class="macro-card">', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        years = st.multiselect(
            "Year",
            base_options["year"],
            default=[],
            key="macro_filter_year",
        )

    options_after_year = get_filter_options(
        df,
        selected_years=years,
    )

    with col2:
        period_grains = st.multiselect(
            "Period Grain",
            options_after_year["period_grain"],
            default=[],
            key="macro_filter_period_grain",
        )

    options_after_period = get_filter_options(
        df,
        selected_years=years,
        selected_period_grains=period_grains,
    )

    with col3:
        default_indicators = [
            x for x in options_after_period["indicator_name"]
            if "CPI" in x or "INFLATION" in x or "LẠM PHÁT" in x
        ]

        indicators = st.multiselect(
            "Indicator",
            options_after_period["indicator_name"],
            default=default_indicators[:3],
            key="macro_filter_indicator",
        )

    options_after_indicator = get_filter_options(
        df,
        selected_years=years,
        selected_period_grains=period_grains,
        selected_indicators=indicators,
    )

    with col4:
        units = st.multiselect(
            "Unit",
            options_after_indicator["unit_name"],
            default=[],
            key="macro_filter_unit",
        )

    st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(
        df=df,
        years=years,
        period_grains=period_grains,
        indicators=indicators,
        units=units,
    )


# ====================================================================
# FORMAT / KPI HELPERS
# ====================================================================

def _format_number(value: float) -> str:
    """Format số."""
    if pd.isna(value):
        return "N/A"

    abs_value = abs(value)
    sign = "-" if value < 0 else ""

    if abs_value >= 1_000_000_000_000:
        return f"{sign}{abs_value / 1_000_000_000_000:.2f}T"
    if abs_value >= 1_000_000_000:
        return f"{sign}{abs_value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.2f}K"

    return f"{sign}{abs_value:.2f}"


def _format_percent(value: float) -> str:
    """Format phần trăm."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"


def _kpi_card(label: str, value: str, css_class: str = "") -> str:
    """Tạo HTML KPI card."""
    return f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value {css_class}">{value}</div>
        </div>
    """


def render_kpis(df: pd.DataFrame) -> None:
    """Render KPI tổng quan macro."""
    st.markdown(
        '<div class="macro-section-title">Tổng quan KPI kinh tế vĩ mô</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.markdown(
            '<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>',
            unsafe_allow_html=True,
        )
        return

    latest_date = df["date"].max()
    latest_df = df[df["date"] == latest_date]

    latest_avg_value = latest_df["value"].mean()
    avg_value = df["value"].mean()
    latest_yoy = latest_df["yoy_growth_pct"].mean()
    latest_mom_qoq = latest_df["mom_qoq_growth_pct"].mean()
    max_yoy = df["yoy_growth_pct"].max()
    min_yoy = df["yoy_growth_pct"].min()
    indicator_count = df["indicator_name"].nunique()

    yoy_class = (
        "kpi-positive"
        if pd.notna(latest_yoy) and latest_yoy >= 0
        else "kpi-negative"
    )

    mom_qoq_class = (
        "kpi-positive"
        if pd.notna(latest_mom_qoq) and latest_mom_qoq >= 0
        else "kpi-negative"
    )

    cols = st.columns(7)

    kpi_data = [
        ("Latest Avg Value", _format_number(latest_avg_value), ""),
        ("Avg Value", _format_number(avg_value), ""),
        ("Latest YoY", _format_percent(latest_yoy), yoy_class),
        ("Latest MoM/QoQ", _format_percent(latest_mom_qoq), mom_qoq_class),
        ("Max YoY", _format_percent(max_yoy), "kpi-positive"),
        ("Min YoY", _format_percent(min_yoy), "kpi-negative"),
        ("Indicators", f"{indicator_count}", ""),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)


# ====================================================================
# CHART THEME
# ====================================================================

def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    """Áp dụng theme Plotly."""
    fig.update_layout(
        plot_bgcolor=CHART_PLOT_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(color=CHART_FONT_COLOR, size=12),
        height=height,
        margin=dict(l=40, r=30, t=50, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color="#FFFFFF"),
        ),
        title_font=dict(color="#FFFFFF"),
        xaxis=dict(color="#FFFFFF", gridcolor="rgba(255,255,255,0.15)"),
        yaxis=dict(color="#FFFFFF", gridcolor="rgba(255,255,255,0.15)"),
    )

    return fig


def _empty_chart_placeholder(message: str = "Không có dữ liệu để hiển thị") -> None:
    """Hiển thị placeholder."""
    st.markdown(
        f'<div class="empty-state">{message}</div>',
        unsafe_allow_html=True,
    )


def _chart_card(
    render_fn: Callable[[pd.DataFrame], go.Figure],
    df: pd.DataFrame,
    key: str,
) -> None:
    """Render chart trong card với key riêng để tránh trùng Plotly element ID."""
    st.markdown('<div class="macro-chart-wrapper">', unsafe_allow_html=True)

    if df.empty:
        _empty_chart_placeholder()
    else:
        fig = render_fn(df)
        st.plotly_chart(
            fig,
            use_container_width=True,
            key=key,
        )

    st.markdown("</div>", unsafe_allow_html=True)


# ====================================================================
# CHART FUNCTIONS
# ====================================================================

def chart_indicator_trend(df: pd.DataFrame) -> go.Figure:
    """Line chart: xu hướng giá trị indicator."""
    grouped = (
        df.groupby(["date", "indicator_name"], as_index=False)
        .agg(value=("value", "mean"))
        .sort_values("date")
    )

    fig = px.line(
        grouped,
        x="date",
        y="value",
        color="indicator_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Indicator Value Trend",
        labels={
            "date": "Date",
            "value": "Value",
            "indicator_name": "Indicator",
        },
    )

    return _apply_chart_theme(fig, height=420)


def chart_yoy_growth_bar(df: pd.DataFrame) -> go.Figure:
    """Bar chart: YoY growth, phù hợp cho CPI/lạm phát."""
    grouped = (
        df.groupby(["date", "indicator_name"], as_index=False)
        .agg(yoy_growth_pct=("yoy_growth_pct", "mean"))
        .dropna(subset=["yoy_growth_pct"])
        .sort_values("date")
    )

    if grouped.empty:
        fig = go.Figure()
        fig.update_layout(title="YoY Growth / Inflation Rate")
        return _apply_chart_theme(fig, height=420)

    fig = px.bar(
        grouped,
        x="date",
        y="yoy_growth_pct",
        color="indicator_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="YoY Growth / Inflation Rate",
        labels={
            "date": "Date",
            "yoy_growth_pct": "YoY Growth (%)",
            "indicator_name": "Indicator",
        },
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color=COLOR_TEXT_MUTED,
    )

    return _apply_chart_theme(fig, height=420)


def chart_mom_qoq_growth_bar(df: pd.DataFrame) -> go.Figure:
    """Bar chart: MoM/QoQ growth."""
    grouped = (
        df.groupby(["date", "indicator_name"], as_index=False)
        .agg(mom_qoq_growth_pct=("mom_qoq_growth_pct", "mean"))
        .dropna(subset=["mom_qoq_growth_pct"])
        .sort_values("date")
    )

    if grouped.empty:
        fig = go.Figure()
        fig.update_layout(title="MoM / QoQ Growth")
        return _apply_chart_theme(fig, height=380)

    fig = px.bar(
        grouped,
        x="date",
        y="mom_qoq_growth_pct",
        color="indicator_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="MoM / QoQ Growth",
        labels={
            "date": "Date",
            "mom_qoq_growth_pct": "MoM / QoQ Growth (%)",
            "indicator_name": "Indicator",
        },
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color=COLOR_TEXT_MUTED,
    )

    return _apply_chart_theme(fig, height=380)


def chart_combined_value_yoy(df: pd.DataFrame) -> go.Figure:
    """Combined chart: cột value và đường YoY growth cho indicator đầu tiên."""
    indicators = sorted(df["indicator_name"].dropna().unique().tolist())

    if not indicators:
        fig = go.Figure()
        fig.update_layout(title="Value and YoY Growth")
        return _apply_chart_theme(fig, height=420)

    if len(indicators) == 1:
        selected_indicator = indicators[0]
    else:
        selected_indicator = st.selectbox(
            "Indicator cho biểu đồ Value + YoY",
            indicators,
            key="macro_combined_indicator",
        )

    chart_df = (
        df[df["indicator_name"] == selected_indicator]
        .groupby("date", as_index=False)
        .agg(
            value=("value", "mean"),
            yoy_growth_pct=("yoy_growth_pct", "mean"),
        )
        .sort_values("date")
    )

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=chart_df["date"],
            y=chart_df["value"],
            name="Value",
            marker_color=COLOR_ACCENT,
            yaxis="y",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["yoy_growth_pct"],
            name="YoY Growth (%)",
            mode="lines+markers",
            line=dict(color=COLOR_POSITIVE, width=2),
            yaxis="y2",
        )
    )

    fig.update_layout(
        title=f"Value and YoY Growth - {selected_indicator}",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Value", side="left"),
        yaxis2=dict(
            title="YoY Growth (%)",
            overlaying="y",
            side="right",
            color="#FFFFFF",
            gridcolor="rgba(255,255,255,0)",
        ),
    )

    return _apply_chart_theme(fig, height=440)


def chart_rolling_average(df: pd.DataFrame) -> go.Figure:
    """Line chart: rolling average để làm mượt CPI/lạm phát."""
    grouped = (
        df.groupby(["date", "indicator_name"], as_index=False)
        .agg(
            value=("value", "mean"),
            rolling_avg_3p=("rolling_avg_3p", "mean"),
            rolling_avg_12p=("rolling_avg_12p", "mean"),
        )
        .sort_values("date")
    )

    melted = grouped.melt(
        id_vars=["date", "indicator_name"],
        value_vars=["value", "rolling_avg_3p", "rolling_avg_12p"],
        var_name="metric",
        value_name="metric_value",
    )

    melted["series"] = melted["indicator_name"] + " - " + melted["metric"]

    fig = px.line(
        melted,
        x="date",
        y="metric_value",
        color="series",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Value and Rolling Average",
        labels={
            "date": "Date",
            "metric_value": "Value",
            "series": "Series",
        },
    )

    return _apply_chart_theme(fig, height=420)


# ====================================================================
# RENDER SECTIONS
# ====================================================================

def render_trend_section(df: pd.DataFrame) -> None:
    """Render trend và YoY."""
    st.markdown(
        '<div class="macro-section-title">Xu hướng chỉ tiêu và lạm phát YoY</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        _chart_card(
            chart_indicator_trend,
            df,
            key="macro_chart_indicator_trend_main",
        )

    with col2:
        _chart_card(
            chart_yoy_growth_bar,
            df,
            key="macro_chart_yoy_growth_main",
        )


def render_growth_section(df: pd.DataFrame) -> None:
    """Render growth ngắn hạn và combined chart."""
    st.markdown(
        '<div class="macro-section-title">Biến động ngắn hạn và phân tích kết hợp</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        _chart_card(
            chart_mom_qoq_growth_bar,
            df,
            key="macro_chart_mom_qoq_growth_main",
        )

    with col2:
        _chart_card(
            chart_combined_value_yoy,
            df,
            key="macro_chart_combined_value_yoy_main",
        )


def render_smoothing_section(df: pd.DataFrame) -> None:
    """Render rolling average, bỏ heatmap."""
    st.markdown(
        '<div class="macro-section-title">Làm mượt xu hướng</div>',
        unsafe_allow_html=True,
    )

    _chart_card(
        chart_rolling_average,
        df,
        key="macro_chart_rolling_average_main",
    )


# ====================================================================
# DRILL-DOWN
# ====================================================================

def render_drilldown_section(df: pd.DataFrame) -> None:
    """Drill-down theo Indicator -> Period Grain, không cần Unit."""
    st.markdown(
        '<div class="macro-section-title">Drill-down: Indicator → Period Grain</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.markdown('<div class="macro-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    indicators = sorted(df["indicator_name"].dropna().unique().tolist())

    if not indicators:
        st.markdown('<div class="macro-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có Indicator để drill-down.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="macro-card">', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        selected_indicator = st.selectbox(
            "Indicator",
            indicators,
            key="macro_drilldown_indicator",
        )

    indicator_df = df[df["indicator_name"] == selected_indicator].copy()
    period_grains = sorted(indicator_df["period_grain"].dropna().unique().tolist())

    if not period_grains:
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="macro-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có Period Grain cho Indicator đã chọn.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    with col2:
        selected_period_grain = st.selectbox(
            "Period Grain",
            period_grains,
            key="macro_drilldown_period_grain",
        )

    detail_df = indicator_df[
        indicator_df["period_grain"] == selected_period_grain
    ].copy()

    st.markdown("</div>", unsafe_allow_html=True)

    if detail_df.empty:
        st.markdown('<div class="macro-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có dữ liệu cho lựa chọn hiện tại.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    latest_row = detail_df.sort_values("date").tail(1).iloc[0]

    latest_date = latest_row["date"]
    latest_value = latest_row["value"]
    latest_yoy = latest_row["yoy_growth_pct"]
    volatility = detail_df["rolling_volatility_12p"].mean()

    st.markdown('<div class="macro-card">', unsafe_allow_html=True)

    kpi_cols = st.columns(6)

    drill_kpis = [
        ("Indicator", selected_indicator, ""),
        ("Period Grain", selected_period_grain, ""),
        (
            "Latest Date",
            latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A",
            "",
        ),
        ("Latest Value", _format_number(latest_value), ""),
        (
            "Latest YoY",
            _format_percent(latest_yoy),
            "kpi-positive" if pd.notna(latest_yoy) and latest_yoy >= 0 else "kpi-negative",
        ),
        ("12P Volatility", _format_percent(volatility), ""),
    ]

    for col, (label, value, css_class) in zip(kpi_cols, drill_kpis):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        '<div class="macro-section-title">Biểu đồ chi tiết Indicator đã chọn</div>',
        unsafe_allow_html=True,
    )

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        _chart_card(
            chart_indicator_trend,
            detail_df,
            key="macro_chart_indicator_trend_drilldown",
        )

    with col_chart2:
        _chart_card(
            chart_yoy_growth_bar,
            detail_df,
            key="macro_chart_yoy_growth_drilldown",
        )

    detail_cols = [
        "date",
        "year",
        "quarter",
        "month",
        "indicator_name",
        "unit_name",
        "period_grain",
        "value",
        "prev_value",
        "mom_qoq_change",
        "mom_qoq_growth_pct",
        "prev_year_value",
        "yoy_change",
        "yoy_growth_pct",
        "rolling_avg_3p",
        "rolling_avg_12p",
        "rolling_volatility_12p",
    ]

    existing_cols = [col for col in detail_cols if col in detail_df.columns]

    detail_table = (
        detail_df[existing_cols]
        .sort_values("date", ascending=False)
        .rename(
            columns={
                "date": "Date",
                "year": "Year",
                "quarter": "Quarter",
                "month": "Month",
                "indicator_name": "Indicator",
                "unit_name": "Unit",
                "period_grain": "Period Grain",
                "value": "Value",
                "prev_value": "Previous Value",
                "mom_qoq_change": "MoM/QoQ Change",
                "mom_qoq_growth_pct": "MoM/QoQ Growth (%)",
                "prev_year_value": "Previous Year Value",
                "yoy_change": "YoY Change",
                "yoy_growth_pct": "YoY Growth (%)",
                "rolling_avg_3p": "Rolling Avg 3P",
                "rolling_avg_12p": "Rolling Avg 12P",
                "rolling_volatility_12p": "Rolling Volatility 12P",
            }
        )
    )

    st.markdown(
        '<div class="macro-section-title">Bảng dữ liệu chi tiết</div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="macro-chart-wrapper">', unsafe_allow_html=True)

    st.dataframe(
        detail_table.style.format(
            {
                "Value": "{:,.4f}",
                "Previous Value": "{:,.4f}",
                "MoM/QoQ Change": "{:,.4f}",
                "MoM/QoQ Growth (%)": "{:.2f}",
                "Previous Year Value": "{:,.4f}",
                "YoY Change": "{:,.4f}",
                "YoY Growth (%)": "{:.2f}",
                "Rolling Avg 3P": "{:,.4f}",
                "Rolling Avg 12P": "{:,.4f}",
                "Rolling Volatility 12P": "{:.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("</div>", unsafe_allow_html=True)


# ====================================================================
# MAIN RENDER
# ====================================================================

def render_dashboard() -> None:
    """Render toàn bộ Macro Indicator Dashboard."""
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="macro-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold_gold.fact_macro_indicator.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    if filtered_df.empty:
        st.markdown('<div class="macro-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có dữ liệu phù hợp với bộ lọc hiện tại.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_growth_section(filtered_df)
    render_smoothing_section(filtered_df)
    render_drilldown_section(filtered_df)