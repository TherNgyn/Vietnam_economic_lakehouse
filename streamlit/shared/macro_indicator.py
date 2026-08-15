from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.spark import get_spark_session


COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#102B55"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_POSITIVE = "#2ECC71"
COLOR_NEGATIVE = "#E74C3C"
COLOR_TEXT = "#FFFFFF"
COLOR_TEXT_MUTED = "#FFFFFF"

CHART_PLOT_BG = "#090A44"
CHART_PAPER_BG = "#090A44"
CHART_FONT_COLOR = "#FFFFFF"
TITLE_FONT_COLOR = "#FFFFFF"

INDICATOR_LABEL_MAP = {
    "CPI_BASE_2000": "CPI Base 2000 - CPI gốc 2000",
    "CPI_BASE_2005": "CPI Base 2005 - CPI gốc 2005",
    "CPI_BASE_2010": "CPI Base 2010 - CPI gốc 2010",
    "CPI_BASE_PREV_YEAR": "CPI Previous Year Base - CPI gốc năm trước",
    "CPI_MOM": "CPI MoM - CPI thay đổi tháng",
    "INFLATION": "Inflation - Lạm phát",
    "POLICY_RATE": "Policy Rate - Lãi suất điều hành",
    "PPI_QOQ": "PPI QoQ - PPI thay đổi quý",
    "BROAD_MONEY": "Broad Money - Cung tiền mở rộng",
    "MONEY_SUPPLY": "Money Supply - Cung tiền",
}
LABEL_MAP = {
    "dashboard_title": "Macro Indicator Dashboard - Bảng Theo Dõi Chỉ Số Vĩ Mô",
    "dashboard_subtitle": "Theo dõi CPI, PPI, Broad Money và các chỉ số kinh tế vĩ mô theo thời gian",

    "year": "Year - Năm",
    "period_grain": "Period Grain - Kỳ dữ liệu",
    "indicator": "Indicator - Chỉ số",
    "unit": "Unit - Đơn vị",

    "latest_value": "Latest Value - Giá trị mới nhất",
    "previous_value": "Previous Value - Giá trị kỳ trước",
    "period_change": "Period Change - Thay đổi so với kỳ trước",
    "yoy_change": "YoY Change - Thay đổi so với cùng kỳ",
    "rolling_avg_3p": "Rolling Avg 3P - Trung bình 3 kỳ",
    "volatility_12p": "12P Volatility - Biến động 12 kỳ",

    "indicator_trend": "Indicator Value Trend - Xu hướng giá trị chỉ số",
    "change_trend": "Change Trend - Xu hướng thay đổi",
    "rolling_trend": "Rolling Average Trend - Xu hướng trung bình động",
    "combined_chart": "Value and YoY Change - Giá trị và thay đổi cùng kỳ",
    "detail_table": "Detail Table - Bảng dữ liệu chi tiết",

    "date_axis": "Date - Ngày",
    "value_axis": "Value - Giá trị",
    "change_axis": "Change - Mức thay đổi",
    "rolling_axis": "Rolling Value - Giá trị trung bình động",

    "empty_data": "Không có dữ liệu phù hợp với bộ lọc hiện tại.",
    "load_error": "Không thể tải dữ liệu từ gold_gold.fact_macro_indicator.",
}

def _indicator_label(value: Any) -> str:
    key = str(value).upper().strip()
    return INDICATOR_LABEL_MAP.get(key, key.replace("_", " ").title())
def inject_custom_css() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .macro-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .macro-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .macro-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .macro-section-title {{
            color: {COLOR_TEXT};
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
        }}

        .kpi-label {{
            color: {COLOR_TEXT_MUTED};
            font-size: 13px;
            font-weight: 500;
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
        }}

        .kpi-value {{
            color: {COLOR_TEXT};
            font-size: 22px;
            font-weight: 700;
        }}

        .kpi-positive {{
            color: {COLOR_POSITIVE};
        }}

        .kpi-negative {{
            color: {COLOR_NEGATIVE};
        }}

        .empty-state {{
            color: {COLOR_TEXT_MUTED};
            text-align: center;
            padding: 40px 0;
            font-size: 15px;
        }}

        .stSelectbox label,
        .stSelectbox label p,
        .stMultiSelect label,
        .stMultiSelect label p {{
            color: {COLOR_TEXT} !important;
            font-weight: 600 !important;
        }}

        .stSelectbox div[data-baseweb="select"] *,
        .stMultiSelect div[data-baseweb="select"] * {{
            color: {COLOR_TEXT} !important;
        }}

        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div {{
            background-color: {COLOR_CARD} !important;
            border-color: {COLOR_BORDER} !important;
        }}

        .stSelectbox div[data-baseweb="select"] input,
        .stMultiSelect div[data-baseweb="select"] input {{
            color: {COLOR_TEXT} !important;
        }}

        div[data-baseweb="popover"] {{
            background-color: {COLOR_CARD} !important;
        }}

        div[data-baseweb="popover"] * {{
            color: {COLOR_TEXT} !important;
        }}

        ul[role="listbox"],
        li[role="option"] {{
            background-color: {COLOR_CARD} !important;
            color: {COLOR_TEXT} !important;
        }}

        section[data-testid="stSidebar"] {{
            background-color: {COLOR_CARD};
            border-right: 1px solid {COLOR_BORDER};
        }}

        div[data-testid="stDataFrame"] {{
            color: {COLOR_TEXT} !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    st.markdown(
        f"""
        <div class="macro-header">
            <h1>{LABEL_MAP["dashboard_title"]}</h1>
            <p>{LABEL_MAP["dashboard_subtitle"]}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner="Đang tải dữ liệu Macro Indicator...")
def load_data() -> pd.DataFrame:
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold_gold.fact_macro_indicator")
    dim_time: SparkDataFrame = spark.table("gold_gold.dim_time")
    dim_indicator: SparkDataFrame = spark.table("gold_gold.dim_indicator")

    base_df = (
        fact.alias("f")
        .join(dim_time.alias("t"), F.col("f.time_key") == F.col("t.time_key"), "left")
        .join(dim_indicator.alias("i"), F.col("f.indicator_key") == F.col("i.indicator_key"), "left")
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
            & F.col("year").isNotNull()
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
        .withColumn("prev_value", F.lag("value", 1).over(w_ordered))
        .withColumn("period_change", F.col("value") - F.col("prev_value"))
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
            .otherwise(F.lag("value", 12).over(w_ordered)),
        )
        .withColumn("yoy_change", F.col("value") - F.col("prev_year_value"))
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

    final_df = (
        metric_df
        .withColumn("rolling_avg_3p", F.avg("value").over(w_3p))
        .withColumn("rolling_avg_12p", F.avg("value").over(w_12p))
        .withColumn("rolling_volatility_12p", F.stddev_samp("period_change").over(w_12p))
        .withColumn("date_label", F.date_format(F.col("date"), "yyyy-MM-dd"))
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
            "period_change",
            "prev_year_value",
            "yoy_change",
            "rolling_avg_3p",
            "rolling_avg_12p",
            "rolling_volatility_12p",
        )
        .orderBy("indicator_name", "period_grain", "unit_name", "date")
    )

    pdf = final_df.toPandas()

    if pdf.empty:
        return pdf

    pdf["date"] = pd.to_datetime(pdf["date"], errors="coerce")

    numeric_cols = [
        "value",
        "prev_value",
        "period_change",
        "prev_year_value",
        "yoy_change",
        "rolling_avg_3p",
        "rolling_avg_12p",
        "rolling_volatility_12p",
    ]

    for col in numeric_cols:
        pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

    pdf = pdf.dropna(
        subset=[
            "date",
            "indicator_name",
            "period_grain",
            "unit_name",
            "value",
        ]
    )

    pdf = pdf.sort_values(
        ["indicator_name", "period_grain", "unit_name", "date"]
    ).reset_index(drop=True)

    return pdf


def _default_indicator(options: list[Any]) -> Any | None:
    if not options:
        return None

    preferred_keywords = ["CPI", "INFLATION", "LẠM PHÁT"]

    for keyword in preferred_keywords:
        for item in options:
            if keyword in str(item).upper():
                return item

    return options[0]


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    indicator: Any,
    period_grain: Any,
    unit_name: Any,
) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]

    filtered = filtered[
        (filtered["indicator_name"] == indicator)
        & (filtered["period_grain"] == period_grain)
        & (filtered["unit_name"] == unit_name)
    ].copy()

    return filtered


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    with st.container(border=True):
        col1, col2, col3, col4 = st.columns(4)

        years = sorted(df["year"].dropna().unique().tolist())
        indicators = sorted(df["indicator_name"].dropna().unique().tolist())

        default_indicator = _default_indicator(indicators)
        default_indicator_index = indicators.index(default_indicator) if default_indicator in indicators else 0

        with col1:
            selected_years = st.multiselect(
                LABEL_MAP["year"],
                years,
                default=[],
                key="macro_filter_year",
            )

        with col2:
            selected_indicator = st.selectbox(
                LABEL_MAP["indicator"],
                indicators,
                index=default_indicator_index,
                key="macro_filter_indicator",
                format_func=_indicator_label,
            )

        period_pool = df[df["indicator_name"] == selected_indicator].copy()
        period_grains = sorted(period_pool["period_grain"].dropna().unique().tolist())

        with col3:
            selected_period_grain = st.selectbox(
                LABEL_MAP["period_grain"],
                period_grains,
                index=0,
                key="macro_filter_period_grain",
            )

        unit_pool = period_pool[period_pool["period_grain"] == selected_period_grain].copy()
        units = sorted(unit_pool["unit_name"].dropna().unique().tolist())

        with col4:
            selected_unit = st.selectbox(
                LABEL_MAP["unit"],
                units,
                index=0,
                key="macro_filter_unit",
            )

    return apply_filters(
        df=df,
        years=selected_years,
        indicator=selected_indicator,
        period_grain=selected_period_grain,
        unit_name=selected_unit,
    )


def _format_number(value: float) -> str:
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


def _kpi_card(label: str, value: str, css_class: str = "") -> str:
    return f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value {css_class}">{value}</div>
        </div>
    """


def render_kpis(df: pd.DataFrame) -> None:
    st.markdown(
        '<div class="macro-section-title">Tổng quan KPI - KPI Overview</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.markdown(
            '<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>',
            unsafe_allow_html=True,
        )
        return

    latest_row = df.sort_values("date").tail(1).iloc[0]

    latest_value = latest_row["value"]
    previous_value = latest_row["prev_value"]
    period_change = latest_row["period_change"]
    yoy_change = latest_row["yoy_change"]
    rolling_avg_3p = latest_row["rolling_avg_3p"]

    volatility_series = df["rolling_volatility_12p"].dropna().tail(1)
    latest_volatility = (
        volatility_series.iloc[0]
        if not volatility_series.empty
        else pd.NA
    )

    period_change_class = (
        "kpi-positive"
        if pd.notna(period_change) and period_change >= 0
        else "kpi-negative"
    )

    yoy_change_class = (
        "kpi-positive"
        if pd.notna(yoy_change) and yoy_change >= 0
        else "kpi-negative"
    )

    cols = st.columns(6)

    kpi_data = [
        (LABEL_MAP["latest_value"], _format_number(latest_value), ""),
        (LABEL_MAP["previous_value"], _format_number(previous_value), ""),
        (LABEL_MAP["period_change"], _format_number(period_change), period_change_class),
        (LABEL_MAP["yoy_change"], _format_number(yoy_change), yoy_change_class),
        (LABEL_MAP["rolling_avg_3p"], _format_number(rolling_avg_3p), ""),
        (LABEL_MAP["volatility_12p"], _format_number(latest_volatility), ""),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)


def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        plot_bgcolor=CHART_PLOT_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(color=CHART_FONT_COLOR, size=12),
        title=dict(
            font=dict(color=TITLE_FONT_COLOR, size=16),
            x=0,
            xanchor="left",
            y=0.99,
            yanchor="top",
        ),
        height=height,
        margin=dict(l=70, r=30, t=85, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color=CHART_FONT_COLOR),
        ),
        xaxis=dict(
            color=CHART_FONT_COLOR,
            gridcolor="rgba(255,255,255,0.15)",
        ),
        yaxis=dict(
            color=CHART_FONT_COLOR,
            gridcolor="rgba(255,255,255,0.15)",
        ),
    )
    return fig


def _empty_chart_placeholder(message: str = "Không có dữ liệu để hiển thị") -> None:
    st.markdown(
        f'<div class="empty-state">{message}</div>',
        unsafe_allow_html=True,
    )


def _chart_card(render_fn, df: pd.DataFrame) -> None:
    with st.container(border=True):
        if df.empty:
            _empty_chart_placeholder()
        else:
            fig = render_fn(df)
            st.plotly_chart(fig, use_container_width=True)


def _active_label(df: pd.DataFrame) -> tuple[str, str]:
    if df.empty:
        return "", ""

    indicator = df["indicator_name"].iloc[0]
    unit = df["unit_name"].iloc[0]

    return str(indicator), str(unit)


def chart_indicator_trend(df: pd.DataFrame) -> go.Figure:
    chart_df = df.sort_values("date").copy()
    indicator, unit = _active_label(chart_df)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["value"],
            mode="lines",
            name="Value - Giá trị",
            line=dict(color=COLOR_ACCENT, width=1.3),
        )
    )

    fig.update_layout(
        title=f"{LABEL_MAP['indicator_trend']} - {indicator}",
        xaxis_title=LABEL_MAP["date_axis"],
        yaxis_title=f"{LABEL_MAP['value_axis']} ({unit})",
    )

    return _apply_chart_theme(fig, height=420)


def chart_change_trend(df: pd.DataFrame) -> go.Figure:
    chart_df = df.sort_values("date").copy()
    indicator, unit = _active_label(chart_df)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["period_change"],
            mode="lines",
            name="Period Change - Thay đổi kỳ trước",
            line=dict(color=COLOR_ACCENT, width=1.3),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["yoy_change"],
            mode="lines",
            name="YoY Change - Thay đổi cùng kỳ",
            line=dict(color=COLOR_POSITIVE, width=1.3),
        )
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color=COLOR_TEXT_MUTED,
    )

    fig.update_layout(
        title=f"{LABEL_MAP['change_trend']} - {indicator}",
        xaxis_title=LABEL_MAP["date_axis"],
        yaxis_title=f"{LABEL_MAP['change_axis']} ({unit})",
    )

    return _apply_chart_theme(fig, height=420)


def chart_combined_value_yoy_change(df: pd.DataFrame) -> go.Figure:
    chart_df = df.sort_values("date").copy()
    indicator, unit = _active_label(chart_df)

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=chart_df["date"],
            y=chart_df["value"],
            name="Value - Giá trị",
            marker_color=COLOR_ACCENT,
            yaxis="y",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["yoy_change"],
            name="YoY Change - Thay đổi cùng kỳ",
            mode="lines",
            line=dict(color=COLOR_POSITIVE, width=1.3),
            yaxis="y2",
        )
    )

    fig.update_layout(
        title=f"{LABEL_MAP['combined_chart']} - {indicator}",
        xaxis=dict(title=LABEL_MAP["date_axis"]),
        yaxis=dict(title=f"{LABEL_MAP['value_axis']} ({unit})", side="left"),
        yaxis2=dict(
            title=f"{LABEL_MAP['change_axis']} ({unit})",
            overlaying="y",
            side="right",
            color=CHART_FONT_COLOR,
            gridcolor="rgba(255,255,255,0)",
        ),
    )

    return _apply_chart_theme(fig, height=430)


def chart_rolling_average(df: pd.DataFrame) -> go.Figure:
    chart_df = df.sort_values("date").copy()
    indicator, unit = _active_label(chart_df)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["value"],
            mode="lines",
            name="Value - Giá trị",
            line=dict(color=COLOR_ACCENT, width=1.2),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["rolling_avg_3p"],
            mode="lines",
            name="Rolling Avg 3P - Trung bình 3 kỳ",
            line=dict(color=COLOR_POSITIVE, width=1.3),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["rolling_avg_12p"],
            mode="lines",
            name="Rolling Avg 12P - Trung bình 12 kỳ",
            line=dict(color="#F5B041", width=1.3),
        )
    )

    fig.update_layout(
        title=f"{LABEL_MAP['rolling_trend']} - {indicator}",
        xaxis_title=LABEL_MAP["date_axis"],
        yaxis_title=f"{LABEL_MAP['value_axis']} ({unit})",
    )

    return _apply_chart_theme(fig, height=420)


def render_trend_section(df: pd.DataFrame) -> None:
    st.markdown(
        '<div class="macro-section-title">Xu hướng chỉ số - Indicator Trend</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        _chart_card(chart_indicator_trend, df)

    with col2:
        _chart_card(chart_change_trend, df)


def render_analysis_section(df: pd.DataFrame) -> None:
    st.markdown(
        '<div class="macro-section-title">Phân tích kết hợp - Combined Analysis</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        _chart_card(chart_combined_value_yoy_change, df)

    with col2:
        _chart_card(chart_rolling_average, df)


def render_detail_table(df: pd.DataFrame) -> None:
    st.markdown(
        '<div class="macro-section-title">Bảng dữ liệu chi tiết - Detail Table</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        with st.container(border=True):
            _empty_chart_placeholder()
        return

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
        "period_change",
        "prev_year_value",
        "yoy_change",
        "rolling_avg_3p",
        "rolling_avg_12p",
        "rolling_volatility_12p",
    ]

    existing_cols = [col for col in detail_cols if col in df.columns]

    detail_table = (
        df[existing_cols]
        .sort_values("date", ascending=False)
        .rename(
            columns={
                "date": "Date - Ngày",
                "year": "Year - Năm",
                "quarter": "Quarter - Quý",
                "month": "Month - Tháng",
                "indicator_name": "Indicator - Chỉ số",
                "unit_name": "Unit - Đơn vị",
                "period_grain": "Period Grain - Kỳ dữ liệu",
                "value": "Value - Giá trị",
                "prev_value": "Previous Value - Giá trị kỳ trước",
                "period_change": "Period Change - Thay đổi kỳ trước",
                "prev_year_value": "Previous Year Value - Giá trị cùng kỳ trước",
                "yoy_change": "YoY Change - Thay đổi cùng kỳ",
                "rolling_avg_3p": "Rolling Avg 3P - Trung bình 3 kỳ",
                "rolling_avg_12p": "Rolling Avg 12P - Trung bình 12 kỳ",
                "rolling_volatility_12p": "Rolling Volatility 12P - Biến động 12 kỳ",
            }
        )
    )

    with st.container(border=True):
        st.dataframe(
            detail_table.style.format(
                {
                    "Value - Giá trị": "{:,.4f}",
                    "Previous Value - Giá trị kỳ trước": "{:,.4f}",
                    "Period Change - Thay đổi kỳ trước": "{:,.4f}",
                    "Previous Year Value - Giá trị cùng kỳ trước": "{:,.4f}",
                    "YoY Change - Thay đổi cùng kỳ": "{:,.4f}",
                    "Rolling Avg 3P - Trung bình 3 kỳ": "{:,.4f}",
                    "Rolling Avg 12P - Trung bình 12 kỳ": "{:,.4f}",
                    "Rolling Volatility 12P - Biến động 12 kỳ": "{:,.4f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_dashboard() -> None:
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        with st.container(border=True):
            _empty_chart_placeholder(LABEL_MAP["load_error"])
        return

    filtered_df = render_filters(raw_df)

    if filtered_df.empty:
        with st.container(border=True):
            _empty_chart_placeholder(LABEL_MAP["empty_data"])
        return

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_analysis_section(filtered_df)
    render_detail_table(filtered_df)