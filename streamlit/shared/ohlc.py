from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

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
COLOR_TEXT_MUTED = "#FFFFFF"

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
TITLE_FONT_COLOR = "#FFFFFF"


# ====================================================================
# LABEL MAP
# ====================================================================

LABEL_MAP = {
    "dashboard_title": "OHLC Market Dashboard - Bảng Theo Dõi Thị Trường",
    "dashboard_subtitle": "Theo dõi giá Open / High / Low / Close của chỉ số chứng khoán, hàng hóa, tiền tệ và các tài sản khác theo thời gian",

    "year": "Year - Năm",
    "month": "Month - Tháng",
    "asset_class": "Asset Class - Nhóm tài sản",
    "symbol": "Symbol - Mã tài sản",

    "latest_close": "Latest Close - Giá đóng cửa mới nhất",
    "daily_return": "Daily Return - Lợi suất ngày",
    "max_drawdown": "Max Drawdown - Mức sụt giảm lớn nhất",
    "unit": "Unit - Đơn vị",
    "date_range": "Date Range - Khoảng thời gian",
    "records": "Records - Số dòng",

    "ohlc_charts": "OHLC Charts - Biểu đồ OHLC",
    "candlestick": "OHLC Candlestick - Biểu đồ nến OHLC",
    "close_trend": "Close Price Trend - Xu hướng giá đóng cửa",
    "drawdown": "Drawdown Trend - Xu hướng sụt giảm",
    "detail_table": "Detail Table - Bảng dữ liệu chi tiết",

    "date_axis": "Date - Ngày",
    "price_axis": "Price - Giá",
    "close_axis": "Close Price - Giá đóng cửa",
    "drawdown_axis": "Drawdown (%) - Mức sụt giảm (%)",

    "empty_data": "Không có dữ liệu phù hợp với bộ lọc hiện tại.",
    "load_error": "Không thể tải dữ liệu từ gold_gold.fact_ohlc.",
}

def inject_custom_css() -> None:
    """
    Tạo giao diện đồng bộ tone màu tối, card bo góc, border xanh dương,
    chữ trắng và đồng bộ với dashboard GDP.
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .ohlc-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .ohlc-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .ohlc-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .ohlc-section-title {{
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

        .stSelectbox div[data-baseweb="select"] input::placeholder,
        .stMultiSelect div[data-baseweb="select"] input::placeholder {{
            color: {COLOR_TEXT} !important;
            opacity: 1 !important;
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

        div[data-testid="stAlert"] {{
            color: {COLOR_TEXT} !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    """Render header chính của dashboard."""
    st.markdown(
        f"""
        <div class="ohlc-header">
            <h1>{LABEL_MAP["dashboard_title"]}</h1>
            <p>{LABEL_MAP["dashboard_subtitle"]}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu OHLC từ Gold layer...")
def load_data() -> pd.DataFrame:
    """
    Truy vấn dữ liệu OHLC từ Gold layer bằng Spark.

    Returns:
        pd.DataFrame: Dữ liệu OHLC đã join đầy đủ dimension.
    """
    spark = get_spark_session()

    fact: SparkDataFrame = spark.table("gold_gold.fact_ohlc")
    dim_time: SparkDataFrame = spark.table("gold_gold.dim_time")
    dim_asset: SparkDataFrame = spark.table("gold_gold.dim_asset")
    dim_asset_class: SparkDataFrame = spark.table("gold_gold.dim_asset_class")
    dim_market: SparkDataFrame = spark.table("gold_gold.dim_market")

    select_cols = [
        F.col("t.full_date").alias("date"),
        F.col("t.year").alias("year"),
        F.col("t.quarter").alias("quarter"),
        F.col("t.month").alias("month"),
        F.col("ac.asset_class_name").alias("asset_class_name"),
        F.col("m.market_name").alias("market_name"),
        F.col("m.country").alias("country"),
        F.col("a.symbol").alias("symbol"),
        F.col("a.asset_name").alias("asset_name"),
        F.col("f.unit_name").alias("unit_name"),
        F.col("f.open_price").alias("open"),
        F.col("f.high_price").alias("high"),
        F.col("f.low_price").alias("low"),
        F.col("f.close_price").alias("close"),
        F.col("f.previous_close").alias("previous_close"),
        F.col("f.volume").alias("volume"),
    ]

    if "source_name" in fact.columns:
        select_cols.append(F.col("f.source_name").alias("source_name"))

    df = (
        fact.alias("f")
        .join(
            dim_time.alias("t"),
            F.col("f.time_key") == F.col("t.time_key"),
            "left",
        )
        .join(
            dim_asset.alias("a"),
            F.col("f.asset_key") == F.col("a.asset_key"),
            "left",
        )
        .join(
            dim_asset_class.alias("ac"),
            F.col("a.asset_class_key") == F.col("ac.asset_class_key"),
            "left",
        )
        .join(
            dim_market.alias("m"),
            F.col("a.market_key") == F.col("m.market_key"),
            "left",
        )
        .select(*select_cols)
    )

    pdf = df.toPandas()

    if pdf.empty:
        return pdf

    pdf["date"] = pd.to_datetime(pdf["date"], errors="coerce")
    pdf = pdf.dropna(subset=["date"])
    pdf["date_label"] = pdf["date"].dt.strftime("%Y-%m-%d")

    text_cols = [
        "symbol",
        "asset_name",
        "asset_class_name",
        "market_name",
        "country",
        "unit_name",
    ]

    for col in text_cols:
        if col in pdf.columns:
            pdf[col] = pdf[col].astype(str).str.upper().str.strip()

    pdf = pdf[
        ~pdf["symbol"].isin(["NAN", "NULL", "NONE", ""])
    ].copy()

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
    ]

    for col in numeric_cols:
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("float64")

    pdf = pdf.dropna(
        subset=[
            "date",
            "date_label",
            "symbol",
            "asset_class_name",
            "market_name",
            "close",
        ]
    )

    group_cols = ["asset_class_name", "market_name", "symbol"]

    pdf = pdf.sort_values(group_cols + ["date"]).reset_index(drop=True)

    pdf["daily_return_pct"] = (
        pdf.groupby(group_cols)["close"]
        .pct_change()
        .mul(100)
    )

    pdf["price_change"] = pdf["close"] - pdf["open"]

    pdf["price_change_pct"] = np.where(
        pdf["open"].notna() & (pdf["open"] != 0),
        (pdf["close"] - pdf["open"]) / pdf["open"] * 100,
        np.nan,
    )

    pdf["cummax_close"] = (
        pdf.groupby(group_cols)["close"]
        .cummax()
    )

    pdf["drawdown_pct"] = np.where(
        pdf["cummax_close"].notna() & (pdf["cummax_close"] != 0),
        (pdf["close"] - pdf["cummax_close"]) / pdf["cummax_close"] * 100,
        np.nan,
    )

    return pdf


# ====================================================================
# FILTERS
# ====================================================================

def _default_asset_class(options: list[Any]) -> Any | None:
    """Chọn nhóm tài sản mặc định khi dashboard mở lần đầu."""
    if not options:
        return None

    preferred_order = [
        "VIETNAM_INDEX",
        "INDEX",
        "COMMODITY",
        "CURRENCY",
    ]

    for item in preferred_order:
        if item in options:
            return item

    return options[0]


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Render filter toàn cục.

    Vì các OHLC asset có đơn vị khác nhau, dashboard chỉ cho chọn
    một Asset Class và một Symbol tại một thời điểm để tránh vẽ sai trục Y.
    """
    if df.empty:
        return df

    with st.container(border=True):
        col1, col2, col3, col4 = st.columns(4)

        years = sorted(df["year"].dropna().unique().tolist())
        months = sorted(df["month"].dropna().unique().tolist())
        asset_classes = sorted(df["asset_class_name"].dropna().unique().tolist())

        default_asset_class = _default_asset_class(asset_classes)
        default_asset_index = (
            asset_classes.index(default_asset_class)
            if default_asset_class in asset_classes
            else 0
        )

        with col1:
            selected_years = st.multiselect(
                LABEL_MAP["year"],
                years,
                default=[],
                key="ohlc_filter_year",
            )

        with col2:
            selected_months = st.multiselect(
                LABEL_MAP["month"],
                months,
                default=[],
                key="ohlc_filter_month",
            )

        with col3:
            selected_asset_class = st.selectbox(
                LABEL_MAP["asset_class"],
                asset_classes,
                index=default_asset_index,
                key="ohlc_filter_asset_class",
            )

        symbol_pool = df[
            df["asset_class_name"] == selected_asset_class
        ].copy()

        symbols = sorted(symbol_pool["symbol"].dropna().unique().tolist())

        with col4:
            selected_symbol = st.selectbox(
                LABEL_MAP["symbol"],
                symbols,
                index=0,
                key="ohlc_filter_symbol",
            )

    filtered = df.copy()

    if selected_years:
        filtered = filtered[filtered["year"].isin(selected_years)]

    if selected_months:
        filtered = filtered[filtered["month"].isin(selected_months)]

    filtered = filtered[
        (filtered["asset_class_name"] == selected_asset_class)
        & (filtered["symbol"] == selected_symbol)
    ].copy()

    return filtered


# ====================================================================
# KPI HELPERS
# ====================================================================

def _format_number(value: float) -> str:
    """Format số lớn theo dạng rút gọn K, M, B, T."""
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
    """Format số dạng phần trăm."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"


def _kpi_card(label: str, value: str, css_class: str = "") -> str:
    """Tạo HTML cho một KPI card."""
    return f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value {css_class}">{value}</div>
        </div>
    """


def render_kpis(df: pd.DataFrame) -> None:
    """Render KPI tổng quan cho OHLC."""
    st.markdown(
        '<div class="ohlc-section-title">Tổng quan OHLC - OHLC Overview</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.markdown(
            f'<div class="empty-state">{LABEL_MAP["empty_data"]}</div>',
            unsafe_allow_html=True,
        )
        return

    latest_row = df.sort_values("date").tail(1).iloc[0]

    latest_close = latest_row["close"]
    latest_return = latest_row["daily_return_pct"]
    max_drawdown = df["drawdown_pct"].min()
    unit_name = latest_row.get("unit_name", "N/A")

    min_date = df["date"].min()
    max_date = df["date"].max()
    date_range = f"{min_date:%Y-%m-%d} → {max_date:%Y-%m-%d}"

    return_class = (
        "kpi-positive"
        if pd.notna(latest_return) and latest_return >= 0
        else "kpi-negative"
    )

    drawdown_class = (
        "kpi-negative"
        if pd.notna(max_drawdown) and max_drawdown < 0
        else "kpi-positive"
    )

    cols = st.columns(6)

    kpi_data = [
        (LABEL_MAP["latest_close"], _format_number(latest_close), ""),
        (LABEL_MAP["daily_return"], _format_percent(latest_return), return_class),
        (LABEL_MAP["max_drawdown"], _format_percent(max_drawdown), drawdown_class),
        (LABEL_MAP["unit"], str(unit_name), ""),
        (LABEL_MAP["date_range"], date_range, ""),
        (LABEL_MAP["records"], f"{len(df):,}", ""),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(
                _kpi_card(label, value, css_class),
                unsafe_allow_html=True,
            )


# ====================================================================
# CHART HELPERS
# ====================================================================

def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    """Áp dụng theme cho biểu đồ Plotly."""
    fig.update_layout(
        plot_bgcolor=CHART_PLOT_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(color=CHART_FONT_COLOR, size=12),
        title=dict(
            font=dict(color=TITLE_FONT_COLOR, size=16),
            x=0,
            xanchor="left",
            y=0.98,
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
    """Hiển thị placeholder khi không có dữ liệu."""
    st.markdown(
        f'<div class="empty-state">{message}</div>',
        unsafe_allow_html=True,
    )


def _chart_card(render_fn, df: pd.DataFrame) -> None:
    """Wrapper render một biểu đồ trong st.container."""
    with st.container(border=True):
        if df.empty:
            _empty_chart_placeholder()
        else:
            fig = render_fn(df)
            st.plotly_chart(fig, use_container_width=True)


# ====================================================================
# CHARTS
# ====================================================================

def chart_candlestick(df: pd.DataFrame) -> go.Figure:
    """Vẽ biểu đồ nến OHLC cho một symbol."""
    chart_df = (
        df.dropna(subset=["date", "open", "high", "low", "close"])
        .sort_values("date")
        .copy()
    )

    symbol = chart_df["symbol"].iloc[0] if not chart_df.empty else ""
    unit_name = (
        chart_df["unit_name"].iloc[0]
        if "unit_name" in chart_df.columns and not chart_df.empty
        else ""
    )

    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=chart_df["date"],
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            name=symbol,
            increasing_line_color=COLOR_POSITIVE,
            decreasing_line_color=COLOR_NEGATIVE,
        )
    )

    fig.update_layout(
        title=f"{LABEL_MAP['candlestick']} - {symbol}",
        xaxis_title=LABEL_MAP["date_axis"],
        yaxis_title=f"{LABEL_MAP['price_axis']} ({unit_name})",
        xaxis_rangeslider_visible=False,
    )

    return _apply_chart_theme(fig, height=520)


def chart_close_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ đường giá đóng cửa, chỉ một symbol để tránh sai đơn vị."""
    chart_df = df.sort_values("date").copy()

    symbol = chart_df["symbol"].iloc[0] if not chart_df.empty else ""
    unit_name = (
        chart_df["unit_name"].iloc[0]
        if "unit_name" in chart_df.columns and not chart_df.empty
        else ""
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["close"],
            mode="lines",
            name="Close - Đóng cửa",
            line=dict(
                color=COLOR_ACCENT,
                width=1.3,
            ),
        )
    )

    fig.update_layout(
        title=f"{LABEL_MAP['close_trend']} - {symbol}",
        xaxis_title=LABEL_MAP["date_axis"],
        yaxis_title=f"{LABEL_MAP['close_axis']} ({unit_name})",
    )

    return _apply_chart_theme(fig, height=360)


def chart_drawdown(df: pd.DataFrame) -> go.Figure:
    """Vẽ Drawdown theo thời gian cho một symbol."""
    chart_df = df.sort_values("date").copy()

    symbol = chart_df["symbol"].iloc[0] if not chart_df.empty else ""

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=chart_df["date"],
            y=chart_df["drawdown_pct"],
            mode="lines",
            name="Drawdown - Sụt giảm",
            line=dict(
                color=COLOR_NEGATIVE,
                width=1.3,
            ),
            fill="tozeroy",
            fillcolor="rgba(231, 76, 60, 0.15)",
        )
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color=COLOR_TEXT_MUTED,
    )

    fig.update_layout(
        title=f"{LABEL_MAP['drawdown']} - {symbol}",
        xaxis_title=LABEL_MAP["date_axis"],
        yaxis_title=LABEL_MAP["drawdown_axis"],
    )

    return _apply_chart_theme(fig, height=360)


# ====================================================================
# RENDER SECTIONS
# ====================================================================

def render_chart_sections(df: pd.DataFrame) -> None:
    """Render nhóm biểu đồ OHLC."""
    st.markdown(
        '<div class="ohlc-section-title">Biểu đồ OHLC - OHLC Charts</div>',
        unsafe_allow_html=True,
    )

    _chart_card(chart_candlestick, df)

    col1, col2 = st.columns(2)

    with col1:
        _chart_card(chart_close_trend, df)

    with col2:
        _chart_card(chart_drawdown, df)


def render_detail_table(df: pd.DataFrame) -> None:
    """Render bảng dữ liệu chi tiết."""
    st.markdown(
        '<div class="ohlc-section-title">Bảng dữ liệu chi tiết - Detail Table</div>',
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
        "asset_class_name",
        "market_name",
        "country",
        "symbol",
        "asset_name",
        "unit_name",
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
        "daily_return_pct",
        "price_change",
        "price_change_pct",
        "drawdown_pct",
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
                "asset_class_name": "Asset Class - Nhóm tài sản",
                "market_name": "Market - Thị trường",
                "country": "Country - Quốc gia",
                "symbol": "Symbol - Mã",
                "asset_name": "Asset Name - Tên tài sản",
                "unit_name": "Unit - Đơn vị",
                "open": "Open - Mở cửa",
                "high": "High - Cao nhất",
                "low": "Low - Thấp nhất",
                "close": "Close - Đóng cửa",
                "previous_close": "Previous Close - Đóng cửa trước",
                "volume": "Volume - Khối lượng",
                "daily_return_pct": "Daily Return (%) - Lợi suất ngày",
                "price_change": "Price Change - Thay đổi giá",
                "price_change_pct": "Price Change (%) - Thay đổi giá (%)",
                "drawdown_pct": "Drawdown (%) - Sụt giảm (%)",
            }
        )
    )

    with st.container(border=True):
        st.dataframe(
            detail_table.style.format(
                {
                    "Open - Mở cửa": "{:,.2f}",
                    "High - Cao nhất": "{:,.2f}",
                    "Low - Thấp nhất": "{:,.2f}",
                    "Close - Đóng cửa": "{:,.2f}",
                    "Previous Close - Đóng cửa trước": "{:,.2f}",
                    "Volume - Khối lượng": "{:,.2f}",
                    "Daily Return (%) - Lợi suất ngày": "{:.2f}",
                    "Price Change - Thay đổi giá": "{:,.2f}",
                    "Price Change (%) - Thay đổi giá (%)": "{:.2f}",
                    "Drawdown (%) - Sụt giảm (%)": "{:.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


# ====================================================================
# MAIN RENDER FUNCTION
# ====================================================================

def render_dashboard() -> None:
    """
    Render toàn bộ OHLC Market Performance Dashboard.

    Flow:
        inject CSS
        render header
        load data
        filter
        KPI
        OHLC charts
        detail table
    """
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
    render_chart_sections(filtered_df)
    render_detail_table(filtered_df)