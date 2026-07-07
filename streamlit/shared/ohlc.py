"""
OHLC Market Performance Dashboard.

Module này chứa toàn bộ logic của dashboard OHLC:
    - Truy vấn dữ liệu từ Spark Gold layer.
    - Join fact_ohlc với dim_time, dim_asset, dim_asset_class, dim_market.
    - Áp dụng bộ lọc:
        Year -> Month -> Date
        Asset Class -> Symbol
    - Tính toán KPI trực tiếp trên Pandas phục vụ dashboard.
    - Vẽ các biểu đồ chính:
        Candlestick, Volume, Close Trend, Daily Return, Drawdown.
    - Drill-down tách biệt 2 hierarchy:
        Date hierarchy: Year -> Month -> Date
        Asset hierarchy: Asset Class -> Symbol
    - Không dùng Market trong drill-down.
    - Inject CSS đồng bộ giao diện và chuyển chữ sang màu trắng.

app.py và tabs/ohlc.py không được chứa logic xử lý dữ liệu;
toàn bộ nằm trong file này, được expose qua hàm render_dashboard().
"""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pandas as pd
import plotly.express as px
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
# CSS INJECTION
# ====================================================================

def inject_custom_css() -> None:
    """Inject CSS tuỳ chỉnh cho toàn bộ dashboard OHLC."""
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: #FFFFFF;
        }}

        .ohlc-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .ohlc-header h1 {{
            color: #FFFFFF !important;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .ohlc-header p {{
            color: #FFFFFF !important;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .ohlc-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
            color: #FFFFFF !important;
        }}

        .ohlc-section-title {{
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

        .ohlc-chart-wrapper {{
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
        .stSelectbox label,
        .stTextInput label,
        .stDateInput label,
        .stNumberInput label {{
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
    """Render header chính của dashboard."""
    st.markdown(
        """
        <div class="ohlc-header">
            <h1>OHLC Market Performance Dashboard</h1>
            <p>Theo dõi dữ liệu thị trường theo Date hierarchy và Asset hierarchy</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING
# ====================================================================

@st.cache_data(show_spinner="Đang tải dữ liệu OHLC...")
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

    df = (
        fact.alias("f")
        .join(dim_time.alias("t"), F.col("f.time_key") == F.col("t.time_key"), "left")
        .join(dim_asset.alias("a"), F.col("f.asset_key") == F.col("a.asset_key"), "left")
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
        .select(
            F.col("t.full_date").alias("date"),
            F.col("t.year").alias("year"),
            F.col("t.quarter").alias("quarter"),
            F.col("t.month").alias("month"),
            F.col("ac.asset_class_name").alias("asset_class_name"),
            F.col("m.market_name").alias("market_name"),
            F.col("m.country").alias("country"),
            F.col("a.symbol").alias("symbol"),
            F.col("a.asset_name").alias("asset_name"),
            F.col("f.open_price").alias("open"),
            F.col("f.high_price").alias("high"),
            F.col("f.low_price").alias("low"),
            F.col("f.close_price").alias("close"),
            F.col("f.previous_close").alias("previous_close"),
            F.col("f.volume").alias("volume"),
        )
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

    pdf = pdf.dropna(subset=["date", "date_label", "symbol", "close"])
    pdf = pdf.sort_values(["symbol", "date"]).reset_index(drop=True)

    pdf["daily_return_pct"] = (
        pdf.groupby("symbol")["close"]
        .pct_change()
        .mul(100)
    )

    pdf["price_change"] = pdf["close"] - pdf["open"]

    pdf["price_change_pct"] = np.where(
        pdf["open"].notna() & (pdf["open"] != 0),
        (pdf["close"] - pdf["open"]) / pdf["open"] * 100,
        np.nan,
    )

    pdf["range_pct"] = np.where(
        pdf["open"].notna() & (pdf["open"] != 0),
        (pdf["high"] - pdf["low"]) / pdf["open"] * 100,
        np.nan,
    )

    pdf["rolling_volatility_20d"] = (
        pdf.groupby("symbol")["daily_return_pct"]
        .transform(
            lambda s: pd.to_numeric(s, errors="coerce")
            .astype("float64")
            .rolling(20, min_periods=5)
            .std()
        )
    )

    pdf["rolling_ma_20"] = (
        pdf.groupby("symbol")["close"]
        .transform(
            lambda s: pd.to_numeric(s, errors="coerce")
            .astype("float64")
            .rolling(20, min_periods=5)
            .mean()
        )
    )

    pdf["rolling_ma_50"] = (
        pdf.groupby("symbol")["close"]
        .transform(
            lambda s: pd.to_numeric(s, errors="coerce")
            .astype("float64")
            .rolling(50, min_periods=10)
            .mean()
        )
    )

    pdf["cummax_close"] = (
        pdf.groupby("symbol")["close"]
        .cummax()
    )

    pdf["drawdown_pct"] = np.where(
        pdf["cummax_close"].notna() & (pdf["cummax_close"] != 0),
        (pdf["close"] - pdf["cummax_close"]) / pdf["cummax_close"] * 100,
        np.nan,
    )

    return pdf


# ====================================================================
# FILTER OPTIONS & APPLY FILTERS
# ====================================================================

def get_filter_options(
    df: pd.DataFrame,
    selected_years: list[Any] | None = None,
    selected_months: list[Any] | None = None,
    selected_dates: list[Any] | None = None,
    selected_asset_classes: list[Any] | None = None,
) -> dict[str, list[Any]]:
    """
    Lấy option filter theo 2 hierarchy:
        Date hierarchy: Year -> Month -> Date
        Asset hierarchy: Asset Class -> Symbol
    """
    if df.empty:
        return {
            "year": [],
            "month": [],
            "date_label": [],
            "asset_class_name": [],
            "symbol": [],
        }

    date_pool = df.copy()

    if selected_years:
        date_pool = date_pool[date_pool["year"].isin(selected_years)]

    if selected_months:
        date_pool = date_pool[date_pool["month"].isin(selected_months)]

    if selected_dates:
        date_pool = date_pool[date_pool["date_label"].isin(selected_dates)]

    asset_pool = df.copy()

    if selected_asset_classes:
        asset_pool = asset_pool[asset_pool["asset_class_name"].isin(selected_asset_classes)]

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "month": sorted(date_pool["month"].dropna().unique().tolist()),
        "date_label": sorted(date_pool["date_label"].dropna().unique().tolist()),
        "asset_class_name": sorted(df["asset_class_name"].dropna().unique().tolist()),
        "symbol": sorted(asset_pool["symbol"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    months: list[Any],
    dates: list[Any],
    asset_classes: list[Any],
    symbols: list[Any],
) -> pd.DataFrame:
    """Áp dụng filter toàn cục."""
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]

    if months:
        filtered = filtered[filtered["month"].isin(months)]

    if dates:
        filtered = filtered[filtered["date_label"].isin(dates)]

    if asset_classes:
        filtered = filtered[filtered["asset_class_name"].isin(asset_classes)]

    if symbols:
        filtered = filtered[filtered["symbol"].isin(symbols)]

    return filtered


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Render filter hierarchy và trả về DataFrame đã lọc.

    Date hierarchy:
        Year -> Month -> Date

    Asset hierarchy:
        Asset Class -> Symbol
    """
    base_options = get_filter_options(df)

    st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)

    st.markdown(
        """
        <div style="color:#FFFFFF;font-weight:700;margin-bottom:10px;">
            Date Hierarchy: Year → Month → Date
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        years = st.multiselect(
            "Year",
            base_options["year"],
            default=[],
            key="ohlc_filter_year",
        )

    options_after_year = get_filter_options(
        df,
        selected_years=years,
    )

    with col2:
        months = st.multiselect(
            "Month",
            options_after_year["month"],
            default=[],
            key="ohlc_filter_month",
        )

    options_after_month = get_filter_options(
        df,
        selected_years=years,
        selected_months=months,
    )

    with col3:
        dates = st.multiselect(
            "Date",
            options_after_month["date_label"],
            default=[],
            key="ohlc_filter_date",
        )

    st.markdown(
        """
        <div style="color:#FFFFFF;font-weight:700;margin:18px 0 10px 0;">
            Asset Hierarchy: Asset Class → Symbol
        </div>
        """,
        unsafe_allow_html=True,
    )

    col4, col5 = st.columns(2)

    with col4:
        asset_classes = st.multiselect(
            "Asset Class",
            base_options["asset_class_name"],
            default=[],
            key="ohlc_filter_asset_class",
        )

    options_after_asset = get_filter_options(
        df,
        selected_asset_classes=asset_classes,
    )

    with col5:
        symbols = st.multiselect(
            "Symbol",
            options_after_asset["symbol"],
            default=[],
            key="ohlc_filter_symbol",
        )

    st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(
        df=df,
        years=years,
        months=months,
        dates=dates,
        asset_classes=asset_classes,
        symbols=symbols,
    )


# ====================================================================
# KPI HELPERS
# ====================================================================

def _format_number(value: float) -> str:
    """Format số lớn theo dạng rút gọn."""
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
    """Render KPI tổng quan cho OHLC."""
    st.markdown(
        '<div class="ohlc-section-title">Tổng quan KPI</div>',
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

    latest_close_avg = latest_df["close"].mean()
    avg_daily_return = df["daily_return_pct"].mean()
    avg_volatility = df["rolling_volatility_20d"].mean()
    max_drawdown = df["drawdown_pct"].min()
    total_volume = df["volume"].sum() if "volume" in df.columns else 0
    number_of_symbols = df["symbol"].nunique()

    return_class = "kpi-positive" if pd.notna(avg_daily_return) and avg_daily_return >= 0 else "kpi-negative"
    drawdown_class = "kpi-negative" if pd.notna(max_drawdown) and max_drawdown < 0 else "kpi-positive"

    cols = st.columns(6)

    kpi_data = [
        ("Latest Avg Close", _format_number(latest_close_avg), ""),
        ("Avg Daily Return", _format_percent(avg_daily_return), return_class),
        ("20D Volatility", _format_percent(avg_volatility), ""),
        ("Max Drawdown", _format_percent(max_drawdown), drawdown_class),
        ("Total Volume", _format_number(total_volume), ""),
        ("Symbols", f"{number_of_symbols}", ""),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)


# ====================================================================
# CHART STYLING HELPER
# ====================================================================

def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    """Áp dụng theme cho biểu đồ Plotly."""
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
    """Hiển thị placeholder khi không có dữ liệu."""
    st.markdown(
        f'<div class="empty-state">{message}</div>',
        unsafe_allow_html=True,
    )


def _chart_card(
    render_fn: Callable[[pd.DataFrame], go.Figure],
    df: pd.DataFrame,
    key: str,
) -> None:
    """Wrapper render biểu đồ trong card."""
    st.markdown('<div class="ohlc-chart-wrapper">', unsafe_allow_html=True)

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

def chart_candlestick(df: pd.DataFrame) -> go.Figure:
    """Candlestick Chart theo symbol đã filter."""
    available_symbols = sorted(df["symbol"].dropna().unique().tolist())

    if not available_symbols:
        fig = go.Figure()
        fig.update_layout(title="Candlestick")
        return _apply_chart_theme(fig, height=540)

    if len(available_symbols) == 1:
        selected_symbol = available_symbols[0]
    else:
        selected_symbol = st.selectbox(
            "Symbol cho biểu đồ nến",
            available_symbols,
            key="ohlc_candlestick_symbol",
        )

    candle_df = (
        df[df["symbol"] == selected_symbol]
        .dropna(subset=["date", "open", "high", "low", "close"])
        .sort_values("date")
    )

    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=candle_df["date"],
            open=candle_df["open"],
            high=candle_df["high"],
            low=candle_df["low"],
            close=candle_df["close"],
            name=selected_symbol,
            increasing_line_color=COLOR_POSITIVE,
            decreasing_line_color=COLOR_NEGATIVE,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=candle_df["date"],
            y=candle_df["rolling_ma_20"],
            mode="lines",
            name="MA20",
            line=dict(color=COLOR_ACCENT, width=1.6),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=candle_df["date"],
            y=candle_df["rolling_ma_50"],
            mode="lines",
            name="MA50",
            line=dict(color="#F5B041", width=1.6),
        )
    )

    fig.update_layout(
        title=f"Candlestick - {selected_symbol}",
        xaxis_rangeslider_visible=False,
        xaxis_title="Date",
        yaxis_title="Price",
    )

    return _apply_chart_theme(fig, height=560)


def chart_volume_trend(df: pd.DataFrame) -> go.Figure:
    """Bar Chart: volume theo thời gian."""
    grouped = (
        df.groupby(["date", "symbol"], as_index=False)
        .agg(volume=("volume", "sum"))
        .sort_values("date")
    )

    fig = px.bar(
        grouped,
        x="date",
        y="volume",
        color="symbol",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Volume Trend",
        labels={
            "date": "Date",
            "volume": "Volume",
            "symbol": "Symbol",
        },
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Volume",
    )

    return _apply_chart_theme(fig, height=380)


def chart_close_trend(df: pd.DataFrame) -> go.Figure:
    """Line Chart: xu hướng giá đóng cửa theo thời gian."""
    grouped = (
        df.groupby(["date", "symbol"], as_index=False)
        .agg(close=("close", "mean"))
        .sort_values("date")
    )

    fig = px.line(
        grouped,
        x="date",
        y="close",
        color="symbol",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Close Price Trend",
        labels={
            "date": "Date",
            "close": "Close",
            "symbol": "Symbol",
        },
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Close Price",
    )

    return _apply_chart_theme(fig, height=380)


def chart_return_trend(df: pd.DataFrame) -> go.Figure:
    """Line Chart: Daily Return %."""
    grouped = (
        df.groupby(["date", "symbol"], as_index=False)
        .agg(daily_return_pct=("daily_return_pct", "mean"))
        .sort_values("date")
    )

    fig = px.line(
        grouped,
        x="date",
        y="daily_return_pct",
        color="symbol",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Daily Return Trend",
        labels={
            "date": "Date",
            "daily_return_pct": "Daily Return (%)",
            "symbol": "Symbol",
        },
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color=COLOR_TEXT_MUTED,
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Daily Return (%)",
    )

    return _apply_chart_theme(fig, height=380)


def chart_drawdown(df: pd.DataFrame) -> go.Figure:
    """Line Chart: Drawdown theo thời gian."""
    grouped = (
        df.groupby(["date", "symbol"], as_index=False)
        .agg(drawdown_pct=("drawdown_pct", "mean"))
        .sort_values("date")
    )

    fig = px.line(
        grouped,
        x="date",
        y="drawdown_pct",
        color="symbol",
        color_discrete_sequence=DISCRETE_PALETTE,
        title="Drawdown Trend",
        labels={
            "date": "Date",
            "drawdown_pct": "Drawdown (%)",
            "symbol": "Symbol",
        },
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color=COLOR_TEXT_MUTED,
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Drawdown (%)",
    )

    return _apply_chart_theme(fig, height=380)


# ====================================================================
# RENDER CHART SECTIONS
# ====================================================================

def render_candlestick_section(df: pd.DataFrame) -> None:
    """Render biểu đồ nến."""
    st.markdown(
        '<div class="ohlc-section-title">Biểu đồ nến OHLC</div>',
        unsafe_allow_html=True,
    )
    _chart_card(
        chart_candlestick,
        df,
        key="ohlc_chart_candlestick_main",
    )


def render_volume_section(df: pd.DataFrame) -> None:
    """Render volume."""
    st.markdown(
        '<div class="ohlc-section-title">Khối lượng giao dịch</div>',
        unsafe_allow_html=True,
    )
    _chart_card(
        chart_volume_trend,
        df,
        key="ohlc_chart_volume_main",
    )


def render_trend_section(df: pd.DataFrame) -> None:
    """Render xu hướng giá đóng cửa và lợi suất."""
    st.markdown(
        '<div class="ohlc-section-title">Xu hướng giá và lợi suất</div>',
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        _chart_card(
            chart_close_trend,
            df,
            key="ohlc_chart_close_main",
        )

    with col2:
        _chart_card(
            chart_return_trend,
            df,
            key="ohlc_chart_return_main",
        )


def render_risk_section(df: pd.DataFrame) -> None:
    """Render drawdown."""
    st.markdown(
        '<div class="ohlc-section-title">Phân tích rủi ro</div>',
        unsafe_allow_html=True,
    )
    _chart_card(
        chart_drawdown,
        df,
        key="ohlc_chart_drawdown_main",
    )


# ====================================================================
# DRILL-DOWN SECTION
# ====================================================================

def _required_selectbox(
    label: str,
    options: list[Any],
    key: str,
    placeholder: str = "-- Chọn --",
) -> Any | None:
    """
    Selectbox bắt buộc chọn.
    Trả về None nếu chưa chọn hoặc không có option.
    """
    clean_options = [x for x in options if pd.notna(x)]

    if not clean_options:
        st.selectbox(
            label,
            [placeholder],
            index=0,
            key=key,
            disabled=True,
        )
        return None

    display_options = [placeholder] + clean_options

    selected = st.selectbox(
        label,
        display_options,
        index=0,
        key=key,
    )

    if selected == placeholder:
        return None

    return selected


def render_drilldown_section(df: pd.DataFrame) -> None:
    """
    Drill-down tách biệt 2 hierarchy:
        Date hierarchy: Year -> Month -> Date
        Asset hierarchy: Asset Class -> Symbol

    Không sử dụng Market trong drill-down.
    """
    st.markdown(
        '<div class="ohlc-section-title">Drill-down: Date Hierarchy và Asset Hierarchy</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có dữ liệu để drill-down.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)

    st.markdown(
        """
        <div style="color:#FFFFFF;font-weight:700;margin-bottom:10px;">
            Date Hierarchy: Year → Month → Date
        </div>
        """,
        unsafe_allow_html=True,
    )

    date_col1, date_col2, date_col3 = st.columns(3)

    with date_col1:
        years = sorted(df["year"].dropna().unique().tolist())
        selected_year = _required_selectbox(
            "Year",
            years,
            key="ohlc_drilldown_year",
        )

    if selected_year is None:
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Vui lòng chọn Year để bắt đầu drill-down.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    year_df = df[df["year"] == selected_year].copy()

    with date_col2:
        months = sorted(year_df["month"].dropna().unique().tolist())
        selected_month = _required_selectbox(
            "Month",
            months,
            key="ohlc_drilldown_month",
        )

    if selected_month is None:
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Vui lòng chọn Month để tiếp tục.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    month_df = year_df[year_df["month"] == selected_month].copy()

    with date_col3:
        dates = sorted(month_df["date_label"].dropna().unique().tolist())
        selected_date = _required_selectbox(
            "Date",
            dates,
            key="ohlc_drilldown_date",
        )

    if selected_date is None:
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Vui lòng chọn Date để tiếp tục.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    date_df = month_df[month_df["date_label"] == selected_date].copy()

    st.markdown(
        """
        <div style="color:#FFFFFF;font-weight:700;margin:18px 0 10px 0;">
            Asset Hierarchy: Asset Class → Symbol
        </div>
        """,
        unsafe_allow_html=True,
    )

    asset_col1, asset_col2 = st.columns(2)

    with asset_col1:
        asset_classes = sorted(df["asset_class_name"].dropna().unique().tolist())
        selected_asset_class = _required_selectbox(
            "Asset Class",
            asset_classes,
            key="ohlc_drilldown_asset_class",
        )

    if selected_asset_class is None:
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Vui lòng chọn Asset Class để tiếp tục.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    asset_df_all_dates = df[df["asset_class_name"] == selected_asset_class].copy()

    with asset_col2:
        symbols = sorted(asset_df_all_dates["symbol"].dropna().unique().tolist())
        selected_symbol = _required_selectbox(
            "Symbol",
            symbols,
            key="ohlc_drilldown_symbol",
        )

    st.markdown("</div>", unsafe_allow_html=True)

    if selected_symbol is None:
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Vui lòng chọn Symbol để xem chi tiết.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    selected_day_df = date_df[
        (date_df["asset_class_name"] == selected_asset_class)
        & (date_df["symbol"] == selected_symbol)
    ].copy()

    symbol_month_df = month_df[
        (month_df["asset_class_name"] == selected_asset_class)
        & (month_df["symbol"] == selected_symbol)
    ].copy().sort_values("date")

    symbol_year_df = year_df[
        (year_df["asset_class_name"] == selected_asset_class)
        & (year_df["symbol"] == selected_symbol)
    ].copy().sort_values("date")

    symbol_all_df = df[
        (df["asset_class_name"] == selected_asset_class)
        & (df["symbol"] == selected_symbol)
    ].copy().sort_values("date")

    if selected_day_df.empty:
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder(
            "Không có dữ liệu cho tổ hợp Date / Asset Class / Symbol đã chọn."
        )
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if symbol_month_df.empty:
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có dữ liệu theo tháng cho Symbol đã chọn.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    latest_row = selected_day_df.sort_values("date").tail(1).iloc[0]

    latest_date = latest_row["date"]
    latest_open = latest_row["open"]
    latest_high = latest_row["high"]
    latest_low = latest_row["low"]
    latest_close = latest_row["close"]
    daily_return = latest_row["daily_return_pct"]
    price_change_pct = latest_row["price_change_pct"]

    st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)

    kpi_cols = st.columns(8)

    drill_kpis = [
        ("Asset Class", selected_asset_class, ""),
        ("Symbol", selected_symbol, ""),
        (
            "Date",
            latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else "N/A",
            "",
        ),
        ("Open", _format_number(latest_open), ""),
        ("High", _format_number(latest_high), ""),
        ("Low", _format_number(latest_low), ""),
        ("Close", _format_number(latest_close), ""),
        (
            "Daily Return",
            _format_percent(daily_return),
            "kpi-positive" if pd.notna(daily_return) and daily_return >= 0 else "kpi-negative",
        ),
    ]

    for col, (label, value, css_class) in zip(kpi_cols, drill_kpis):
        with col:
            st.markdown(
                _kpi_card(label, value, css_class),
                unsafe_allow_html=True,
            )

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        '<div class="ohlc-section-title">Biểu đồ Symbol theo tháng đã chọn</div>',
        unsafe_allow_html=True,
    )

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        _chart_card(
            chart_candlestick,
            symbol_month_df,
            key="ohlc_drilldown_candlestick_month",
        )

    with col_chart2:
        _chart_card(
            chart_volume_trend,
            symbol_month_df,
            key="ohlc_drilldown_volume_month",
        )

    st.markdown(
        '<div class="ohlc-section-title">Xu hướng Symbol theo năm đã chọn</div>',
        unsafe_allow_html=True,
    )

    col_chart3, col_chart4 = st.columns(2)

    with col_chart3:
        _chart_card(
            chart_close_trend,
            symbol_year_df,
            key="ohlc_drilldown_close_year",
        )

    with col_chart4:
        _chart_card(
            chart_return_trend,
            symbol_year_df,
            key="ohlc_drilldown_return_year",
        )

    st.markdown(
        '<div class="ohlc-section-title">Drawdown của Symbol đã chọn</div>',
        unsafe_allow_html=True,
    )

    _chart_card(
        chart_drawdown,
        symbol_all_df,
        key="ohlc_drilldown_drawdown_symbol_all",
    )

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
        "open",
        "high",
        "low",
        "close",
        "previous_close",
        "volume",
        "daily_return_pct",
        "price_change",
        "price_change_pct",
        "range_pct",
        "rolling_volatility_20d",
        "drawdown_pct",
    ]

    existing_cols = [col for col in detail_cols if col in symbol_month_df.columns]

    detail_table = (
        symbol_month_df[existing_cols]
        .sort_values("date", ascending=False)
        .rename(
            columns={
                "date": "Date",
                "year": "Year",
                "quarter": "Quarter",
                "month": "Month",
                "asset_class_name": "Asset Class",
                "market_name": "Market",
                "country": "Country",
                "symbol": "Symbol",
                "asset_name": "Asset Name",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "previous_close": "Previous Close",
                "volume": "Volume",
                "daily_return_pct": "Daily Return (%)",
                "price_change": "Price Change",
                "price_change_pct": "Price Change (%)",
                "range_pct": "Range (%)",
                "rolling_volatility_20d": "20D Volatility (%)",
                "drawdown_pct": "Drawdown (%)",
            }
        )
    )

    st.markdown(
        '<div class="ohlc-section-title">Bảng dữ liệu chi tiết theo tháng đã chọn</div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="ohlc-chart-wrapper">', unsafe_allow_html=True)

    st.dataframe(
        detail_table.style.format(
            {
                "Open": "{:,.2f}",
                "High": "{:,.2f}",
                "Low": "{:,.2f}",
                "Close": "{:,.2f}",
                "Previous Close": "{:,.2f}",
                "Volume": "{:,.2f}",
                "Daily Return (%)": "{:.2f}",
                "Price Change": "{:,.2f}",
                "Price Change (%)": "{:.2f}",
                "Range (%)": "{:.2f}",
                "20D Volatility (%)": "{:.2f}",
                "Drawdown (%)": "{:.2f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("</div>", unsafe_allow_html=True)


# ====================================================================
# MAIN RENDER FUNCTION
# ====================================================================

def render_dashboard() -> None:
    """Render toàn bộ OHLC Market Performance Dashboard."""
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold_gold.fact_ohlc.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    if filtered_df.empty:
        st.markdown('<div class="ohlc-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không có dữ liệu phù hợp với bộ lọc hiện tại.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    render_kpis(filtered_df)
    render_candlestick_section(filtered_df)
    render_volume_section(filtered_df)
    render_trend_section(filtered_df)
    render_risk_section(filtered_df)
    render_drilldown_section(filtered_df)