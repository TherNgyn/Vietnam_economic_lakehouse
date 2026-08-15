"""
Crop Yield Performance Dashboard.

Module này chứa toàn bộ logic của dashboard Crop Yield:
    - Truy vấn dữ liệu từ Spark (Gold layer).
    - Join các bảng dimension (dim_time, dim_crop).
    - Áp dụng bộ lọc (filter) toàn cục, trong đó Crop Name phụ thuộc
      Crop Category.
    - Tính toán KPI.
    - Vẽ toàn bộ biểu đồ bằng Plotly.
    - Inject CSS đồng bộ hoàn toàn với GDP Growth Dashboard.

app.py và tabs/crop.py không được chứa logic xử lý dữ liệu;
toàn bộ nằm trong file này, được expose qua hàm `render_dashboard()`.
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

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
BLUE_SEQUENCE_PALETTE = [
    "#3FA9F5",
    "#5DADE2",
    "#85C1E9",
    "#AED6F1",
    "#D6EAF8",
]
GREEN_SEQUENCE_PALETTE = [
    "#2ECC71",
    "#58D68D",
    "#82E0AA",
    "#ABEBC6",
    "#D5F5E3",
]

CHART_PLOT_BG = "#090A44"
CHART_PAPER_BG = "#090A44"
CHART_FONT_COLOR = "#FFFFFF"
TITLE_FONT_COLOR = "#FFFFFF"

TERM_MAP = {
    "crop_category": "Nhóm cây trồng",
    "crop_name": "Cây trồng",
    "yield_value": "Sản lượng",
    "area": "Diện tích",
    "productivity": "Năng suất",
    "productivity_yoy_growth_rate": "Tăng trưởng năng suất theo năm",
    "productivity_share_pct": "Tỷ trọng năng suất cây trồng",
}


LABEL_MAP = {
    "dashboard_title": "Crop Yield Performance Dashboard - Trang Theo Dõi Năng Suất Cây Trồng",
    "dashboard_subtitle": "Theo dõi sản lượng, diện tích và năng suất theo Nhóm cây trồng / Cây trồng và Năm",

    # Filters
    "year": "Year - Năm",
    "crop_category": "Crop Category - Nhóm cây trồng",
    "crop_name": "Crop Name - Cây trồng",
    "yield_unit": "Yield Unit - Đơn vị sản lượng",
    "productivity_unit": "Productivity - Đơn vị năng suất",
    "area_unit": "Area Unit - Đơn vị diện tích",

    # KPI labels
    "total_yield": "Total Yield - <br>Tổng sản lượng",
    "total_area": "Total Area - <br>Tổng diện tích",
    "avg_productivity": "Avg Productivity - <br>Trung bình năng suất",
    "avg_yoy_growth": "Avg YoY Growth (%) - <br>Trung bình tăng trưởng năng suất theo năm",
    "top_productivity_crop": "Top Productivity Crop - <br>Cây trồng có năng suất cao nhất",
    "largest_productivity_share": "Largest Productivity Share (%) - <br>Tỷ trọng năng suất lớn nhất",

    # Chart titles
    "yield_trend": "Yield Trend - Xu hướng sản lượng và diện tích",
    "productivity_trend": "Productivity Trend - Xu hướng năng suất trung bình",
    "yield_by_category": "Yield by Category - Sản lượng theo nhóm cây trồng",
    "crop_share": "Crop Share - Tỷ trọng sản lượng theo nhóm cây trồng",
    "top10_yield": "Top 10 Yield - Top 10 cây trồng theo sản lượng",
    "top10_productivity": "Top 10 Productivity - Top 10 cây trồng theo năng suất",
    "crop_structure": "Crop Structure (Treemap) - Cấu trúc cây trồng theo sản lượng",
    "drilldown": "Drill-down: Crop Analysis - Phân tích chi tiết cây trồng",

    # Series / legend
    "yield_series": "Yield - Sản lượng",
    "area_series": "Area - Diện tích",
    "avg_productivity_series": "Average Productivity - Năng suất trung bình",

    # Axis labels
    "year_axis": "Year - Năm",
    "yield_axis": "Yield - Sản lượng",
    "area_axis": "Area - Diện tích",
    "productivity_axis": "Productivity - Năng suất",
    "crop_category_axis": "Crop Category - Nhóm cây trồng",
    "crop_name_axis": "Crop Name - Cây trồng",
    "growth_axis": "YoY Growth (%) - Tăng trưởng năm",
    "share_axis": "Share (%) - Tỷ trọng",

    # Drilldown
    "drilldown_select": "Crop Category - Chọn nhóm cây trồng để xem chi tiết cây trồng",
    "col_crop_name": "Crop Name - Cây trồng",
    "col_yield": "Yield - Sản lượng",
    "col_area": "Area - Diện tích",
    "col_productivity": "Productivity - Năng suất",
    "col_growth": "Growth (%) - Tăng trưởng",
    "col_share": "Yield Share (%) - Tỷ trọng sản lượng",
}

def inject_custom_css() -> None:
    """Inject CSS tuỳ chỉnh cho toàn bộ dashboard Crop Yield.

    Sử dụng cùng class name và token màu với GDP Growth Dashboard để
    đảm bảo giao diện đồng bộ hoàn toàn (font, khoảng cách section,
    card bo góc, shadow nhẹ, chart nền trắng).
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}
        .stSelectbox label,
        .stSelectbox label p {{
            color: #FFFFFF !important;
        }}

        .stSelectbox div[data-baseweb="select"] * {{
            color: #FFFFFF !important;
        }}

        .stSelectbox div[data-baseweb="select"] > div {{    
            background-color: #102B55 !important;
            border-color: #2C6FB8 !important;
        }}

        .stSelectbox div[data-baseweb="select"] input {{
            color: #FFFFFF !important;
        }}

        .stSelectbox div[data-baseweb="select"] input::placeholder {{
            color: #FFFFFF !important;
            opacity: 1 !important;
        }}

        .stMultiSelect label,
        .stMultiSelect label p {{
            color: #FFFFFF !important;
        }}

        .stMultiSelect div[data-baseweb="select"] * {{
            color: #FFFFFF !important;
        }}

        .stMultiSelect div[data-baseweb="select"] input {{
            color: #FFFFFF !important;
        }}

        .stMultiSelect div[data-baseweb="select"] > div {{
            background-color: #102B55 !important;
            border-color: #2C6FB8 !important;
        }}

        div[data-baseweb="popover"] {{
            background-color: #102B55 !important;
        }}

        div[data-baseweb="popover"] * {{
            color: #FFFFFF !important;
        }}

        ul[role="listbox"],
        li[role="option"] {{
            background-color: #102B55 !important;
            color: #FFFFFF !important;
        }}
        .crop-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .crop-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .crop-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .crop-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
        }}

        .crop-section-title {{
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
            font-size: 24px;
            font-weight: 700;
        }}

        .kpi-positive {{
            color: {COLOR_POSITIVE};
        }}

        .kpi-negative {{
            color: {COLOR_NEGATIVE};
        }}

        .crop-chart-wrapper {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 10px 14px 4px 14px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 16px;
        }}

        section[data-testid="stSidebar"] {{
            background-color: {COLOR_CARD};
            border-right: 1px solid {COLOR_BORDER};
        }}

        div[data-baseweb="select"] > div {{
            background-color: {COLOR_CARD};
            border-color: {COLOR_BORDER};
            color: {COLOR_TEXT};
        }}

        .stTabs [data-baseweb="tab-list"] {{
            gap: 6px;
        }}

        .stTabs [data-baseweb="tab"] {{
            background-color: {COLOR_CARD};
            border-radius: 10px 10px 0 0;
            color: {COLOR_TEXT_MUTED};
            border: 1px solid {COLOR_BORDER};
        }}

        .stTabs [aria-selected="true"] {{
            background-color: {COLOR_HEADER};
            color: #FFFFFF;
        }}

        .streamlit-expanderHeader {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 10px;
            color: {COLOR_TEXT};
        }}

        div[data-testid="stMetric"] {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 12px 14px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
        }}

        div[data-testid="stMetricLabel"] {{
            color: {COLOR_TEXT_MUTED};
        }}

        div[data-testid="stMetricValue"] {{
            color: {COLOR_TEXT};
        }}

        .empty-state {{
            color: {COLOR_TEXT_MUTED};
            text-align: center;
            padding: 40px 0;
            font-size: 15px;
        }}
        </style>
        """, unsafe_allow_html=True
    )



def render_header() -> None:
    """Render header chính của dashboard."""
    st.markdown(
        f"""
        <div class="crop-header">
            <h1>{LABEL_MAP['dashboard_title']}</h1>
            <p>{LABEL_MAP['dashboard_subtitle']}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ====================================================================
# DATA LOADING (SPARK)
# ====================================================================

# @st.cache_data(show_spinner="Đang tải dữ liệu Crop Yield...")
# def load_data() -> pd.DataFrame:
#     """Truy vấn dữ liệu Crop Yield từ Gold layer bằng Spark.

#     Thực hiện join giữa fact_crop_yield với dim_time và dim_crop.
#     Dữ liệu chỉ được convert sang Pandas ở bước cuối cùng (sau khi đã
#     join xong bằng Spark), phục vụ cho việc filter/vẽ biểu đồ phía
#     Streamlit.

#     Returns:
#         pd.DataFrame: Dữ liệu Crop Yield đã join đầy đủ dimension.
#     """
#     spark = get_spark_session()

#     fact: SparkDataFrame = spark.table("gold.fact_crop_yield")
#     dim_time: SparkDataFrame = spark.table("gold.dim_time")
#     dim_crop: SparkDataFrame = spark.table("gold.dim_crop")

#     df = (
#         fact.join(dim_time, on="time_key", how="left")
#         .join(dim_crop, on="crop_key", how="left")
#         .select(
#             dim_time["year"],
#             dim_time["quarter"],
#             dim_crop["crop_name"],
#             dim_crop["crop_category"],
#             fact["yield_unit"],
#             fact["productivity_unit"],
#             fact["area_unit"],
#             fact["area"],
#             fact["yield_value"],
#             fact["productivity"],
#             fact["area_pre_year"],
#             fact["yield_pre_year"],
#             fact["productivity_pre_year"],
#             fact["productivity_yoy_growth_rate"],
#             fact["productivity_share_pct"],
#         )
#     )

#     pdf = df.toPandas()
#     return pdf
@st.cache_data(show_spinner="Đang tải dữ liệu Crop Yield...")
def load_data() -> pd.DataFrame:
    """Truy vấn dữ liệu Crop Yield từ Gold Mart bằng Spark.

    Dashboard không tự tính toán lại các chỉ số.
    Các chỉ số như productivity_yoy_growth_rate, productivity_share_pct
    được lấy trực tiếp từ mart ở database gold_marts.

    Returns:
        pd.DataFrame: Dữ liệu Crop Yield đã join dimension, đúng schema
        phục vụ filter, KPI và biểu đồ hiện tại.
    """
    spark = get_spark_session()

    mart: SparkDataFrame = spark.table("gold_marts.mart_crop_metrics")
    dim_time: SparkDataFrame = spark.table("gold_gold.dim_time")
    dim_crop: SparkDataFrame = spark.table("gold_gold.dim_crop")
    
    df = (
        mart.alias("m")
        .join(
            dim_time.alias("t"),
            F.col("m.time_key") == F.col("t.time_key"),
            "left",
        )
        .join(
            dim_crop.alias("c"),
            F.col("m.crop_key") == F.col("c.crop_key"),
            "left",
        )
        .select(
            F.col("t.year").alias("year"),
            F.col("t.quarter").alias("quarter"),
            F.col("c.crop_name").alias("crop_name"),
            F.when(F.col("c.crop_category") == "ANNUAL", "Cây hàng năm")
                .when(F.col("c.crop_category") == "PERENNIAL", "Cây lâu năm")
                .when(F.col("c.crop_category") == "STAPLE", "Cây lương thực chủ yếu")
                .otherwise(F.col("c.crop_category"))
                .alias("crop_category"),

            F.col("m.yield_unit").alias("yield_unit"),
            F.col("m.productivity_unit").alias("productivity_unit"),
            F.col("m.area_unit").alias("area_unit"),

            F.col("m.area").alias("area"),
            F.col("m.yield_value").alias("yield_value"),
            F.col("m.productivity").alias("productivity"),

            F.col("m.area_pre_year").alias("area_pre_year"),
            F.col("m.yield_pre_year").alias("yield_pre_year"),
            F.col("m.productivity_pre_year").alias("productivity_pre_year"),

            F.col("m.productivity_yoy_growth_rate").alias("productivity_yoy_growth_rate"),
            F.col("m.productivity_share_pct").alias("productivity_share_pct"),
        )
    )

    pdf = df.toPandas()
    return pdf

# filter 
def get_filter_options(df: pd.DataFrame, selected_categories: list[Any] | None = None) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Crop Name phụ thuộc Crop Category: nếu có category được chọn,
    danh sách crop_name chỉ gồm các crop thuộc category đó.

    Args:
        df: DataFrame nguồn (chưa lọc).
        selected_categories: Danh sách Crop Category đã chọn (dùng để
            lọc phụ thuộc cho Crop Name). None hoặc rỗng nghĩa là
            chưa chọn category nào -> hiển thị tất cả crop_name.

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "crop_category": [],
            "crop_name": [],
            "yield_unit": [],
            "productivity_unit": [],
            "area_unit": [],
        }

    crop_name_pool = df
    if selected_categories:
        crop_name_pool = df[df["crop_category"].isin(selected_categories)]

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "crop_category": sorted(df["crop_category"].dropna().unique().tolist()),
        "crop_name": sorted(crop_name_pool["crop_name"].dropna().unique().tolist()),
        "yield_unit": sorted(df["yield_unit"].dropna().unique().tolist()),
        "productivity_unit": sorted(df["productivity_unit"].dropna().unique().tolist()),
        "area_unit": sorted(df["area_unit"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    crop_categories: list[Any],
    crop_names: list[Any],
    yield_units: list[Any],
    productivity_unit: list[Any],
    area_units: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        crop_categories: Danh sách Crop Category được chọn.
        crop_names: Danh sách Crop Name được chọn.
        yield_units: Danh sách đơn vị sản lượng được chọn.
        productivity_unit: Danh sách đơn vị năng suất được chọn.
        area_units: Danh sách đơn vị diện tích được chọn.

    Returns:
        pd.DataFrame: DataFrame đã lọc.
    """
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]
    if crop_categories:
        filtered = filtered[filtered["crop_category"].isin(crop_categories)]
    if crop_names:
        filtered = filtered[filtered["crop_name"].isin(crop_names)]
    if yield_units:
        filtered = filtered[filtered["yield_unit"].isin(yield_units)]
    if productivity_unit:
        filtered = filtered[filtered["productivity_unit"].isin(productivity_unit)]
    if area_units:
        filtered = filtered[filtered["area_unit"].isin(area_units)]

    return filtered


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render khu vực bộ lọc toàn cục và trả về DataFrame đã lọc."""
    base_options = get_filter_options(df)

    with st.container(border=True):
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            years = st.multiselect(
                LABEL_MAP["year"],
                base_options["year"],
                default=[],
                key="crop_filter_year",
            )

        with col2:
            crop_categories = st.multiselect(
                LABEL_MAP["crop_category"],
                base_options["crop_category"],
                default=[],
                key="crop_filter_category",
            )

        dependent_options = get_filter_options(
            df,
            selected_categories=crop_categories,
        )

        with col3:
            crop_names = st.multiselect(
                LABEL_MAP["crop_name"],
                dependent_options["crop_name"],
                default=[],
                key="crop_filter_name",
            )

        with col4:
            yield_units = st.multiselect(
                LABEL_MAP["yield_unit"],
                base_options["yield_unit"],
                default=[],
                key="crop_filter_yield_unit",
            )

        with col5:
            productivity_unit = st.multiselect(
                LABEL_MAP["productivity_unit"],
                base_options["productivity_unit"],
                default=[],
                key="crop_filter_productivity_unit",
            )

        with col6:
            area_units = st.multiselect(
                LABEL_MAP["area_unit"],
                base_options["area_unit"],
                default=[],
                key="crop_filter_area_unit",
            )

    return apply_filters(
        df,
        years,
        crop_categories,
        crop_names,
        yield_units,
        productivity_unit,
        area_units,
    )


def _format_number(value: float) -> str:
    """Format số lớn theo dạng rút gọn (K, M, B, T)."""
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
    """Format số dạng phần trăm với 2 chữ số thập phân."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"


def _kpi_card(label: str, value: str, css_class: str = "") -> str:
    """Tạo HTML cho một KPI card.

    Args:
        label: Tên KPI.
        value: Giá trị hiển thị (đã format).
        css_class: Class CSS bổ sung (vd: kpi-positive / kpi-negative).

    Returns:
        str: Chuỗi HTML của KPI card.
    """
    return f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value {css_class}">{value}</div>
        </div>
    """


def render_kpis(df: pd.DataFrame) -> None:
    """Render 6 KPI cards: Total Production, Total Area, Average Productivity,
    Average YoY Growth, Largest Crop Share, Top Producing Crop.

    Args:
        df: DataFrame đã được lọc theo filter hiện tại.
    """
    st.markdown('<div class="crop-section-title">Tổng quan KPI</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>', unsafe_allow_html=True)
        return

    total_yield = df["yield_value"].sum()
    total_area = df["area"].sum()
    avg_productivity = df["productivity"].mean()
    avg_yoy_growth = df["productivity_yoy_growth_rate"].mean()
    largest_share = df["productivity_share_pct"].mean()

    grouped = (
    df.groupby(["crop_category", "crop_name"], as_index=False)
        .agg(
            productivity=("productivity", "sum"),
            area=("area", "sum"),
            yield_value=("yield_value", "sum"),
            productivity_share_pct=("productivity_share_pct", "mean"),
        )
    )
    top_row = grouped.loc[grouped["productivity"].idxmax()]

    top_producing_crop = top_row["crop_name"]
    largest_share_top = top_row["productivity_share_pct"]
    cols = st.columns(6)
    kpi_data = [
        (
            LABEL_MAP["total_yield"],
            f'{_format_number(total_yield)} {df["yield_unit"].dropna().iloc[0] if not df["yield_unit"].dropna().empty else ""}',
            "",
        ),
        (
            LABEL_MAP["total_area"],
            f'{_format_number(total_area)} {df["area_unit"].dropna().iloc[0] if not df["area_unit"].dropna().empty else ""}',
            "",
        ),
        (
            LABEL_MAP["avg_productivity"],
            f'{_format_number(avg_productivity)} {df["productivity_unit"].dropna().iloc[0] if not df["productivity_unit"].dropna().empty else ""}',
            "",
        ),
        (
            LABEL_MAP["avg_yoy_growth"],
            _format_percent(avg_yoy_growth),
            "kpi-positive" if avg_yoy_growth >= 0 else "kpi-negative",
        ),
        (
            LABEL_MAP["top_productivity_crop"],
            top_producing_crop,
            "kpi-positive",
        ),
        (
            LABEL_MAP["largest_productivity_share"],
            _format_percent(largest_share_top),
            "",
        ),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)



def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    """Áp dụng theme nền tối đồng bộ cho mọi biểu đồ Plotly."""
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
    )
    return fig


def _empty_chart_placeholder(message: str = "Không có dữ liệu để hiển thị") -> None:
    """Hiển thị placeholder khi DataFrame rỗng."""
    st.markdown(f'<div class="empty-state">{message}</div>', unsafe_allow_html=True)


def chart_yield_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Yield (Yield Value) theo Year, kèm Area trên trục phụ."""
    grouped = (
        df.groupby("year", as_index=False)
        .agg(yield_value=("yield_value", "sum"), area=("area", "sum"))
        .sort_values("year")
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["year"],
            y=grouped["yield_value"],
            mode="lines+markers",
            name=LABEL_MAP["yield_series"],
            line=dict(color=COLOR_ACCENT, width=3),
            yaxis="y1",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["year"],
            y=grouped["area"],
            mode="lines+markers",
            name=LABEL_MAP["area_series"],
            line=dict(color=COLOR_POSITIVE, width=3, dash="dot"),
            yaxis="y2",
        )
    )
    fig.update_layout(
        title=LABEL_MAP["yield_trend"],
        xaxis=dict(title=LABEL_MAP["year_axis"]),
        yaxis=dict(title=LABEL_MAP["yield_axis"]),
        yaxis2=dict(title=LABEL_MAP["area_axis"], overlaying="y", side="right", showgrid=False),
    )
    return _apply_chart_theme(fig)


def chart_productivity_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Average Productivity theo Year."""
    grouped = (
        df[df["productivity"] != 0]
        .groupby("year", as_index=False)
        .agg(productivity=("productivity", "mean"))
        .sort_values("year")
    )
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["year"],
            y=grouped["productivity"],
            mode="lines+markers",
            name=LABEL_MAP["avg_productivity_series"],
            line=dict(color=COLOR_ACCENT, width=3),
        )
    )
    fig.update_layout(
        title=LABEL_MAP["productivity_trend"],
        xaxis=dict(title=LABEL_MAP["year_axis"]),
        yaxis=dict(title=LABEL_MAP["productivity_axis"]),
        )
    return _apply_chart_theme(fig)


def chart_yield_by_category(df: pd.DataFrame) -> go.Figure:
    """Vẽ Stacked Bar: Yield theo Year, group theo Crop Category."""
    grouped = (
        df.groupby(["year", "crop_category"], as_index=False)
        .agg(yield_value=("yield_value", "sum"))
        .sort_values("year")
    )

    fig = px.bar(
        grouped,
        x="year",
        y="yield_value",
        color="crop_category",
        barmode="stack",
        color_discrete_sequence=DISCRETE_PALETTE,
        title=LABEL_MAP["yield_by_category"],
        labels={
            "year": LABEL_MAP["year_axis"],
            "yield_value": LABEL_MAP["yield_axis"],
            "crop_category": LABEL_MAP["crop_category_axis"],
        },
    )
    return _apply_chart_theme(fig)


def chart_crop_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Donut Chart: Crop Share (Yield Share) theo Crop Category."""
    grouped = (
    df.groupby("crop_category", as_index=False)
    .agg(
        yield_value=("yield_value", "sum"),
        yield_unit=("yield_unit", "first"),
    )
    )

    fig = px.pie(
        grouped,
        names="crop_category",
        values="yield_value",
        hole=0.55,
        color_discrete_sequence=DISCRETE_PALETTE,
        title=LABEL_MAP["crop_share"],
        custom_data=["yield_unit"],
    )

    fig.update_traces(
        hovertemplate="""
    <b>%{label}</b><br>
    Sản lượng: %{value:,.0f} %{customdata[0]}<br>
    Tỷ trọng: %{percent}<extra></extra>
    """
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _apply_chart_theme(fig)


def chart_top10_yield(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Crop theo Yield (Yield Value)."""
    grouped = (
        df.groupby("crop_name", as_index=False)
        .agg(
            yield_value=("yield_value", "sum"),
            yield_unit=("yield_unit", "first"),
        )
        .sort_values("yield_value", ascending=False)
        .head(10)
        .sort_values("yield_value")
    )

    fig = px.bar(
        grouped,
        x="yield_value",
        y="crop_name",
        orientation="h",
        color="yield_value",
        color_continuous_scale="Blues",
        title=LABEL_MAP["top10_yield"],
        labels={
            "yield_value": LABEL_MAP["yield_axis"],
            "crop_name": LABEL_MAP["crop_name_axis"],
        },
        custom_data=["yield_unit"],
    )

    fig.update_traces(
        texttemplate="%{x:,.0f}",
        textposition="outside",
        hovertemplate="""
    <b>%{y}</b><br>
    sản lượng: %{x:,.0f} %{customdata[0]}
    <extra></extra>
    """
    )
    return _apply_chart_theme(fig)


def chart_top10_productivity(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 Crop theo Productivity."""
    grouped = (
        df.groupby("crop_name", as_index=False)
        .agg(
            productivity=("productivity", "mean"),
            productivity_unit=("productivity_unit", "first"),
        )
    )

    grouped["productivity"] = grouped["productivity"].round(2)

    grouped = (
        grouped.sort_values("productivity", ascending=False)
        .head(10)
        .sort_values("productivity")
    )

    fig = px.bar(
        grouped,
        x="productivity",
        y="crop_name",
        orientation="h",
        color="productivity",
        color_continuous_scale="Greens",
        custom_data=["productivity_unit"],
        title=LABEL_MAP["top10_productivity"],
        labels={
            "productivity": LABEL_MAP["productivity_axis"],
            "crop_name": LABEL_MAP["crop_name_axis"],
        },
    )

    fig.update_coloraxes(showscale=False)

    fig.update_traces(
        texttemplate="%{x:.2f} %{customdata[0]}",
        textposition="outside",
        hovertemplate="""
    <b>%{y}</b><br>
    Productivity: %{x:.2f} %{customdata[0]}
    <extra></extra>
    """
    )
    return _apply_chart_theme(fig)


def chart_treemap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Treemap: Crop Category -> Crop Name (size = Yield)."""

    grouped = (
        df.groupby(
            ["crop_category", "crop_name"], as_index=False
        )
        .agg(
            productivity=("productivity", "mean"),
            area=("area", "sum"),
            yield_value=("yield_value", "sum"),
            productivity_unit=("productivity_unit", "first"),
            area_unit=("area_unit", "first"),
            yield_unit=("yield_unit", "first"),
        )
    )

    # Loại bỏ dữ liệu không hợp lệ
    grouped = grouped[grouped["yield_value"] > 0].copy()

    # Sắp xếp theo sản lượng giảm dần trong từng crop_category
    grouped = grouped.sort_values(
        ["crop_category", "yield_value"],
        ascending=[True, False],
        kind="stable",
    )

    # Tính tỷ trọng sản lượng trong từng crop_category
    grouped["yield_pct"] = (
        grouped["yield_value"]
        / grouped.groupby("crop_category")["yield_value"].transform("sum")
        * 100
    )

    fig = px.treemap(
        grouped,
        path=["crop_category", "crop_name"],
        values="yield_value",
        color="crop_category",
        color_continuous_scale="Set1",
        custom_data=[
            "yield_value",
            "yield_unit",
            "yield_pct",
            "productivity",
            "productivity_unit",
            "area",
            "area_unit",
        ],
        title=LABEL_MAP["crop_structure"],
    )

    fig.update_traces(
        texttemplate="<b>%{label}</b><br>%{customdata[2]:.1f}%",
        hovertemplate="""
    <b>%{label}</b><br>
    Crop Category: %{parent}<br><br>

    <b>Yield</b>: %{customdata[0]:,.2f} %{customdata[1]}<br>
    <b>Share in Category</b>: %{customdata[2]:.2f}%<br>
    <b>Productivity</b>: %{customdata[3]:,.2f} %{customdata[4]}<br>
    <b>Area</b>: %{customdata[5]:,.2f} %{customdata[6]}<br>

    <extra></extra>
    """
    )

    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Yield Share (%)"
        ),
        margin=dict(t=40, l=10, r=10, b=10),
    )
    return _apply_chart_theme(fig, height=480)


def chart_current_vs_previous_year(df: pd.DataFrame) -> go.Figure:
    """Vẽ Grouped Bar: Current vs Previous Year cho Area, Yield, Productivity.

    Mỗi metric được chuẩn hoá theo % so với giá trị lớn nhất của chính
    metric đó để có thể so sánh trực quan trên cùng một trục, do các
    metric (Area, Yield, Productivity) có đơn vị và độ lớn khác
    nhau.
    """
    current = {
        "Area": df["area"].sum(),
        "Yield": df["yield_value"].sum(),
        "Productivity": df["productivity"].mean(),
    }
    previous = {
        "Area": df["area_pre_year"].sum(),
        "Yield": df["yield_pre_year"].sum(),
        "Productivity": df["productivity_pre_year"].mean(),
    }

    metrics = list(current.keys())

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=metrics,
            y=[current[m] for m in metrics],
            name="Current Year",
            marker_color=COLOR_ACCENT,
        )
    )
    fig.add_trace(
        go.Bar(
            x=metrics,
            y=[previous[m] for m in metrics],
            name="Previous Year",
            marker_color=COLOR_TEXT_MUTED,
        )
    )
    fig.update_layout(title="Current vs Previous Year", barmode="group")
    return _apply_chart_theme(fig)


# ====================================================================
# RENDER SECTIONS
# ====================================================================

def _chart_card(render_fn: Callable[[pd.DataFrame], go.Figure], df: pd.DataFrame) -> None:
    """Wrapper render một biểu đồ trong khung card, xử lý trường hợp rỗng."""
    with st.container(border=True):
        if df.empty:
            _empty_chart_placeholder()
        else:
            fig = render_fn(df)
            st.plotly_chart(fig, use_container_width=True)


def render_trend_section(df: pd.DataFrame) -> None:
    """Render Row 1: Yield Trend (Line) & Productivity Trend (Line)."""
    st.markdown('<div class="crop-section-title">Xu hướng Năng suất & Năng suất</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_yield_trend, df)
    with col2:
        _chart_card(chart_productivity_trend, df)


def render_structure_section(df: pd.DataFrame) -> None:
    """Render Row 2 (Yield by Category, Crop Share) và Row 4 (Treemap)."""
    st.markdown('<div class="crop-section-title">Cơ cấu Năng suất theo </div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_yield_by_category, df)
    with col2:
        _chart_card(chart_crop_share, df)

    st.markdown('<div class="crop-section-title">Cấu trúc Crop (Treemap)</div>', unsafe_allow_html=True)
    _chart_card(chart_treemap, df)


def render_ranking_section(df: pd.DataFrame) -> None:
    """Render Row 3: Top 10 Yield & Top 10 Productivity (Horizontal Bar)."""
    st.markdown('<div class="crop-section-title">Bảng xếp hạng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_top10_yield, df)
    with col2:
        _chart_card(chart_top10_productivity, df)



def render_comparison_section(df: pd.DataFrame) -> None:
    """Render Row 6: Current vs Previous Year (Grouped Bar)."""
    st.markdown('<div class="crop-section-title">So sánh Năm hiện tại & Năm trước</div>', unsafe_allow_html=True)
    _chart_card(chart_current_vs_previous_year, df)


def render_drilldown_section(df: pd.DataFrame) -> None:
    """Render Row 7: Crop Drill-down theo Crop Category được chọn.

    Hiển thị bảng chi tiết theo Crop Name gồm Yield, Area,
    Productivity, Growth, Yield Share.
    """
    st.markdown(
        '<div class="crop-section-title">Drill-down: Phân tích cây trồng theo nhóm</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        with st.container(border=True):
            _empty_chart_placeholder()
        return

    categories = sorted(df["crop_category"].dropna().unique().tolist())

    if not categories:
        with st.container(border=True):
            _empty_chart_placeholder()
        return

    with st.container(border=True):
        selected_category = st.selectbox(
            LABEL_MAP["drilldown_select"],
            categories,
            key="crop_drilldown_category",
        )

    category_df = df[df["crop_category"] == selected_category]

    drilldown_table = (
        category_df.groupby("crop_name", as_index=False)
        .agg(
            yield_value=("yield_value", "sum"),
            area=("area", "sum"),
            productivity=("productivity", "mean"),
            productivity_yoy_growth_rate=("productivity_yoy_growth_rate", "mean"),
            productivity_share_pct=("productivity_share_pct", "mean"),
        )
        .sort_values("productivity_share_pct", ascending=False)
        .rename(
            columns={
                "crop_name": LABEL_MAP["col_crop_name"],
                "yield_value": LABEL_MAP["col_yield"],
                "area": LABEL_MAP["col_area"],
                "productivity": LABEL_MAP["col_productivity"],
                "productivity_yoy_growth_rate": LABEL_MAP["col_growth"],
                "productivity_share_pct": LABEL_MAP["col_share"],
            }
        )
    )

    with st.expander("Xem chi tiết bảng dữ liệu", expanded=True):
        with st.container(border=True):
            if drilldown_table.empty:
                _empty_chart_placeholder()
            else:
                st.dataframe(
                    drilldown_table.style.format(
                        {
                            LABEL_MAP["col_yield"]: "{:,.2f}",
                            LABEL_MAP["col_area"]: "{:,.2f}",
                            LABEL_MAP["col_productivity"]: "{:,.2f}",
                            LABEL_MAP["col_growth"]: "{:.2f}",
                            LABEL_MAP["col_share"]: "{:.2f}",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
        


# ====================================================================
# MAIN RENDER FUNCTION
# ====================================================================

def render_dashboard() -> None:
    """Render toàn bộ Crop Yield Performance Dashboard.

    Đây là entry point duy nhất được gọi từ tabs/crop.py. Hàm này
    điều phối toàn bộ flow: inject CSS -> load data -> filter ->
    KPI -> các section biểu đồ -> comparison -> drill-down.
    """
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        with st.container(border=True):
            _empty_chart_placeholder("Không thể tải dữ liệu từ gold.fact_crop_yield.")
        return

    filtered_df = render_filters(raw_df)

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_structure_section(filtered_df)
    render_ranking_section(filtered_df)

    render_drilldown_section(filtered_df)