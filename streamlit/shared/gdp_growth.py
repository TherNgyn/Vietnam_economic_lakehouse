"""
GDP Growth Performance Dashboard.

Module này chứa toàn bộ logic của dashboard GDP Growth:
    - Truy vấn dữ liệu từ Spark (Gold layer).
    - Join các bảng dimension.
    - Áp dụng bộ lọc (filter) toàn cục.
    - Tính toán KPI.
    - Vẽ toàn bộ biểu đồ bằng Plotly.
    - Inject CSS để tạo giao diện chuyên nghiệp, đồng bộ tone màu
      với dashboard Sales Performance.

app.py và tabs/gdp.py không được chứa logic xử lý dữ liệu;
toàn bộ nằm trong file này, được expose qua hàm `render_dashboard()`.
"""

from __future__ import annotations

from typing import Any

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

CHART_PLOT_BG = "#090A44"
CHART_PAPER_BG = "#090A44"
CHART_FONT_COLOR = "#FFFFFF"
TITLE_FONT_COLOR = "#FFFFFF"

TERM_MAP = {
    "sector": "Khu vực kinh tế",
    "sub_sector": "Phân ngành",

    "market_value": "GDP theo giá hiện hành",
    "constant_value": "GDP theo giá so sánh năm 2010",

    "market_qoq_growth_rate": "Tăng trưởng quý theo giá hiện hành",
    "market_yoy_growth_rate": "Tăng trưởng năm theo giá hiện hành",
    "real_qoq_growth_rate": "Tăng trưởng quý theo giá so sánh 2010",
    "real_yoy_growth_rate": "Tăng trưởng năm theo giá so sánh 2010",

    "sector_share_pct": "Tỷ trọng phân ngành trong khu vực kinh tế",
    "gdp_share_pct": "Tỷ trọng phân ngành trong GDP",
}

LABEL_MAP = {
    "dashboard_title": "GDP Growth Performance Dashboard - Trang Theo Dõi Tăng Trưởng GDP",
    "dashboard_subtitle": "Theo dõi tăng trưởng GDP theo Khu vực kinh tế / Phân ngành và Năm / Quý",

    "year": "Year - Năm",
    "quarter": "Quarter - Quý",
    "sector": "Sector - Khu vực kinh tế",
    "sub_sector": "Sub-sector - Phân ngành",

    "market_gdp": "Market GDP - <br>GDP theo giá hiện hành (Tỷ đồng)",
    "real_gdp_2010": "Real GDP / 2010 GDP - <br>GDP theo giá so sánh năm 2010 (Tỷ đồng)",
    "avg_qoq_growth": "Avg QoQ Growth (%) - <br>Trung bình tăng trưởng GDP theo quý",
    "avg_yoy_growth": "Avg YoY Growth (%) - <br>Trung bình tăng trưởng GDP theo năm",
    "top_growing_sub_sector": "Top Growing Sub-sector - <br>Phân ngành tăng trưởng cao nhất",
    "gdp_share": "GDP Share (%) - <br>Tỷ trọng GDP đóng góp",

    "gdp_trend": "GDP Trend - Xu hướng tổng GDP theo quý",
    "growth_trend": "Growth Trend - Xu hướng tăng trưởng GDP theo quý",
    "gdp_by_sector": "GDP by Sector - GDP theo khu vực kinh tế",
    "sector_share": "Sector Share - Tỷ trọng GDP theo khu vực kinh tế",
    "top_10_growth": "Top 10 Growth (Real YoY) - Top 10 phân ngành có GDP so sánh tăng trưởng cao nhất",
    "top_gdp_share": "Top GDP Share - Top 10 phân ngành có tỷ trọng GDP cao nhất",
    "sector_structure": "Sector Structure (Treemap) - Cấu trúc GDP theo khu vực kinh tế và phân ngành",
    "drilldown": "Drill-down: Sub-sector Analysis - Phân tích chi tiết phân ngành",


    "market_gdp_series": "Market GDP - GDP theo giá hiện hành",
    "real_gdp_series": "Real GDP - GDP theo giá so sánh năm 2010",
    "market_qoq": "Market QoQ - Tăng trưởng quý theo giá hiện hành",
    "market_yoy": "Market YoY - Tăng trưởng năm theo giá hiện hành",

    "quarter_axis": "Quarter - Quý",
    "market_gdp_axis": "Market GDP - GDP theo giá hiện hành",
    "real_yoy_growth_axis": "Real YoY Growth (%) - Tăng trưởng GDP hàng năm theo giá so sánh",
    "gdp_share_axis": "GDP Share (%) - Tỷ trọng GDP",
    "sector_axis": "Sector - Khu vực kinh tế",
    "sub_sector_axis": "Sub-sector - Phân ngành",

    "col_sub_sector": "Sub-sector - Phân ngành",
    "col_market_gdp": "Market GDP - GDP hiện hành",
    "col_real_gdp": "Real GDP - GDP so sánh 2010",
    "col_yoy_growth": "YoY Growth (%) - Tăng trưởng năm",
    "col_qoq_growth": "QoQ Growth (%) - Tăng trưởng quý",
    "col_gdp_share": "GDP Share (%) - Tỷ trọng GDP",

    "drilldown_select": "Sector - Chọn khu vực kinh tế để xem chi tiết phân ngành",
}

def inject_custom_css() -> None:
    """
    Tạo giao diện đồng bộ tone màu tối (dark navy), card bo góc, có
    shadow nhẹ, border xanh dương, đồng nhất với dashboard Sales
    Performance.
    """
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .gdp-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            padding: 22px 28px;
            border-radius: 14px;
            margin-bottom: 22px;
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .gdp-header h1 {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .gdp-header p {{
            color: #E7F1FE;
            margin: 4px 0 0 0;
            font-size: 14px;
        }}

        .gdp-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
            margin-bottom: 14px;
        }}

        .gdp-section-title {{
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
        .stSelectbox label,
        .stSelectbox label p {{
            color: {COLOR_TEXT} !important;
        }}
        .stSelectbox div[data-baseweb="select"] * {{
            color: {COLOR_TEXT} !important;
        }}

        .stSelectbox div[data-baseweb="select"] > div {{
            background-color: {COLOR_CARD} !important;
            border-color: {COLOR_BORDER} !important;
        }}

        .stSelectbox div[data-baseweb="select"] input {{
            color: {COLOR_TEXT} !important;
        }}

        .stSelectbox div[data-baseweb="select"] input::placeholder {{
            color: {COLOR_TEXT} !important;
            opacity: 1 !important;
        }}
        .stMultiSelect label,
        .stMultiSelect label p {{
            color: {COLOR_TEXT} !important;
        }}
        .stMultiSelect div[data-baseweb="select"] * {{
            color: {COLOR_TEXT} !important;
        }}

        .stMultiSelect div[data-baseweb="select"] input {{
            color: {COLOR_TEXT} !important;
        }}

        .stMultiSelect div[data-baseweb="select"] > div {{
            background-color: {COLOR_CARD} !important;
            border-color: {COLOR_BORDER} !important;
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
        .gdp-chart-wrapper {{
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

        .empty-state {{
            color: {COLOR_TEXT_MUTED};
            text-align: center;
            padding: 40px 0;
            font-size: 15px;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header() -> None:
    """Render header chính của dashboard."""
    st.markdown(
        """
        <div class="gdp-header">
            <h1>GDP Growth Performance Dashboard - Trang Theo Dõi Tăng Trưởng GDP</h1>
            <p>Theo dõi tăng trưởng GDP theo Khu vực kinh tế / Phân ngành và Năm / Quý</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner="Đang tải dữ liệu GDP Growth...")
def load_data() -> pd.DataFrame:
    """Truy vấn dữ liệu GDP Growth từ Gold Mart bằng Spark.

    Dashboard không tự tính toán lại các chỉ số.
    Toàn bộ metric tính toán được lấy trực tiếp từ mart ở gold_marts.

    Returns:
        pd.DataFrame: Dữ liệu GDP Growth đã join đầy đủ dimension.
    """
    spark = get_spark_session()

    mart: SparkDataFrame = spark.table("gold_marts.mart_gdp_metrics")
    dim_time: SparkDataFrame = spark.table("gold_gold.dim_time")
    dim_sub_sector: SparkDataFrame = spark.table("gold_gold.dim_sub_sector")
    dim_sector: SparkDataFrame = spark.table("gold_gold.dim_sector")

    df = (
        mart.alias("m")
        .join(
            dim_time.alias("t"),
            F.col("m.time_key") == F.col("t.time_key"),
            "left",
        )
        .join(
            dim_sub_sector.alias("ss"),
            F.col("m.sub_sector_key") == F.col("ss.sub_sector_key"),
            "left",
        )
        .join(
            dim_sector.alias("s"),
            F.col("ss.sector_key") == F.col("s.sector_key"),
            "left",
        )
        .select(
            F.col("t.year").alias("year"),
            F.col("t.quarter").alias("quarter"),
            F.col("s.sector_name").alias("sector_name"),
            F.col("ss.sub_sector_name").alias("sub_sector_name"),

            F.col("m.unit").alias("unit"),

            F.col("m.market_value").alias("market_value"),
            F.col("m.constant_value").alias("constant_value"),

            F.col("m.market_value_pre_quarter").alias("market_value_pre_quarter"),
            F.col("m.market_value_pre_year").alias("market_value_pre_year"),
            F.col("m.constant_value_pre_quarter").alias("constant_value_pre_quarter"),
            F.col("m.constant_value_pre_year").alias("constant_value_pre_year"),

            F.col("m.market_qoq_growth_rate").alias("market_qoq_growth_rate"),
            F.col("m.market_yoy_growth_rate").alias("market_yoy_growth_rate"),
            F.col("m.real_qoq_growth_rate").alias("real_qoq_growth_rate"),
            F.col("m.real_yoy_growth_rate").alias("real_yoy_growth_rate"),
            F.col("m.implicit_price_deflator").alias("implicit_price_deflator"),
            F.col("m.sector_share_pct").alias("sector_share_pct"),
            F.col("m.gdp_share_pct").alias("gdp_share_pct"),
        )
    )

    df = df.withColumn(
        "quarter_label",
        F.concat(
            F.lit("Q"),
            F.col("quarter").cast("string"),
            F.lit(" "),
            F.col("year").cast("string"),
        ),
    )

    pdf = df.toPandas()
    return pdf
# @st.cache_data(show_spinner="Đang tải dữ liệu GDP Growth...")
# def load_data() -> pd.DataFrame:
#     """Truy vấn dữ liệu GDP Growth từ Gold layer bằng Spark.

#     Thực hiện join giữa fact_gdp_growth với dim_time, dim_sub_sector,
#     dim_sector. Dữ liệu chỉ được convert sang Pandas ở bước cuối cùng
#     (sau khi đã join xong bằng Spark), phục vụ cho việc filter/vẽ
#     biểu đồ phía Streamlit.

#     Returns:
#         pd.DataFrame: Dữ liệu GDP Growth đã join đầy đủ dimension.
#     """
#     spark = get_spark_session()

#     fact: SparkDataFrame = spark.table("gold.fact_gdp_growth")
#     dim_time: SparkDataFrame = spark.table("gold.dim_time")
#     dim_sub_sector: SparkDataFrame = spark.table("gold.dim_sub_sector")
#     dim_sector: SparkDataFrame = spark.table("gold.dim_sector")

#     df = (
#         fact.join(dim_time, on="time_key", how="left")
#         .join(dim_sub_sector, on="sub_sector_key", how="left")
#         .join(dim_sector, on="sector_key", how="left")
#         .select(
#             dim_time["year"],
#             dim_time["quarter"],
#             dim_sector["sector_name"],
#             dim_sub_sector["sub_sector_name"],
#             fact["unit"],
#             fact["market_value"],
#             fact["constant_value"],
#             fact["market_value_pre_quarter"],
#             fact["market_value_pre_year"],
#             fact["constant_value_pre_quarter"],
#             fact["constant_value_pre_year"],
#             fact["market_qoq_growth_rate"],
#             fact["market_yoy_growth_rate"],
#             fact["real_qoq_growth_rate"],
#             fact["real_yoy_growth_rate"],
#             fact["implicit_price_deflator"],
#             fact["sector_share_pct"],
#             fact["gdp_share_pct"],
#         )
#     )

#     df = df.withColumn(
#         "quarter_label",
#         F.concat(F.lit("Q"), F.col("quarter").cast("string"), F.lit(" "), F.col("year").cast("string")),
#     )

#     pdf = df.toPandas()
#     return pdf


def get_filter_options(df: pd.DataFrame) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Args:
        df: DataFrame nguồn (chưa lọc).

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "quarter": [],
            "sector": [],
            "sub_sector": [],
            "unit": [],
        }

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "quarter": sorted(df["quarter"].dropna().unique().tolist()),
        "sector": sorted(df["sector_name"].dropna().unique().tolist()),
        "sub_sector": sorted(df["sub_sector_name"].dropna().unique().tolist()),
        "unit": sorted(df["unit"].dropna().unique().tolist()),
    }
def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    quarters: list[Any],
    sectors: list[Any],
    sub_sectors: list[Any],
    units: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        quarters: Danh sách quý được chọn.
        sectors: Danh sách sector được chọn.
        sub_sectors: Danh sách sub-sector được chọn.
        units: Danh sách đơn vị được chọn.

    Returns:
        pd.DataFrame: DataFrame đã lọc.
    """
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]
    if quarters:
        filtered = filtered[filtered["quarter"].isin(quarters)]
    if sectors:
        filtered = filtered[filtered["sector_name"].isin(sectors)]
    if sub_sectors:
        filtered = filtered[filtered["sub_sector_name"].isin(sub_sectors)]
    if units:
        filtered = filtered[filtered["unit"].isin(units)]

    return filtered


def get_filter_options(df: pd.DataFrame) -> dict[str, list[Any]]:
    """Lấy danh sách giá trị duy nhất cho từng bộ lọc.

    Args:
        df: DataFrame nguồn (chưa lọc).

    Returns:
        dict: Mapping tên filter -> danh sách giá trị (đã sort).
    """
    if df.empty:
        return {
            "year": [],
            "quarter": [],
            "sector": [],
            "sub_sector": [],
            "unit": [],
        }

    return {
        "year": sorted(df["year"].dropna().unique().tolist()),
        "quarter": sorted(df["quarter"].dropna().unique().tolist()),
        "sector": sorted(df["sector_name"].dropna().unique().tolist()),
        "sub_sector": sorted(df["sub_sector_name"].dropna().unique().tolist()),
        "unit": sorted(df["unit"].dropna().unique().tolist()),
    }


def apply_filters(
    df: pd.DataFrame,
    years: list[Any],
    quarters: list[Any],
    sectors: list[Any],
    sub_sectors: list[Any],
) -> pd.DataFrame:
    """Áp dụng các bộ lọc toàn cục lên DataFrame.

    Nếu một filter để trống (danh sách rỗng) thì coi như chọn tất cả
    giá trị của cột đó.

    Args:
        df: DataFrame nguồn.
        years: Danh sách năm được chọn.
        quarters: Danh sách quý được chọn.
        sectors: Danh sách sector được chọn.
        sub_sectors: Danh sách sub-sector được chọn.
        units: Danh sách đơn vị được chọn.

    Returns:
        pd.DataFrame: DataFrame đã lọc.
    """
    if df.empty:
        return df

    filtered = df.copy()

    if years:
        filtered = filtered[filtered["year"].isin(years)]
    if quarters:
        filtered = filtered[filtered["quarter"].isin(quarters)]
    if sectors:
        filtered = filtered[filtered["sector_name"].isin(sectors)]
    if sub_sectors:
        filtered = filtered[filtered["sub_sector_name"].isin(sub_sectors)]

    return filtered

# filter
def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render khu vực bộ lọc toàn cục và trả về DataFrame đã lọc.

    Args:
        df: DataFrame gốc (chưa lọc).

    Returns:
        pd.DataFrame: DataFrame sau khi áp dụng filter người dùng chọn.
    """
    options = get_filter_options(df)
    with st.container(border=True):
    # st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            years = st.multiselect(LABEL_MAP["year"], options["year"], default=[])

        with col2:
            quarters = st.multiselect(LABEL_MAP["quarter"], options["quarter"], default=[])

        with col3:
            sectors = st.multiselect(LABEL_MAP["sector"], options["sector"], default=[])

        with col4:
            sub_sectors = st.multiselect(LABEL_MAP["sub_sector"], options["sub_sector"], default=[])
    

    # st.markdown("</div>", unsafe_allow_html=True)

    return apply_filters(df, years, quarters, sectors, sub_sectors)


# KPI HELPERS
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
    """Render 6 KPI cards: Market GDP, Real GDP, QoQ, YoY, GDP Share, Top Growing Sub-sector.

    Args:
        df: DataFrame đã được lọc theo filter hiện tại.
    """
    st.markdown('<div class="gdp-section-title">Tổng quan KPI</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="empty-state">Không có dữ liệu phù hợp với bộ lọc hiện tại.</div>', unsafe_allow_html=True)
        return

    market_gdp = df["market_value"].sum()
    real_gdp = df["constant_value"].sum()
    avg_qoq = df["market_qoq_growth_rate"].mean()
    avg_yoy = df["market_yoy_growth_rate"].mean()
    avg_gdp_share = df["gdp_share_pct"].mean()

    top_growing = "N/A"
    # if not df["real_yoy_growth_rate"].dropna().empty:
    #     top_row = df.loc[df["real_yoy_growth_rate"].mean().idxmax()]
    #     top_growing = f"{top_row['sub_sector_name']} ({top_row['real_yoy_growth_rate'].mean():.2f}%)"
    #     top_gdp_share = top_row["gdp_share_pct"].mean()
    if not df["real_yoy_growth_rate"].dropna().empty:
       summary = (
        df.groupby("sub_sector_name", as_index=False)
        .agg(
            real_yoy_growth_rate=("market_yoy_growth_rate", "mean"),
            market_value=("market_value", "sum")
        )
    )

    top_row = summary.loc[summary["real_yoy_growth_rate"].idxmax()]

    top_growing = (
        f"{top_row['sub_sector_name']} "
        f"({top_row['real_yoy_growth_rate']:.2f}%)"
    )

    top_gdp_share = (
        top_row["market_value"] / summary["market_value"].sum() * 100
        if summary["market_value"].sum() > 0
        else 0
    )
    qoq_class = "kpi-positive" if avg_qoq >= 0 else "kpi-negative"
    yoy_class = "kpi-positive" if avg_yoy >= 0 else "kpi-negative"

    cols = st.columns(6)
    kpi_data = [
        (LABEL_MAP["market_gdp"], _format_number(market_gdp), ""),
        (LABEL_MAP["real_gdp_2010"], _format_number(real_gdp), ""),
        (LABEL_MAP["avg_qoq_growth"], _format_percent(avg_qoq), qoq_class),
        (LABEL_MAP["avg_yoy_growth"], _format_percent(avg_yoy), yoy_class),
        (LABEL_MAP["top_growing_sub_sector"], top_growing, "kpi-positive"),
        (LABEL_MAP["gdp_share"], _format_percent(top_gdp_share), ""),
    ]

    for col, (label, value, css_class) in zip(cols, kpi_data):
        with col:
            st.markdown(_kpi_card(label, value, css_class), unsafe_allow_html=True)


def _apply_chart_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    """Áp dụng theme nền trắng đồng bộ cho mọi biểu đồ Plotly.

    Args:
        fig: Đối tượng Figure của Plotly.
        height: Chiều cao biểu đồ (px).

    Returns:
        go.Figure: Figure đã áp dụng theme.
    """
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


def chart_gdp_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart: Market GDP & Real GDP theo Quarter."""
    grouped = (
        df.groupby(["year", "quarter"], as_index=False)
        .agg(market_value=("market_value", "sum"), constant_value=("constant_value", "sum"))
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["market_value"],
            mode="lines+markers",
            name = LABEL_MAP["market_gdp_series"],
            line=dict(color=COLOR_ACCENT, width=1.5),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["quarter_label"],
            y=grouped["constant_value"],
            mode="lines+markers",
            name=LABEL_MAP["real_gdp_series"],
            line=dict(color=COLOR_POSITIVE, width=1.5),
        )
    )
    fig.update_layout(title=LABEL_MAP["gdp_trend"], yaxis_title=LABEL_MAP["market_gdp_axis"], xaxis_title=LABEL_MAP["quarter_axis"])
    return _apply_chart_theme(fig)


def chart_growth_trend(df: pd.DataFrame) -> go.Figure:
    """Vẽ Line Chart 4 đường: Market QoQ, Market YoY, Real QoQ, Real YoY."""
    grouped = (
        df.groupby(["year", "quarter"], as_index=False)
        .agg(
            market_qoq_growth_rate=("market_qoq_growth_rate", "mean"),
            market_yoy_growth_rate=("market_yoy_growth_rate", "mean"),
        )
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    series_config = [
        ("market_qoq_growth_rate", "Market QoQ", COLOR_ACCENT),
        ("market_yoy_growth_rate", "Market YoY", COLOR_POSITIVE),
    ]

    fig = go.Figure()
    for column, name, color in series_config:
        fig.add_trace(
            go.Scatter(
                x=grouped["quarter_label"],
                y=grouped[column],
                mode="lines+markers",
                name=name,
                line=dict(color=color, width=1.5),
            )
        )
    fig.update_layout(title=LABEL_MAP["growth_trend"], yaxis_title=LABEL_MAP["market_gdp_axis"], xaxis_title=LABEL_MAP["quarter_axis"])
    return _apply_chart_theme(fig)


def chart_gdp_by_sector(df: pd.DataFrame) -> go.Figure:
    """Vẽ Stacked Bar: Market GDP theo Quarter, group theo Sector."""
    grouped = (
        df.groupby(["year", "quarter", "sector_name"], as_index=False)
        .agg(market_value=("market_value", "sum"))
        .sort_values(["year", "quarter"])
    )
    grouped["quarter_label"] = "Q" + grouped["quarter"].astype(str) + " " + grouped["year"].astype(str)

    fig = px.bar(
        grouped,
        x="quarter_label",
        y="market_value",
        color="sector_name",
        barmode="stack",
        color_discrete_sequence=DISCRETE_PALETTE,
        title=LABEL_MAP["gdp_by_sector"],
        labels={"quarter_label": LABEL_MAP["quarter"], "market_value": LABEL_MAP["market_gdp"], "sector_name": LABEL_MAP["sector"]},
    )
    fig.update_layout(yaxis_title=LABEL_MAP["market_gdp_axis"], xaxis_title=LABEL_MAP["quarter_axis"])
    return _apply_chart_theme(fig)


def chart_sector_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Donut Chart: Sector Share."""
    grouped = df.groupby("sector_name", as_index=False).agg(market_value=("market_value", "sum"))

    fig = px.pie(
        grouped,
        names="sector_name",
        values="market_value",
        hole=0.40,
        color_discrete_sequence=DISCRETE_PALETTE,
        title=LABEL_MAP["sector_share"],
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return _apply_chart_theme(fig)


def chart_top10_growth(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 real_yoy_growth_rate."""
    grouped = (
        df.groupby("sub_sector_name", as_index=False)
        .agg(real_yoy_growth_rate=("real_yoy_growth_rate", "mean"))
        .sort_values("real_yoy_growth_rate", ascending=False)
        .head(10)
        .sort_values("real_yoy_growth_rate")
    )

    fig = px.bar(
        grouped,
        x="real_yoy_growth_rate",
        y="sub_sector_name",
        orientation="h",
        color="real_yoy_growth_rate",
        color_continuous_scale=[COLOR_NEGATIVE, COLOR_ACCENT, COLOR_POSITIVE],
        title=LABEL_MAP["top_10_growth"],
        labels={"real_yoy_growth_rate": "Real YoY Growth (%)", "sub_sector_name": LABEL_MAP["sub_sector"]},
    )
    fig.update_coloraxes(showscale=False)
    return _apply_chart_theme(fig)


def chart_top_gdp_share(df: pd.DataFrame) -> go.Figure:
    """Vẽ Horizontal Bar: Top 10 gdp_share_pct."""
    grouped = (
        df.groupby("sub_sector_name", as_index=False)
        .agg(gdp_share_pct=("gdp_share_pct", "mean"))
        .sort_values("gdp_share_pct", ascending=False)
        .head(10)
        .sort_values("gdp_share_pct")
    )

    fig = px.bar(
        grouped,
        x="gdp_share_pct",
        y="sub_sector_name",
        orientation="h",
        color_discrete_sequence=[COLOR_ACCENT],
        title=LABEL_MAP["top_gdp_share"],
        labels={"gdp_share_pct": "GDP Share (%)", "sub_sector_name": LABEL_MAP["sub_sector"]},
    )
    return _apply_chart_theme(fig)


def chart_treemap(df: pd.DataFrame) -> go.Figure:
    """Vẽ Treemap: Sector -> Sub-sector -> GDP Share."""
    grouped = (
        df.groupby(["sector_name", "sub_sector_name"], as_index=False)
        .agg(market_value=("market_value", "sum"))
    )

    grouped = grouped[grouped["market_value"] > 0]

    grouped["sector_total"] = grouped.groupby("sector_name")["market_value"].transform("sum")

    grouped["sector_share_pct"] = (
        grouped["market_value"] / grouped["sector_total"] * 100
    )

    fig = px.treemap(
        grouped,
        path=["sector_name", "sub_sector_name"],
        values="market_value",
        color="sector_name",
        color_discrete_sequence=DISCRETE_PALETTE,
        custom_data=["sector_share_pct"],
        title=LABEL_MAP["sector_structure"],
    )

    fig.update_traces(
        hovertemplate=
        "<b>%{label}</b><br>"
        "Market Value: %{value:,.0f}<br>"
        "Share in Sector: %{customdata[0]:.2f}%"
        "<extra></extra>"
    )
    return _apply_chart_theme(fig, height=560)



# ====================================================================
# RENDER SECTIONS
# ====================================================================

def _chart_card(render_fn, df: pd.DataFrame) -> None:
    """Wrapper render một biểu đồ trong khung card, xử lý trường hợp rỗng.

    Args:
        render_fn: Hàm tạo Figure (nhận DataFrame, trả về go.Figure).
        df: DataFrame đầu vào cho biểu đồ.
    """
    with st.container(border=True):
        if df.empty:
            _empty_chart_placeholder()
        else:
            fig = render_fn(df)
            st.plotly_chart(fig, use_container_width=True)


def render_trend_section(df: pd.DataFrame) -> None:
    """Render Row 1: GDP Trend (Line) & Growth Trend (Line)."""
    st.markdown('<div class="gdp-section-title">Xu hướng GDP & Tăng trưởng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_gdp_trend, df)
    with col2:
        _chart_card(chart_growth_trend, df)


def render_structure_section(df: pd.DataFrame) -> None:
    """Render Row 2 (GDP by Sector, Sector Share) và Row 4 (Treemap)."""
    st.markdown('<div class="gdp-section-title">Cơ cấu GDP theo Sector</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_gdp_by_sector, df)
    with col2:
        _chart_card(chart_sector_share, df)

    st.markdown('<div class="gdp-section-title">Cấu trúc Sector (Treemap)</div>', unsafe_allow_html=True)
    _chart_card(chart_treemap, df)


def render_ranking_section(df: pd.DataFrame) -> None:
    """Render Row 3: Top 10 Growth & Top GDP Share (Horizontal Bar)."""
    st.markdown('<div class="gdp-section-title">Bảng xếp hạng</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        _chart_card(chart_top10_growth, df)
    with col2:
        _chart_card(chart_top_gdp_share, df)


def render_drilldown_section(df: pd.DataFrame) -> None:
    """Render Row 6: Drill-down Sub-sector Analysis theo Sector được chọn."""
    st.markdown('<div class="gdp-section-title">Drill-down: Phân tích Phân ngành theo từng Khu vực kinh tế</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    sectors = sorted(df["sector_name"].dropna().unique().tolist())
    if not sectors:
        st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        _empty_chart_placeholder()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    with st.container(border=True):
        selected_sector = st.selectbox("Chọn Khu vực kinh tế để xem chi tiết Phân ngành", sectors)

    sector_df = df[df["sector_name"] == selected_sector]

    drilldown_table = (
        sector_df.groupby("sub_sector_name", as_index=False)
        .agg(
            market_gdp=("market_value", "sum"),
            real_gdp=("constant_value", "sum"),
            real_yoy_growth_rate=("real_yoy_growth_rate", "mean"),
            real_qoq_growth_rate=("real_qoq_growth_rate", "mean"),
            gdp_share_pct=("gdp_share_pct", "mean"),
        )
        .sort_values("gdp_share_pct", ascending=False)
        .rename(
            columns={
                "sub_sector_name": "Sub-sector - Phân ngành",
                "market_gdp": "Market GDP - GDP theo giá hiện hành",
                "real_gdp": "Real GDP - GDP theo giá so sánh 2010",
                "real_yoy_growth_rate": "YoY Growth (%)",
                "real_qoq_growth_rate": "QoQ Growth (%)",
                "gdp_share_pct": "GDP Share (%)",
            }
        )
    )

    with st.container(border=True):
        if drilldown_table.empty:
            _empty_chart_placeholder()
        else:
            st.dataframe(
                drilldown_table.style.format(
                    {
                        "Market GDP - GDP theo giá hiện hành": "{:,.2f}",
                        "Real GDP - GDP theo giá so sánh 2010": "{:,.2f}",
                        "YoY Growth (%)": "{:.2f}",
                        "QoQ Growth (%)": "{:.2f}",
                        "GDP Share (%)": "{:.2f}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            

def render_dashboard() -> None:
    """Render toàn bộ GDP Growth Performance Dashboard.

    Đây là entry point duy nhất được gọi từ tabs/gdp.py. Hàm này
    điều phối toàn bộ flow: inject CSS -> load data -> filter ->
    KPI -> các section biểu đồ -> drill-down.
    """
    inject_custom_css()
    render_header()

    raw_df = load_data()

    if raw_df.empty:
        st.markdown('<div class="gdp-card">', unsafe_allow_html=True)
        _empty_chart_placeholder("Không thể tải dữ liệu từ gold.fact_gdp_growth.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    filtered_df = render_filters(raw_df)

    render_kpis(filtered_df)
    render_trend_section(filtered_df)
    render_structure_section(filtered_df)
    render_ranking_section(filtered_df)
    # render_growth_section(filtered_df)
    render_drilldown_section(filtered_df)