from __future__ import annotations

from pathlib import Path


import pandas as pd
import streamlit as st
from pyspark.sql import functions as F

from shared.spark import get_spark_session


COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#102B55"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_TEXT = "#FFFFFF"
COLOR_TEXT_MUTED = "#CFE3FB"


LOGO_CANDIDATE_PATHS = [
    "shared/asset/LOGO.png",
    "shared/assets/LOGO.png",
    "asset/LOGO.png",
    "assets/LOGO.png",
    "shared/asset/LOGO_WHITE.png",
    "shared/assets/LOGO_WHITE.png",
]


def get_existing_logo_path() -> str | None:
    """Trả về đường dẫn logo đầu tiên tồn tại."""
    for path in LOGO_CANDIDATE_PATHS:
        if Path(path).exists():
            return path
    return None


def inject_home_css() -> None:
    """Inject CSS cho trang chủ EcoLakeVN."""
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .block-container {{
            padding-top: 1.1rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 100%;
        }}

        .home-hero-card {{
            background: linear-gradient(135deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            border: 1px solid {COLOR_BORDER};
            border-radius: 20px;
            padding: 28px 32px;
            margin-bottom: 22px;
            box-shadow: 0 5px 18px rgba(0, 0, 0, 0.35);
        }}

        .home-hero-title {{
            color: #FFFFFF !important;
            font-size: 38px;
            font-weight: 800;
            margin: 0;
            line-height: 1.15;
        }}

        .home-hero-subtitle {{
            color: #E7F1FE !important;
            font-size: 19px;
            font-weight: 500;
            margin: 10px 0 0 0;
            line-height: 1.45;
        }}

        .home-logo-fallback {{
            background-color: #FFFFFF;
            color: {COLOR_CARD};
            border-radius: 18px;
            padding: 18px;
            width: 160px;
            min-height: 100px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 800;
            font-size: 20px;
            text-align: center;
        }}

        .home-section-title {{
            color: #FFFFFF !important;
            font-size: 22px;
            font-weight: 700;
            margin: 24px 0 14px 2px;
            border-left: 5px solid {COLOR_ACCENT};
            padding-left: 12px;
        }}

        .home-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 16px;
            padding: 18px 20px;
            color: #FFFFFF;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.25);
            margin-bottom: 16px;
            min-height: 150px;
        }}

        .home-card h3 {{
            color: #FFFFFF !important;
            font-size: 17px;
            margin: 0 0 8px 0;
            font-weight: 700;
        }}

        .home-card p {{
            color: #DCEBFA !important;
            font-size: 14px;
            line-height: 1.55;
            margin: 0;
        }}

        .home-kpi {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 16px;
            padding: 18px 16px;
            text-align: center;
            min-height: 130px;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.25);
        }}

        .home-kpi-label {{
            color: #CFE3FB !important;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}

        .home-kpi-value {{
            color: #FFFFFF !important;
            font-size: 28px;
            font-weight: 800;
            margin-bottom: 4px;
        }}

        .home-kpi-note {{
            color: #BFD7EA !important;
            font-size: 12px;
        }}

        .team-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 16px;
            padding: 18px 20px;
            color: #FFFFFF;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.25);
            margin-bottom: 16px;
            min-height: 180px;
            text-align: center;
        }}

        .team-role {{
            color: {COLOR_ACCENT} !important;
            font-size: 13px;
            font-weight: 800;
            text-transform: uppercase;
            margin-bottom: 10px;
            letter-spacing: 0.5px;
        }}

        .team-card h3 {{
            color: #FFFFFF !important;
            font-size: 17px;
            margin: 0 0 10px 0;
            font-weight: 700;
            line-height: 1.35;
            white-space: normal;
        }}

        .team-card p {{
            color: #DCEBFA !important;
            font-size: 14px;
            line-height: 1.55;
            margin: 0;
        }}

        .source-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px; 
        }}

        .source-item {{
            background-color: rgba(8, 26, 54, 0.65);
            border: 1px solid rgba(44, 111, 184, 0.65);
            border-radius: 12px;
            padding: 14px;
            color: #FFFFFF;
            height: 100%; /* Đảm bảo các hộp có chiều cao bằng nhau trong cùng hàng */
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.18);
            box-sizing: border-box;
            transition: all 0.2s ease-in-out;
        }}

        .source-grid a:hover .source-item {{
            background-color: rgba(27, 79, 156, 0.85);
            border-color: #3FA9F5;
            transform: translateY(-2px);
        }}

        .footer-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 16px;
            padding: 18px 22px;
            color: #FFFFFF;
            margin-top: 28px;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.25);
        }}

        .footer-card h4 {{
            margin: 0;
            font-size: 17px;
            color: #FFFFFF !important;
        }}

        .footer-card p {{
            margin: 4px 0 0 0;
            font-size: 13px;
            color: #CFE3FB !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner="Đang tải dữ liệu trang chủ...")
def load_home_indicators() -> pd.DataFrame:
    """Tổng hợp indicator 3 năm gần nhất từ các bảng Gold/Mart hiện có."""
    spark = get_spark_session()
    datasets = []

    try:
        gdp = (
            spark.table("gold_marts.mart_gdp_metrics")
            .join(spark.table("gold_gold.dim_time"), on="time_key", how="left")
            .groupBy("year")
            .agg(
                F.sum("market_value").alias("gdp_current_value"),
                F.sum("constant_value").alias("gdp_constant_2010_value"),
                F.avg("market_yoy_growth_rate").alias("avg_market_yoy_growth"),
                F.avg("real_yoy_growth_rate").alias("avg_real_yoy_growth"),
                F.avg("implicit_price_deflator").alias("avg_gdp_deflator"),
            )
        )
        datasets.append(gdp)
    except Exception:
        pass

    try:
        macro = (
            spark.table("gold_gold.fact_macro_indicators")
            .join(spark.table("gold_gold.dim_time"), on="time_key", how="left")
            .join(
                spark.table("gold_gold.dim_macro_indicator"),
                on="macro_indicator_key",
                how="left",
            )
            .groupBy("year")
            .agg(
                F.avg(
                    F.when(
                        F.col("indicator_name") == "cpi_mom",
                        F.col("value"),
                    )
                ).alias("inflation_rate")
            )
        )
        datasets.append(macro)
    except Exception:
        pass

    try:
        trade = (
            spark.table("gold_marts.mart_trade_international")
            .groupBy("year")
            .agg(
                F.sum("trade_value").alias("trade_value"),
                F.sum("import_value").alias("import_value"),
            )
        )
        datasets.append(trade)
    except Exception:
        pass

    try:
        investment = (
            spark.table("gold_marts.mart_social_investment")
            .groupBy("year")
            .agg(
                F.sum("investment_value").alias("social_investment_value"),
            )
        )
        datasets.append(investment)
    except Exception:
        pass

    if not datasets:
        return pd.DataFrame()

    result = datasets[0]
    for item in datasets[1:]:
        result = result.join(item, on="year", how="outer")

    pdf = (
        result
        .orderBy(F.col("year").desc())
        .limit(3)
        .orderBy("year")
        .toPandas()
    )

    return pdf


def format_home_number(value) -> str:
    """Format số cho trang chủ."""
    if value is None or pd.isna(value):
        return "N/A"

    value = float(value)
    abs_value = abs(value)

    if abs_value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.2f}K"

    return f"{value:,.2f}"


def render_logo(width: int = 180) -> None:
    """Render logo nếu tìm thấy, nếu không thì render fallback text."""
    logo_path = get_existing_logo_path()

    if logo_path:
        st.image(logo_path, width=width)
    else:
        st.markdown(
            """
            <div class="home-logo-fallback">
                EcoLakeVN
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_hero() -> None:
    """Render hero section không bị chồng logo và text."""
    with st.container(border=True):
        logo_col, text_col = st.columns([1, 5])

        with logo_col:
            render_logo(width=180)

        with text_col:
            st.markdown(
                """
                <h1 class="home-hero-title">EcoLakeVN</h1>
                <h3 class="home-hero-subtitle">
                    Hệ thống Data Lakehouse phân tích kinh tế Việt Nam và dự báo lạm phát
                </h3>
                """,
                unsafe_allow_html=True,
            )


def render_kpis(indicators: pd.DataFrame) -> None:
    """Render KPI nổi bật."""
    latest = indicators.iloc[-1] if not indicators.empty else None

    latest_year = (
        int(latest["year"])
        if latest is not None and "year" in indicators.columns and pd.notna(latest["year"])
        else "N/A"
    )

    gdp_current = latest.get("gdp_current_value", None) if latest is not None else None
    gdp_constant = latest.get("gdp_constant_2010_value", None) if latest is not None else None
    inflation = latest.get("inflation_rate", None) if latest is not None else None
    trade_value = latest.get("trade_value", None) if latest is not None else None
    real_growth = latest.get("avg_real_yoy_growth", None) if latest is not None else None

    st.markdown(
        '<div class="home-section-title">Vietnam Economic Remarkables - Chỉ số nổi bật</div>',
        unsafe_allow_html=True,
    )

    kpi_cols = st.columns(6)

    kpis = [
        ("Năm dữ liệu mới nhất", latest_year, "Latest available year"),
        ("GDP hiện hành", format_home_number(gdp_current), "Market GDP"),
        ("GDP so sánh 2010", format_home_number(gdp_constant), "Real GDP"),
        (
            "Tăng trưởng thực",
            f"{format_home_number(real_growth)}%" if real_growth is not None and not pd.isna(real_growth) else "N/A",
            "Real YoY growth",
        ),
        (
            "Lạm phát",
            f"{format_home_number(inflation)}%" if inflation is not None and not pd.isna(inflation) else "N/A",
            "Inflation rate",
        ),
        ("Thương mại", format_home_number(trade_value), "Trade value"),
    ]

    for col, (label, value, note) in zip(kpi_cols, kpis):
        with col:
            st.markdown(
                f"""
                <div class="home-kpi">
                    <div class="home-kpi-label">{label}</div>
                    <div class="home-kpi-value">{value}</div>
                    <div class="home-kpi-note">{note}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_economic_analysis() -> None:
    """Render phần phân tích mô tả."""
    st.markdown(
        '<div class="home-section-title">Economic Analysis - Phân tích kinh tế</div>',
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns(2)

    with c1:
        st.markdown(
            """
            <div class="home-card">
                <h3>A rising economic data platform</h3>
                <p>
                    EcoLakeVN được xây dựng như một hệ thống Data Lakehouse phục vụ tích hợp,
                    lưu trữ, xử lý và phân tích dữ liệu kinh tế Việt Nam. Hệ thống tập trung
                    vào các nhóm dữ liệu quan trọng như GDP, sản xuất, thương mại quốc tế,
                    đầu tư xã hội, thị trường và chỉ báo vĩ mô.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            """
            <div class="home-card">
                <h3>Inflation forecasting and future insights</h3>
                <p>
                    Bên cạnh các dashboard phân tích dữ liệu lịch sử, hệ thống hỗ trợ định hướng
                    dự báo lạm phát nhằm cung cấp góc nhìn về xu hướng giá cả, tiền tệ và sức khỏe
                    kinh tế trong tương lai.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    c3, c4 = st.columns(2)

    with c3:
        st.markdown(
            """
            <div class="home-card">
                <h3>Manufacturing, trade and market monitoring</h3>
                <p>
                    Hệ thống kết nối các nhóm dữ liệu sản xuất, thương mại, thị trường tài chính
                    và chỉ số vĩ mô, giúp người dùng quan sát sự dịch chuyển của nền kinh tế
                    theo nhiều góc nhìn khác nhau.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c4:
        st.markdown(
            """
            <div class="home-card">
                <h3>Data Lakehouse architecture</h3>
                <p>
                    Kiến trúc Lakehouse giúp kết hợp ưu điểm của Data Lake và Data Warehouse,
                    hỗ trợ lưu trữ dữ liệu lớn, xử lý theo lớp Bronze, Silver, Gold và phục vụ
                    trực tiếp cho dashboard phân tích.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_indicator_table(indicators: pd.DataFrame) -> None:
    """Render bảng indicator: indicator là dòng, năm là cột."""
    st.markdown(
        '<div class="home-section-title">Indicator Summary - Bảng chỉ số tổng hợp 3 năm gần nhất</div>',
        unsafe_allow_html=True,
    )

    if indicators.empty:
        st.warning(
            "Chưa tải được dữ liệu tổng hợp. Vui lòng kiểm tra tên bảng mart trong hàm load_home_indicators()."
        )
        return

    display_df = indicators.copy()

    rename_map = {
        "year": "Năm",
        "gdp_current_value": "GDP hiện hành",
        "gdp_constant_2010_value": "GDP so sánh 2010",
        "avg_market_yoy_growth": "Tăng trưởng hiện hành YoY (%)",
        "avg_real_yoy_growth": "Tăng trưởng thực YoY (%)",
        "avg_gdp_deflator": "GDP Deflator",
        "inflation_rate": "Lạm phát (%)",
        "trade_value": "Thương mại",
        "import_value": "Nhập khẩu",
        "social_investment_value": "Đầu tư xã hội",
    }

    display_df = display_df.rename(columns=rename_map)

    if "Năm" not in display_df.columns:
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        return

    display_df["Năm"] = display_df["Năm"].astype(int).astype(str)

    indicator_table = (
        display_df
        .set_index("Năm")
        .T
        .reset_index()
        .rename(columns={"index": "Indicator - Chỉ số"})
    )

    st.dataframe(
        indicator_table,
        use_container_width=True,
        hide_index=True,
    )


def render_system_numbers() -> None:
    """Render các con số thống kê hệ thống."""
    st.markdown(
        '<div class="home-section-title">EcoLakeVN</div>',
        unsafe_allow_html=True,
    )

    number_cols = st.columns(4)

    system_numbers = [
        ("9", "Dashboard", "Các phân hệ phân tích trong hệ thống"),
        ("17", "Nguồn dữ liệu", "Dữ liệu kinh tế, thị trường và lạm phát"),
        ("3", "Nhóm phân tích", "Kinh tế, thị trường, dự báo lạm phát"),
        ("1", "Lakehouse", "Kiến trúc dữ liệu tập trung"),
    ]

    for col, (value, title, desc) in zip(number_cols, system_numbers):
        with col:
            st.markdown(
                f"""
                <div class="home-kpi">
                    <div class="home-kpi-value">{value}</div>
                    <div class="home-kpi-label">{title}</div>
                    <div class="home-kpi-note">{desc}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_about_us() -> None:
    """Render About Us."""
    st.markdown(
        '<div class="home-section-title">About Us</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="home-card">
            <p>
                EcoLakeVN là nền tảng phân tích dữ liệu kinh tế Việt Nam được xây dựng trên mô hình
                Data Lakehouse. Hệ thống hướng đến việc hợp nhất dữ liệu từ nhiều nguồn, chuẩn hóa
                qua các lớp xử lý dữ liệu và trình bày thành các dashboard trực quan phục vụ theo dõi,
                phân tích và dự báo.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_team() -> None:
    """Render Meet the Team."""
    st.markdown(
        '<div class="home-section-title">Meet the Team</div>',
        unsafe_allow_html=True,
    )

    team_cols = st.columns(3)

    team_members = [
        {
            "role": "Data Engineer",
            "name": "Nguyen Thi Hong Tho<br>  Doan Quang Lam",
            "desc": "Xây dựng pipeline, mô hình dữ liệu và Lakehouse",
        },
        {
            "role": "Data Analyst",
            "name": "Nguyen Thi Hong Tho<br>  Doan Quang Lam",
            "desc": "Thiết kế dashboard, phân tích KPI và trực quan hóa dữ liệu",
        },
        {
            "role": "Machine Learning",
            "name": "Nguyen Thi Hong Tho",
            "desc": "Xây dựng mô hình dự báo lạm phát và đánh giá kết quả",
        },
    ]

    for col, member in zip(team_cols, team_members):
        with col:
            st.markdown(
                f"""
                <div class="team-card">
                    <div class="team-role">{member["role"]}</div>
                    <h3>{member["name"]}</h3>
                    <p>{member["desc"]}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_sources() -> None:
    """Render Data Sources dưới dạng lưới có thể click vào link."""
    st.markdown(
        '<div class="home-section-title">Data Sources - 17 nguồn dữ liệu</div>',
        unsafe_allow_html=True,
    )

    sources = [
        {
            "name": "GDP, Nguồn vốn đầu tư toàn xã hội, Hàng hóa xuất khẩu và nhập khẩu, Sản xuất lâm nghiệp, Sản lượng chăn nuôi, Sản xuất thủy sản, Sản xuất công nghiệp, Cây trồng hằng năm/ lâu năm/ chủ yếu",
            "desc": "Cục Thống kê - Bộ Tài chính, chuyên mục Báo cáo tình hình kinh tế - xã hội hàng tháng",
            "url": "https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/",
        },
        {
            "name": "Chỉ số giá tiêu dùng của Việt Nam - CPI",
            "desc": "Cục Thống kê - Bộ Tài chính, chuyên mục Chỉ số giá tiêu dùng (CPI)",
            "url": "https://www.nso.gov.vn/px-web-2/?pxid=V1101&theme=Th%C6%B0%C6%A1ng%20m%E1%BA%A1i%2C%20gi%C3%A1%20c%E1%BA%A3",
        },
        {
            "name": "Lạm phát cơ bản của Việt Nam - CPI Core",
            "desc": "Trendonify - Nguồn gốc: Cục Thống kê - Bộ Tài chính",
            "url": "https://trendonify.com/vietnam/core-inflation-rate",
        },
        {
            "name": "Chỉ số lạm phát sản xuất của Việt Nam - PPI",
            "desc": "Trendonify - Nguồn gốc: Cục Thống kê - Bộ Tài chính",

            "url": "https://trendonify.com/vietnam/producer-price-inflation-qoq",
        },
        {
            "name": "Tiền mở rộng - Broad Money và Lãi suất điều hành của Việt Nam - Policy Rate",
            "desc": "ADB ARIC - Economic and Financial Indicators Database",
            "url": "https://aric.adb.org/database/economic-financial-indicators",
        },
        {
            "name": "Lãi suất liên ngân hàng - Interest Rate",
            "desc": "Ngân hàng Nhà nước Việt Nam",
            "url": "https://www.sbv.gov.vn",
        },
        {
            "name": "Chỉ số thị trường chứng khoán Việt Nam thời gian thực: VN-Index, HNX-Index, UPCOM-Index",
            "desc": "Vikkibanks",
            "url": "https://banggia.vikkibanks.vn/",
        },
        {
            "name": "Chỉ số thị trường chứng khoán Việt Nam lịch sử: VN-Index, HNX-Index, UPCOM-Index",
            "desc": "Investing.com",
            "url": "https://www.investing.com/",
        },
        {
            "name": "Thị trường quốc tế: Tỷ giá, chứng khoán quốc tế, vàng, bạc, dầu thô, năng lượng ",
            "desc": "Yahoo Finance",
            "url": "https://finance.yahoo.com/",
        },
    ]

    source_html = '<div class="source-grid">'

    for source in sources:
        name = source["name"]
        desc = source["desc"]
        url = source["url"]
      
        source_html += f"""
<a href="{url}" target="_blank" style="text-decoration: none; color: inherit; display: block; height: 100%;">
<div class="source-item">
<strong>{name}</strong>
<p>{desc}</p>
</div>
</a>
"""

    source_html += "</div>"

    st.markdown(source_html, unsafe_allow_html=True)



def render_footer() -> None:
    """Render footer."""
    with st.container(border=True):
        left_col, right_col = st.columns([2, 1])

        with left_col:
            logo_col, text_col = st.columns([1, 4])

            with logo_col:
                render_logo(width=110)

            with text_col:
                st.markdown(
                    """
                    <h4 style="color:white;margin:0;">EcoLakeVN</h4>
                    <p style="color:#CFE3FB;margin:4px 0 0 0;">
                        Vietnam Economic Data Lakehouse &amp; Inflation Forecasting
                    </p>
                    """,
                    unsafe_allow_html=True,
                )

        with right_col:
            st.markdown(
                """
                <h4 style="color:white;margin:0;">Contact Us</h4>
                <p style="color:#CFE3FB;margin:4px 0 0 0;">Email: nguyenthihongtho0128@gmail.com</p>
                <p style="color:#CFE3FB;margin:4px 0 0 0;">Website: EcoLakeVN</p>
                """,
                unsafe_allow_html=True,
            )


def render_home_page() -> None:
    """Render toàn bộ trang chủ EcoLakeVN."""
    inject_home_css()

    indicators = load_home_indicators()

    render_hero()
    render_kpis(indicators)
    render_sources()
    render_indicator_table(indicators)
    render_economic_analysis()
    # render_system_numbers()
    render_team()
    render_footer()