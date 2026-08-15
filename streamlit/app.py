import streamlit as st

from shared.gdp_growth import render_dashboard as gdp_render
from shared.crop_yield import render_dashboard as crop_render
from shared.production_output import render_dashboard as product_output_render
from shared.international_trade import render_dashboard as international_trade_render
from shared.social_investment import render_dashboard as social_investment_render
from shared.ohlc import render_dashboard as market_render
from shared.macro_indicator import render_dashboard as macro_render

from shared.spark import get_spark_session
import pandas as pd
from pyspark.sql import functions as F

st.set_page_config(
    page_title="EcoLakeVN",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#102B55"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_TEXT = "#FFFFFF"
COLOR_TEXT_MUTED = "#CFE3FB"

GRAFANA_REALTIME_INDEX_URL = (
    "http://localhost:3000/public-dashboards/7f24b35274054df6a503c15f0ca48b26"
)

NAV_ITEMS = [
    "Home - Trang chủ",
    "GDP Growth Performance Dashboard\nTheo dõi tăng trưởng GDP",
    "Crop Yield Dashboard\nNăng suất cây trồng",
    "Social Investment Source\nNguồn vốn đầu tư xã hội",
    "International Production Trade Dashboard\nXuất nhập khẩu sản xuất quốc tế",
    "National Production Dashboard\nSản xuất quốc gia",
    "Market Dashboard\nThị trường quốc tế",
    "Macro Economic Dashboard\nChỉ số kinh tế vĩ mô",
    "Inflation Forecast Dashboard\nDự báo lạm phát",
    "Real time Index Viet Nam\nChỉ số chứng khoán thời gian thực Việt Nam",
]


def inject_global_css() -> None:
    """Inject CSS tổng thể cho app và sidebar navigation."""
    st.markdown(
        f"""
        <style>
        .home-hero {{
            background: linear-gradient(135deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            border: 1px solid {COLOR_BORDER};
            border-radius: 20px;
            padding: 28px 32px;
            margin-bottom: 22px;
            box-shadow: 0 5px 18px rgba(0, 0, 0, 0.35);
            display: flex;
            align-items: center;
            gap: 24px;
        }}

        .home-logo-box {{
            background-color: #FFFFFF;
            border-radius: 18px;
            padding: 12px;
            width: 150px;
            min-width: 150px;
            height: 110px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}

        .home-logo-box img {{
            max-width: 130px;
            max-height: 90px;
            object-fit: contain;
        }}

        .home-hero h1 {{
            color: #FFFFFF !important;
            font-size: 34px;
            font-weight: 800;
            margin: 0;
            line-height: 1.15;
        }}

        .home-hero h3 {{
            color: #E7F1FE !important;
            font-size: 18px;
            font-weight: 500;
            margin: 10px 0 0 0;
            line-height: 1.45;
        }}

        .home-section-title {{
            color: #FFFFFF;
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
            height: 100%;
        }}

        .home-card h3 {{
            color: #FFFFFF;
            font-size: 17px;
            margin: 0 0 8px 0;
            font-weight: 700;
        }}

        .home-card p {{
            color: #DCEBFA;
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
            height: 100%;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.25);
        }}

        .home-kpi-label {{
            color: #CFE3FB;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}

        .home-kpi-value {{
            color: #FFFFFF;
            font-size: 28px;
            font-weight: 800;
            margin-bottom: 4px;
        }}

        .home-kpi-note {{
            color: #BFD7EA;
            font-size: 12px;
        }}

        .source-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 10px;
        }}

        .source-item strong {{
            display: block;
            color: #FFFFFF;
            font-size: 14px;
            font-weight: 700;
            margin-bottom: 6px;
        }}

        .source-item p {{
            color: #CFE3FB;
            font-size: 12px;
            line-height: 1.45;
            margin: 0;
        }}

        .source-link {{
            display: block;
            text-decoration: none !important;
            color: inherit !important;
        }}

        .source-link:hover .source-item {{
            background-color: rgba(27, 79, 156, 0.85);
            border-color: {COLOR_ACCENT};
            transform: translateY(-2px);
            transition: all 0.18s ease-in-out;
        }}

        .footer {{
            margin-top: 28px;
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 16px;
            padding: 18px 22px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            color: #FFFFFF;
        }}

        .footer-left {{
            display: flex;
            align-items: center;
            gap: 14px;
        }}

        .footer-logo {{
            width: 64px;
            height: 48px;
            object-fit: contain;
            background: #FFFFFF;
            border-radius: 10px;
            padding: 4px;
        }}

        .footer h4 {{
            margin: 0;
            font-size: 17px;
            color: #FFFFFF;
        }}

        .footer p {{
            margin: 4px 0 0 0;
            font-size: 13px;
            color: #CFE3FB;
        }}
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

        section[data-testid="stSidebar"] {{
            background-color: {COLOR_CARD};
            border-right: 1px solid {COLOR_BORDER};
        }}

        section[data-testid="stSidebar"] > div {{
            padding-top: 1rem;
            padding-left: 0.75rem;
            padding-right: 0.75rem;
        }}

        .sidebar-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            border-radius: 14px;
            padding: 16px 14px;
            margin-bottom: 16px;
            border: 1px solid {COLOR_BORDER};
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.28);
        }}

        .sidebar-header h2 {{
            color: #FFFFFF !important;
            font-size: 20px;
            font-weight: 700;
            margin: 0;
            line-height: 1.25;
        }}

        .sidebar-header p {{
            color: #E7F1FE !important;
            font-size: 13px;
            margin: 6px 0 0 0;
            line-height: 1.35;
        }}

        .sidebar-section-title {{
            color: {COLOR_TEXT_MUTED} !important;
            font-size: 12px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin: 8px 0 8px 2px;
        }}

        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span {{
            color: #FFFFFF !important;
        }}

        /* Navigation dạng list liên tục */
        div[role="radiogroup"] {{
            background-color: rgba(8, 26, 54, 0.45);
            border: 1px solid rgba(44, 111, 184, 0.70);
            border-radius: 14px;
            overflow: hidden;
        }}

        div[role="radiogroup"] label {{
            background-color: transparent !important;
            border: none !important;
            border-bottom: 1px solid rgba(207, 227, 251, 0.18) !important;
            border-radius: 0 !important;
            padding: 13px 12px !important;
            margin: 0 !important;
            transition: all 0.18s ease-in-out;
            min-height: 44px;
        }}

        div[role="radiogroup"] label:last-child {{
            border-bottom: none !important;
        }}

        div[role="radiogroup"] label:hover {{
            background-color: rgba(27, 79, 156, 0.85) !important;
            box-shadow: inset 3px 0 0 {COLOR_ACCENT};
        }}

        div[role="radiogroup"] label > div:first-child {{
            transform: scale(0.85);
        }}

        div[role="radiogroup"] label p {{
            font-size: 14px !important;
            font-weight: 600 !important;
            line-height: 1.4 !important;
            /* Thêm thuộc tính hỗ trợ đọc dấu xuống dòng \n */
            white-space: pre-line !important; 
        }}

        .main-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            border-radius: 16px;
            padding: 22px 28px;
            margin-bottom: 20px;
            border: 1px solid {COLOR_BORDER};
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.30);
        }}

        .main-header h1 {{
            color: #FFFFFF !important;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .main-header p {{
            color: #E7F1FE !important;
            font-size: 14px;
            margin: 6px 0 0 0;
        }}

        .info-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            color: #FFFFFF;
            margin-bottom: 16px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.25);
        }}

        .info-card p {{
            color: #FFFFFF !important;
            margin: 0 0 10px 0;
            font-size: 14px;
            line-height: 1.5;
        }}

        .grafana-open-button {{
            display: inline-block;
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            color: #FFFFFF !important;
            text-decoration: none !important;
            padding: 12px 18px;
            border-radius: 12px;
            border: 1px solid {COLOR_BORDER};
            font-size: 14px;
            font-weight: 700;
            box-shadow: 0 4px 12px rgba(63, 169, 245, 0.28);
            transition: all 0.2s ease-in-out;
        }}

        .grafana-open-button:hover {{
            filter: brightness(1.08);
            box-shadow: 0 5px 16px rgba(63, 169, 245, 0.38);
        }}

        .url-box {{
            background-color: rgba(8, 26, 54, 0.65);
            border: 1px solid rgba(44, 111, 184, 0.65);
            border-radius: 12px;
            padding: 12px 14px;
            color: #CFE3FB;
            font-size: 13px;
            word-break: break-all;
            margin-top: 12px;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_navigation() -> str:
    """Render sidebar navigation và trả về tên page được chọn."""
    with st.sidebar:
        logo_col_1, logo_col_2, logo_col_3 = st.columns([0.5, 3, 0.5])

        with logo_col_2:
            st.image("shared/asset/LOGO_WHITE.png", width=180)

        st.markdown(
            """
            <div class="sidebar-header">
                <h2>EcoLakeVN</h2>
                <p>Tổng quan kinh tế và thị trường Việt Nam</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            '<div class="sidebar-section-title">Navigation</div>',
            unsafe_allow_html=True,
        )

        selected_page = st.radio(
            label="Navigation",
            options=NAV_ITEMS,
            index=0,
            label_visibility="collapsed",
            key="main_navigation",
        )

    return selected_page


def render_realtime_index_vietnam() -> None:
    st.markdown(
        f"""
        <div class="side-header">
            <h2>Real time Index Viet Nam</h2>
            <a class="grafana-open-button" href="{GRAFANA_REALTIME_INDEX_URL}" target="_blank">
                Open Real time Index Viet Nam
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )


from shared.home import render_home_page


def render_selected_page(selected_page: str) -> None:
    """Route selected navigation item tới dashboard tương ứng."""

    if selected_page == "Home - Trang chủ":
        render_home_page()

    elif (
        selected_page
        == "Real time Index Viet Nam\nChỉ số chứng khoán thời gian thực Việt Nam"
    ):
        render_realtime_index_vietnam()

    elif (
        selected_page
        == "GDP Growth Performance Dashboard\nTheo dõi tăng trưởng GDP"
    ):
        gdp_render()

    elif selected_page == "Crop Yield Dashboard\nNăng suất cây trồng":
        crop_render()

    elif selected_page == "National Production Dashboard\nSản xuất quốc gia":
        product_output_render()

    elif (
        selected_page
        == "International Production Trade Dashboard\nXuất nhập khẩu sản xuất quốc tế"
    ):
        international_trade_render()

    elif selected_page == "Social Investment Source\nNguồn vốn đầu tư xã hội":
        social_investment_render()

    elif selected_page == "Market Dashboard\nThị trường quốc tế":
        market_render()

    elif selected_page == "Macro Economic Dashboard\nChỉ số kinh tế vĩ mô":
        macro_render()

    elif selected_page == "Inflation Forecast Dashboard\nDự báo lạm phát":
        from shared.inflation_forecast_gold import (
            render_dashboard as inflation_forecast_render,
        )

        inflation_forecast_render()

    else:
        st.warning("Không tìm thấy dashboard tương ứng.")


def main() -> None:
    """Main entry point."""
    inject_global_css()
    selected_page = render_sidebar_navigation()
    render_selected_page(selected_page)


if __name__ == "__main__":
    main()