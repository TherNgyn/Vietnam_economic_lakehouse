import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import SparkSession


MODEL_GOLD_DATABASE = os.getenv("MODEL_GOLD_DATABASE", "gold_model")

COLOR_BACKGROUND = "#081A36"
COLOR_CARD = "#071733"
COLOR_CARD_DARK = "#061229"
COLOR_BORDER = "#2C6FB8"
COLOR_HEADER = "#1B4F9C"
COLOR_ACCENT = "#3FA9F5"
COLOR_YELLOW = "#F5B041"
COLOR_TEXT = "#FFFFFF"
COLOR_TEXT_MUTED = "#CFE3FB"


@st.cache_resource
def get_spark():
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    )

    return (
        SparkSession.builder
        .appName("Inflation-Forecast-Dashboard")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.hive.metastore.uris",
            os.getenv("HIVE_METASTORE_URI", "thrift://hive:9083"),
        )
        .config(
            "hive.metastore.uris",
            os.getenv("HIVE_METASTORE_URI", "thrift://hive:9083"),
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .enableHiveSupport()
        .getOrCreate()
    )


def inject_dashboard_css():
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}

        .forecast-header {{
            background: linear-gradient(90deg, {COLOR_HEADER} 0%, {COLOR_ACCENT} 100%);
            border-radius: 16px;
            padding: 22px 28px;
            margin-bottom: 22px;
            border: 1px solid {COLOR_BORDER};
            box-shadow: 0 4px 14px rgba(0, 0, 0, 0.35);
        }}

        .forecast-header h1 {{
            color: #FFFFFF !important;
            font-size: 28px;
            font-weight: 700;
            margin: 0;
        }}

        .forecast-header p {{
            color: #FFFFFF !important;
            font-size: 14px;
            margin: 6px 0 0 0;
        }}

        .forecast-card {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 18px 20px;
            color: #FFFFFF !important;
            margin-bottom: 16px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.28);
        }}

        .forecast-chart-wrapper {{
            background-color: {COLOR_CARD_DARK};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 12px 16px 6px 16px;
            margin-bottom: 18px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.28);
            color: #FFFFFF !important;
        }}

        .forecast-section-title {{
            color: #FFFFFF !important;
            font-size: 18px;
            font-weight: 700;
            margin: 8px 0 12px 4px;
            border-left: 4px solid {COLOR_ACCENT};
            padding-left: 10px;
        }}

        .forecast-caption {{
            color: {COLOR_TEXT_MUTED} !important;
            font-size: 13px;
            margin-top: -6px;
            margin-bottom: 12px;
        }}

        div[data-testid="stMetric"] {{
            background-color: {COLOR_CARD};
            border: 1px solid {COLOR_BORDER};
            border-radius: 14px;
            padding: 14px 16px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.28);
        }}

        div[data-testid="stMetric"] label,
        div[data-testid="stMetric"] div,
        div[data-testid="stMetricValue"] {{
            color: #FFFFFF !important;
        }}

        label,
        .stSelectbox label,
        .stMultiSelect label {{
            color: #FFFFFF !important;
            font-weight: 600 !important;
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

        ul[role="listbox"],
        li[role="option"] {{
            background-color: {COLOR_CARD} !important;
            color: #FFFFFF !important;
        }}

        li[role="option"]:hover {{
            background-color: {COLOR_HEADER} !important;
            color: #FFFFFF !important;
        }}

        .forecast-table-wrapper {{
            background-color: {COLOR_CARD_DARK};
            border: 1px solid {COLOR_BORDER};
            border-radius: 12px;
            padding: 10px;
            overflow-x: auto;
            margin-bottom: 4px;
        }}

        .forecast-html-table {{
            width: 100%;
            border-collapse: collapse;
            background-color: {COLOR_CARD_DARK} !important;
            color: #FFFFFF !important;
            font-size: 13px;
        }}

        .forecast-html-table thead th {{
            background-color: {COLOR_CARD} !important;
            color: #FFFFFF !important;
            border: 1px solid {COLOR_BORDER} !important;
            padding: 9px 10px;
            font-weight: 700;
            text-align: left;
            white-space: nowrap;
        }}

        .forecast-html-table tbody td {{
            background-color: {COLOR_CARD_DARK} !important;
            color: #FFFFFF !important;
            border: 1px solid rgba(44, 111, 184, 0.75) !important;
            padding: 8px 10px;
            white-space: nowrap;
        }}

        .forecast-html-table tbody tr:hover td {{
            background-color: {COLOR_HEADER} !important;
            color: #FFFFFF !important;
        }}

        .forecast-html-table th,
        .forecast-html-table td,
        .forecast-html-table tr,
        .forecast-html-table span,
        .forecast-html-table div,
        .forecast-html-table p {{
            color: #FFFFFF !important;
        }}

        .stMarkdown,
        .stMarkdown p,
        .stMarkdown span,
        .stMarkdown div {{
            color: #FFFFFF !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def safe_read_table(spark, table_name: str) -> pd.DataFrame:
    try:
        return spark.table(table_name).toPandas()
    except Exception as e:
        st.warning(f"Không đọc được bảng {table_name}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=120)
def load_data():
    spark = get_spark()

    pred = safe_read_table(
        spark,
        f"{MODEL_GOLD_DATABASE}.inflation_forecast_predictions",
    )
    metrics = safe_read_table(
        spark,
        f"{MODEL_GOLD_DATABASE}.ml_model_metrics",
    )
    registry = safe_read_table(
        spark,
        f"{MODEL_GOLD_DATABASE}.ml_model_registry",
    )
    importance = safe_read_table(
        spark,
        f"{MODEL_GOLD_DATABASE}.inflation_feature_importance",
    )
    features = safe_read_table(
        spark,
        f"{MODEL_GOLD_DATABASE}.inflation_model_features",
    )

    return pred, metrics, registry, importance, features


def display_model_name(raw_name: str) -> str:
    if str(raw_name).lower() == "varnn_rm":
        return "VARNN-RM"
    return str(raw_name).upper()


def clean_feature_name(value):
    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text or text.lower() in {"nan", "none", "null", "undefined"}:
        return None

    return text


def short_feature_label(feature_name: str) -> str:
    label = str(feature_name)

    replacements = {
        "cpi_mom_processed_inflation": "Inflation",
        "_lag_": " lag ",
        "_roll_mean_": " rolling mean ",
        "_roll_std_": " rolling std ",
        "gasoline_world": "Gasoline",
        "broad_money": "Broad money",
        "policy_rate": "Policy rate",
        "interest_rate": "Interest rate",
        "month_sin": "Month sin",
        "month_cos": "Month cos",
        "is_gfc_2008": "GFC 2008",
        "is_inflation_wave_2011": "Inflation wave 2011",
        "is_covid_shock": "Covid shock",
    }

    for old, new in replacements.items():
        label = label.replace(old, new)

    label = label.replace("_", " ")
    label = " ".join(label.split())

    return label.title()


def render_dark_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Không có dữ liệu để hiển thị.")
        return

    display_df = df.copy()

    for col in display_df.columns:
        if pd.api.types.is_datetime64_any_dtype(display_df[col]):
            display_df[col] = display_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    table_html = display_df.to_html(
        index=False,
        escape=True,
        classes="forecast-html-table",
        border=0,
    )

    st.markdown(
        f"""
        <div class="forecast-table-wrapper">
            {table_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_plotly_layout(fig: go.Figure, title=None, height: int = 420) -> go.Figure:
    layout_kwargs = {
        "paper_bgcolor": COLOR_CARD_DARK,
        "plot_bgcolor": COLOR_CARD_DARK,
        "font": dict(color=COLOR_TEXT, size=12),
        "height": height,
        "legend": dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=COLOR_TEXT),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        "title_font": dict(color=COLOR_TEXT),
        "xaxis": dict(
            gridcolor="rgba(207, 227, 251, 0.12)",
            zerolinecolor="rgba(207, 227, 251, 0.12)",
            color=COLOR_TEXT,
            title_font=dict(color=COLOR_TEXT),
            tickfont=dict(color=COLOR_TEXT),
        ),
        "yaxis": dict(
            gridcolor="rgba(207, 227, 251, 0.12)",
            zerolinecolor="rgba(207, 227, 251, 0.12)",
            color=COLOR_TEXT,
            title_font=dict(color=COLOR_TEXT),
            tickfont=dict(color=COLOR_TEXT),
        ),
        "hoverlabel": dict(
            bgcolor=COLOR_CARD,
            font_color=COLOR_TEXT,
            bordercolor=COLOR_BORDER,
        ),
        "margin": dict(l=45, r=35, t=55, b=40),
    }

    if title is not None:
        layout_kwargs["title"] = title

    fig.update_layout(**layout_kwargs)
    fig.update_xaxes(color=COLOR_TEXT)
    fig.update_yaxes(color=COLOR_TEXT)
    fig.update_traces(textfont=dict(color=COLOR_TEXT))

    return fig


def chart_card(fig: go.Figure, key: str) -> None:
    st.markdown('<div class="forecast-chart-wrapper">', unsafe_allow_html=True)
    st.plotly_chart(fig, use_container_width=True, key=key)
    st.markdown("</div>", unsafe_allow_html=True)


def get_active_model_info(registry: pd.DataFrame, selected_model: str):
    if registry.empty:
        return None

    active = registry[
        (registry["model_name"] == selected_model)
        & (registry["is_active"] == True)
    ].copy()

    if active.empty:
        return None

    if "created_at" in active.columns:
        active["created_at"] = pd.to_datetime(
            active["created_at"],
            errors="coerce",
        )
        active = active.sort_values("created_at", ascending=False)

    return active.iloc[0]


def filter_metrics_for_active_model(
    metrics: pd.DataFrame,
    selected_model: str,
    active_row,
) -> pd.DataFrame:
    if metrics.empty:
        return metrics

    model_metrics = metrics[metrics["model_name"] == selected_model].copy()

    if active_row is not None and "model_version" in model_metrics.columns:
        model_version = active_row.get("model_version")
        model_metrics = model_metrics[
            model_metrics["model_version"].astype(str) == str(model_version)
        ].copy()

    if "created_at" in model_metrics.columns:
        model_metrics["created_at"] = pd.to_datetime(
            model_metrics["created_at"],
            errors="coerce",
        )
        model_metrics = model_metrics.sort_values("created_at", ascending=False)

    return model_metrics


def get_top_features(
    importance: pd.DataFrame,
    selected_model: str,
    active_row=None,
    top_n: int = 3,
):
    if importance.empty:
        return []

    model_importance = importance[
        importance["model_name"] == selected_model
    ].copy()

    if active_row is not None and "model_version" in model_importance.columns:
        active_version = active_row.get("model_version")
        model_importance = model_importance[
            model_importance["model_version"].astype(str) == str(active_version)
        ].copy()

    if model_importance.empty or "feature_name" not in model_importance.columns:
        return []

    model_importance["feature_name"] = model_importance["feature_name"].apply(
        clean_feature_name
    )
    model_importance = model_importance.dropna(subset=["feature_name"])

    if model_importance.empty:
        return []

    model_importance = (
        model_importance
        .sort_values("importance_value", ascending=False)
        .head(top_n)
    )

    return model_importance["feature_name"].tolist()


def render_single_feature_trend(
    features: pd.DataFrame,
    feature_name: str,
    color: str,
    chart_key: str,
) -> None:
    feature_name = clean_feature_name(feature_name)

    if feature_name is None:
        st.info("Tên biến không hợp lệ.")
        return

    if features.empty or feature_name not in features.columns:
        st.info(f"Không có dữ liệu cho biến {feature_name}.")
        return

    if "date" not in features.columns:
        st.info("Bảng feature không có cột date.")
        return

    df = features[["date", feature_name]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[feature_name] = pd.to_numeric(df[feature_name], errors="coerce")
    df = df.dropna(subset=["date", feature_name]).sort_values("date")

    if df.empty:
        st.info(f"Không có dữ liệu hợp lệ cho biến {feature_name}.")
        return

    display_name = short_feature_label(feature_name)
    safe_key = (
        str(chart_key)
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df[feature_name],
            mode="lines",
            name=display_name,
            line=dict(
                color=color,
                width=1.25,
            ),
            hovertemplate=(
                "Date=%{x}<br>"
                f"{display_name}=%{{y:.4f}}"
                "<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Value",
        showlegend=False,
    )

    fig = style_plotly_layout(
        fig,
        title=display_name,
        height=300,
    )

    chart_card(fig, key=safe_key)


def render_feature_trend_charts_three(
    features: pd.DataFrame,
    top_features: list[str],
) -> None:
    if features.empty or not top_features:
        st.info("Chưa có dữ liệu xu hướng biến ảnh hưởng.")
        return

    if "date" not in features.columns:
        st.info("Bảng feature không có cột date.")
        return

    existing_features = []

    for feature_name in top_features:
        clean_name = clean_feature_name(feature_name)

        if (
            clean_name is not None
            and clean_name in features.columns
            and pd.api.types.is_numeric_dtype(features[clean_name])
        ):
            existing_features.append(clean_name)

    existing_features = existing_features[:3]

    if not existing_features:
        st.info("Không tìm thấy các biến ảnh hưởng trong bảng feature.")
        return

    colors = [
        COLOR_ACCENT,
        COLOR_YELLOW,
        "#5DADE2",
    ]

    cols = st.columns(3)

    for i, feature_name in enumerate(existing_features):
        with cols[i]:
            render_single_feature_trend(
                features=features,
                feature_name=feature_name,
                color=colors[i % len(colors)],
                chart_key=f"inflation_feature_trend_{i}_{feature_name}",
            )


def render_dashboard() -> None:
    inject_dashboard_css()

    st.markdown(
        """
        <div class="forecast-header">
            <h1>Inflation Forecast Dashboard</h1>
            <p>Tình hình lạm phát hiện tại và dự báo trong tương lai.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    pred, metrics, registry, importance, features = load_data()

    if pred.empty:
        st.warning("Chưa có dữ liệu dự báo.")
        return

    pred["prediction_for_date"] = pd.to_datetime(
        pred["prediction_for_date"],
        errors="coerce",
    )

    if "created_at" in pred.columns:
        pred["created_at"] = pd.to_datetime(
            pred["created_at"],
            errors="coerce",
        )

    pred = pred.sort_values("prediction_for_date")

    model_names = sorted(
        pred["model_name"]
        .dropna()
        .unique()
        .tolist()
    )

    if not model_names:
        st.warning("Không có model_name trong bảng dự báo.")
        return

    left_filter, _ = st.columns([1, 2])

    with left_filter:
        selected_model = st.selectbox(
            "Model",
            model_names,
            format_func=display_model_name,
        )

    view = pred[pred["model_name"] == selected_model].copy()

    if view.empty:
        st.warning("Không có dữ liệu cho model đã chọn.")
        return

    active_row = get_active_model_info(registry, selected_model)

    if active_row is not None and "model_version" in view.columns:
        active_version = active_row.get("model_version")
        view_active = view[
            view["model_version"].astype(str) == str(active_version)
        ].copy()

        if not view_active.empty:
            view = view_active

    if "created_at" in view.columns:
        latest = view.sort_values("created_at").tail(1).iloc[0]
    else:
        latest = view.sort_values("prediction_for_date").tail(1).iloc[0]

    c1, c2, c3 = st.columns(3)

    c1.metric("Model", display_model_name(latest["model_name"]))
    c2.metric(
        "Forecast month",
        latest["prediction_for_date"].strftime("%Y-%m"),
    )
    c3.metric(
        "Predicted inflation",
        f"{latest['predicted_value']:.4f}",
    )

    st.markdown(
        '<div class="forecast-section-title">Forecast vs Actual</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="forecast-caption">Predicted and actual inflation values by forecast month.</div>',
        unsafe_allow_html=True,
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=view["prediction_for_date"],
            y=view["predicted_value"],
            mode="lines",
            name="Predicted",
            line=dict(color=COLOR_ACCENT, width=1.25),
        )
    )

    actual = view[view["actual_value"].notna()].copy()

    if not actual.empty:
        fig.add_trace(
            go.Scatter(
                x=actual["prediction_for_date"],
                y=actual["actual_value"],
                mode="lines",
                name="Actual",
                line=dict(color=COLOR_YELLOW, width=1.25),
            )
        )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Inflation",
        legend_title="Series",
    )

    fig = style_plotly_layout(fig, height=440)
    chart_card(fig, key="inflation_forecast_vs_actual")

    st.markdown(
        '<div class="forecast-section-title">Model Metrics</div>',
        unsafe_allow_html=True,
    )

    model_metrics = filter_metrics_for_active_model(
        metrics,
        selected_model,
        active_row,
    )

    if model_metrics.empty:
        st.info("Chưa có dữ liệu metrics cho mô hình đang dự báo.")
    else:
        display_cols = [
            c for c in [
                "model_name",
                "dataset_split",
                "rmse",
                "mae",
                "r2",
                "created_at",
            ]
            if c in model_metrics.columns
        ]

        model_metrics_display = model_metrics[display_cols].copy()

        if "model_name" in model_metrics_display.columns:
            model_metrics_display["model_name"] = model_metrics_display[
                "model_name"
            ].apply(display_model_name)

        st.markdown('<div class="forecast-card">', unsafe_allow_html=True)
        render_dark_table(model_metrics_display)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        '<div class="forecast-section-title">Feature Importance</div>',
        unsafe_allow_html=True,
    )

    if importance.empty:
        st.info("Chưa có dữ liệu feature importance.")
        top_features = []
    else:
        if "created_at" in importance.columns:
            importance["created_at"] = pd.to_datetime(
                importance["created_at"],
                errors="coerce",
            )

        model_importance = importance[
            importance["model_name"] == selected_model
        ].copy()

        if active_row is not None and "model_version" in model_importance.columns:
            active_version = active_row.get("model_version")
            model_importance = model_importance[
                model_importance["model_version"].astype(str) == str(active_version)
            ].copy()

        if not model_importance.empty:
            model_importance = (
                model_importance
                .sort_values("importance_value", ascending=False)
                .head(15)
            )

            top_features = get_top_features(
                importance=importance,
                selected_model=selected_model,
                active_row=active_row,
                top_n=3,
            )

            fig_imp = go.Figure()

            positive = model_importance[
                model_importance["direction"] == "positive"
            ]
            negative = model_importance[
                model_importance["direction"] == "negative"
            ]

            if not positive.empty:
                fig_imp.add_trace(
                    go.Bar(
                        x=positive["importance_value"],
                        y=positive["feature_name"],
                        orientation="h",
                        name="Positive",
                        marker=dict(color=COLOR_ACCENT),
                    )
                )

            if not negative.empty:
                fig_imp.add_trace(
                    go.Bar(
                        x=negative["importance_value"],
                        y=negative["feature_name"],
                        orientation="h",
                        name="Negative",
                        marker=dict(color=COLOR_YELLOW),
                    )
                )

            fig_imp.update_layout(
                yaxis={"categoryorder": "total ascending"},
                xaxis_title="Importance",
                yaxis_title="Feature",
            )

            fig_imp = style_plotly_layout(
                fig_imp,
                title="Top feature importance",
                height=500,
            )

            chart_card(
                fig_imp,
                key="inflation_feature_importance",
            )
        else:
            top_features = []
            st.info("Không có feature importance cho mô hình đang dự báo.")

    st.markdown(
        '<div class="forecast-section-title">Trend of Key Forecast Drivers</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="forecast-caption">Xu hướng 3 biến có ảnh hưởng mạnh nhất đến dự báo.</div>',
        unsafe_allow_html=True,
    )

    render_feature_trend_charts_three(
        features=features,
        top_features=top_features,
    )

    st.markdown(
        '<div class="forecast-section-title">Predictions</div>',
        unsafe_allow_html=True,
    )

    prediction_cols = [
        c for c in [
            "prediction_for_date",
            "model_name",
            "predicted_value",
            "actual_value",
            "prediction_type",
            "created_at",
        ]
        if c in view.columns
    ]

    prediction_view = view[prediction_cols].sort_values(
        "prediction_for_date",
        ascending=False,
    )

    if "model_name" in prediction_view.columns:
        prediction_view["model_name"] = prediction_view["model_name"].apply(
            display_model_name
        )

    st.markdown('<div class="forecast-card">', unsafe_allow_html=True)
    render_dark_table(prediction_view)
    st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    render_dashboard()