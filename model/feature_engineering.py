import numpy as np
import pandas as pd

from config import (
    DATE_COL,
    TARGET_COL,
    CANDIDATE_KEEP_VARS,
)


def build_varnn_features(raw_df: pd.DataFrame, keep_target_null: bool = True) -> pd.DataFrame:
    df = raw_df.copy()

    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    df = (
        df.dropna(subset=[DATE_COL])
        .sort_values(DATE_COL)
        .drop_duplicates(subset=[DATE_COL])
    )

    df = df.set_index(DATE_COL).asfreq("MS").ffill()

    keep_vars = [
        c
        for c in CANDIDATE_KEEP_VARS
        if c in df.columns
    ]

    df_base = df[keep_vars].copy()

    transformed_df = pd.DataFrame(index=df_base.index)

    for col in df_base.columns:
        transformed_df[col] = df_base[col].diff().fillna(0)

    calendar_small = pd.DataFrame(index=transformed_df.index)
    calendar_small["month_sin"] = np.sin(
        2 * np.pi * transformed_df.index.month / 12
    )
    calendar_small["month_cos"] = np.cos(
        2 * np.pi * transformed_df.index.month / 12
    )

    shock_small = pd.DataFrame(index=transformed_df.index)

    shock_small["is_gfc_2008"] = (
        (shock_small.index >= "2008-03-01")
        & (shock_small.index <= "2008-12-01")
    ).astype(int)

    shock_small["is_inflation_wave_2011"] = (
        (shock_small.index >= "2011-03-01")
        & (shock_small.index <= "2011-06-01")
    ).astype(int)

    shock_small["is_covid_shock"] = (
        (shock_small.index >= "2020-02-01")
        & (shock_small.index <= "2020-06-01")
    ).astype(int)

    feature_df = pd.DataFrame(index=transformed_df.index)

    lags_to_use = [1, 2, 3, 6]

    for col in keep_vars:
        for lag in lags_to_use:
            feature_df[f"{col}_lag_{lag}"] = transformed_df[col].shift(lag)

    for w in [2, 3, 6]:
        feature_df[f"{TARGET_COL}_roll_mean_{w}"] = (
            transformed_df[TARGET_COL]
            .shift(1)
            .rolling(w)
            .mean()
        )

        feature_df[f"{TARGET_COL}_roll_std_{w}"] = (
            transformed_df[TARGET_COL]
            .shift(1)
            .rolling(w)
            .std()
        )

    for col in shock_small.columns:
        feature_df[col] = shock_small[col]
        feature_df[f"{col}_lag1"] = shock_small[col].shift(1)

    feature_df["month_sin"] = calendar_small["month_sin"]
    feature_df["month_cos"] = calendar_small["month_cos"]

    feature_df["target_next"] = transformed_df[TARGET_COL].shift(-1)

    if keep_target_null:
        feature_df = feature_df.dropna(
            subset=[
                c
                for c in feature_df.columns
                if c != "target_next"
            ]
        )
    else:
        feature_df = feature_df.dropna()

    feature_df = feature_df.reset_index().rename(columns={DATE_COL: "date"})

    if "index" in feature_df.columns:
        feature_df = feature_df.rename(columns={"index": "date"})
    return feature_df

def build_varnn_rm_v2_features(
    raw_df: pd.DataFrame,
    keep_target_null: bool = True,
) -> pd.DataFrame:
    """
    Build feature set cho mô hình VARNN-RM v2 PyTorch.

    Logic chính:
    - Chuẩn hóa dữ liệu về monthly frequency.
    - TARGET_COL giữ nguyên dạng level.
    - Các biến giải thích khác lấy sai phân tháng.
    - Tạo feature tại thời điểm t: *_at_t.
    - Tạo lag 1, 2, 3, 6 cho toàn bộ keep_vars.
    - Tạo rolling mean/std cho TARGET_COL level.
    - Tạo shock dummy variables và lag1 của shock.
    - Tạo month_sin, month_cos.
    - Tạo target_next = TARGET_COL tháng kế tiếp.

    keep_target_null:
    - True: giữ dòng cuối cùng có target_next null để scoring/prediction.
    - False: bỏ các dòng không có target_next để training/evaluation.
    """

    df = raw_df.copy()

    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    df = (
        df.dropna(subset=[DATE_COL])
        .sort_values(DATE_COL)
        .drop_duplicates(subset=[DATE_COL])
    )

    df = df.set_index(DATE_COL).asfreq("MS").ffill()

    keep_vars = [
        c
        for c in CANDIDATE_KEEP_VARS
        if c in df.columns
    ]

    if TARGET_COL not in keep_vars:
        raise ValueError(
            f"TARGET_COL='{TARGET_COL}' không tồn tại trong dữ liệu hoặc CANDIDATE_KEEP_VARS"
        )

    df_base = df[keep_vars].copy()

    transformed_df = pd.DataFrame(index=df_base.index)

    for col in df_base.columns:
        if col == TARGET_COL:
            transformed_df[col] = df_base[col]
        else:
            transformed_df[col] = df_base[col].diff().fillna(0)

    calendar_small = pd.DataFrame(index=transformed_df.index)

    calendar_small["month_sin"] = np.sin(
        2 * np.pi * transformed_df.index.month / 12
    )

    calendar_small["month_cos"] = np.cos(
        2 * np.pi * transformed_df.index.month / 12
    )

    shock_small = pd.DataFrame(index=transformed_df.index)

    shock_small["is_gfc_2008"] = (
        (shock_small.index >= "2008-03-01")
        & (shock_small.index <= "2008-12-01")
    ).astype(int)

    shock_small["is_inflation_wave_2011"] = (
        (shock_small.index >= "2011-03-01")
        & (shock_small.index <= "2011-06-01")
    ).astype(int)

    shock_small["is_covid_shock"] = (
        (shock_small.index >= "2020-02-01")
        & (shock_small.index <= "2020-06-01")
    ).astype(int)

    feature_df = pd.DataFrame(index=transformed_df.index)

    for col in keep_vars:
        feature_df[f"{col}_at_t"] = transformed_df[col]

    lags_to_use = [1, 2, 3, 6]

    for col in keep_vars:
        for lag in lags_to_use:
            feature_df[f"{col}_lag_{lag}"] = transformed_df[col].shift(lag)

    for w in [2, 3, 6]:
        feature_df[f"{TARGET_COL}_roll_mean_{w}"] = (
            transformed_df[TARGET_COL]
            .rolling(w)
            .mean()
        )

        feature_df[f"{TARGET_COL}_roll_std_{w}"] = (
            transformed_df[TARGET_COL]
            .rolling(w)
            .std()
        )

    for col in shock_small.columns:
        feature_df[col] = shock_small[col]
        feature_df[f"{col}_lag1"] = shock_small[col].shift(1)

    feature_df["month_sin"] = calendar_small["month_sin"]
    feature_df["month_cos"] = calendar_small["month_cos"]

    feature_df["target_next"] = transformed_df[TARGET_COL].shift(-1)

    if not keep_target_null:
        feature_df = feature_df.dropna(subset=["target_next"])

    feature_df = feature_df.reset_index().rename(columns={DATE_COL: "date"})

    if "index" in feature_df.columns:
        feature_df = feature_df.rename(columns={"index": "date"})

    return feature_df

def build_tcn_features(
    raw_df: pd.DataFrame,
    keep_target_null: bool = True,
) -> pd.DataFrame:
    """
    Build feature set cho mô hình Window-based TCN Keras.

    Logic giữ nguyên theo notebook TCN:
    - Chuẩn hóa dữ liệu về monthly frequency.
    - Tất cả biến trong CANDIDATE_KEEP_VARS được lấy sai phân:
        transformed_df[col] = df_base[col].diff().bfill()
    - Thêm shock dummy variables.
    - Thêm month_sin, month_cos.
    - target_next = transformed_df[TARGET_COL].shift(-1)

    keep_target_null:
    - True: giữ dòng cuối có target_next null để scoring/prediction.
    - False: drop toàn bộ NaN giống notebook training.
    """

    df = raw_df.copy()

    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    df = (
        df.dropna(subset=[DATE_COL])
        .sort_values(DATE_COL)
        .drop_duplicates(subset=[DATE_COL])
    )

    df = df.set_index(DATE_COL).asfreq("MS").ffill()

    keep_vars = [
        c
        for c in CANDIDATE_KEEP_VARS
        if c in df.columns
    ]

    if TARGET_COL not in keep_vars:
        raise ValueError(
            f"TARGET_COL='{TARGET_COL}' không tồn tại trong dữ liệu hoặc CANDIDATE_KEEP_VARS"
        )

    df_base = df[keep_vars].copy()

    transformed_df = pd.DataFrame(index=df_base.index)

    for col in df_base.columns:
        transformed_df[col] = df_base[col].diff().bfill()

    calendar_small = pd.DataFrame(index=transformed_df.index)

    calendar_small["month_sin"] = np.sin(
        2 * np.pi * transformed_df.index.month / 12
    )

    calendar_small["month_cos"] = np.cos(
        2 * np.pi * transformed_df.index.month / 12
    )

    shock_small = pd.DataFrame(index=transformed_df.index)

    shock_small["is_gfc_2008"] = (
        (shock_small.index >= "2008-03-01")
        & (shock_small.index <= "2008-12-01")
    ).astype(int)

    shock_small["is_inflation_wave_2011"] = (
        (shock_small.index >= "2011-03-01")
        & (shock_small.index <= "2011-06-01")
    ).astype(int)

    shock_small["is_covid_shock"] = (
        (shock_small.index >= "2020-02-01")
        & (shock_small.index <= "2020-06-01")
    ).astype(int)

    feature_df = transformed_df.copy()

    for col in shock_small.columns:
        feature_df[col] = shock_small[col]

    feature_df["month_sin"] = calendar_small["month_sin"]
    feature_df["month_cos"] = calendar_small["month_cos"]

    feature_df["target_next"] = transformed_df[TARGET_COL].shift(-1)

    if keep_target_null:
        feature_df = feature_df.dropna(
            subset=[
                c
                for c in feature_df.columns
                if c != "target_next"
            ]
        )
    else:
        feature_df = feature_df.dropna()

    feature_df = feature_df.reset_index().rename(columns={DATE_COL: "date"})

    if "index" in feature_df.columns:
        feature_df = feature_df.rename(columns={"index": "date"})

    return feature_df