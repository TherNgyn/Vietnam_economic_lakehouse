import argparse
from datetime import datetime


def main():
    p = argparse.ArgumentParser()

    p.add_argument("--task", required=True)

    p.add_argument(
        "--version",
        default=datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
    )

    p.add_argument(
        "--feature-version",
        default=None,
        help="Deep Lake feature snapshot version, ví dụ: varnn_features_diff_all",
    )
    p.add_argument("--model-name", default=None)
    p.add_argument("--mlflow-version", default=None)


    args = p.parse_args()

    if args.task == "create_gold_tables":
        from gold_tables import create_gold_tables

        create_gold_tables()

    elif args.task == "build_gold_features":
        from build_gold_features import build_gold_monthly_features

        build_gold_monthly_features()

    elif args.task == "build_model_features":
        from build_model_features import build_model_features

        build_model_features()

    elif args.task == "save_deeplake_snapshot":
        from save_deeplake_snapshot import save_deeplake_snapshot

        save_deeplake_snapshot(args.version)

    elif args.task == "train_varnn_rm_v1_and_log_mlflow":
        from train_varnn_rm_v1_mlflow import train_varnn_rm

        train_varnn_rm(feature_version=args.feature_version)
    elif args.task == "train_varnn_rm_v2_and_log_mlflow":
        from train_varnn_rm_v2_mlflow import train_varnn_rm

        train_varnn_rm(feature_version=args.feature_version)
    elif args.task == "train_tcn_and_log_mlflow":
        from train_tcn_mlflow import train_tcn
        train_tcn(feature_version=args.feature_version)
    elif args.task == "promote_model":
        from promote_model import promote_latest_model

        promote_latest_model()

    elif args.task == "run_inference":
        from run_inference import run_inference

        run_inference()

    elif args.task == "write_explainability":
        from write_explainability import write_explainability

        write_explainability()

    else:
        raise ValueError(args.task)


if __name__ == "__main__":
    main()