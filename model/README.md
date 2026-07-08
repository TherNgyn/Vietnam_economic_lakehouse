# Inflation MLflow VARNN-RM pipeline

## Run manually inside model runner

```bash
python run_pipeline.py --task create_gold_tables
python run_pipeline.py --task build_gold_features
python run_pipeline.py --task build_model_features
python run_pipeline.py --task save_deeplake_snapshot --version 20260708_001
python run_pipeline.py --task train_varnn_rm_and_log_mlflow
python run_pipeline.py --task promote_model
python run_pipeline.py --task run_inference
python run_pipeline.py --task write_explainability
```
