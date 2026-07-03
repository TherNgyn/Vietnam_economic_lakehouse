import os
import io
import pickle
import numpy as np
import pandas as pd
import s3fs
import deeplake
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="CPI Forecast DL API")

SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")
DEEPLAKE_PATH = os.getenv("DEEPLAKE_PATH", f"s3://{SILVER_BUCKET}/deeplake/cpi_dataset")


def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )


class TrainRequest(BaseModel):
    model_type: str = "lstm"
    epochs: int = 50
    batch_size: int = 32
    seq_len: int = 12
    horizon: int = 3
    learning_rate: float = 0.001


class TrainResponse(BaseModel):
    job_id: str
    status: str
    message: str


_jobs: dict = {}


def _run_dl_training(job_id: str, req: TrainRequest):
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset

    _jobs[job_id] = {"status": "running", "mae": None, "rmse": None}

    try:
        ds = deeplake.load(DEEPLAKE_PATH)
        X_raw = ds["features"][:].numpy()
        y_raw = ds["target"][:].numpy()

        X = torch.FloatTensor(X_raw)
        y = torch.FloatTensor(y_raw)
        dataset = TensorDataset(X, y)
        loader = DataLoader(dataset, batch_size=req.batch_size, shuffle=False)

        input_size = X.shape[-1]

        if req.model_type == "lstm":
            class DLModel(nn.Module):
                def __init__(self):
                    super().__init__()
                    self.lstm = nn.LSTM(input_size, 64, num_layers=2, batch_first=True, dropout=0.2)
                    self.fc = nn.Linear(64, req.horizon)
                def forward(self, x):
                    out, _ = self.lstm(x)
                    return self.fc(out[:, -1, :])
        else:
            class DLModel(nn.Module):
                def __init__(self):
                    super().__init__()
                    self.gru = nn.GRU(input_size, 64, num_layers=2, batch_first=True, dropout=0.2)
                    self.fc = nn.Linear(64, req.horizon)
                def forward(self, x):
                    out, _ = self.gru(x)
                    return self.fc(out[:, -1, :])

        model = DLModel()
        optimizer = torch.optim.Adam(model.parameters(), lr=req.learning_rate)
        criterion = nn.MSELoss()

        for _ in range(req.epochs):
            for xb, yb in loader:
                pred = model(xb)
                loss = criterion(pred, yb)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        model.eval()
        with torch.no_grad():
            preds = model(X).numpy()
        mae = float(np.mean(np.abs(y_raw - preds)))
        rmse = float(np.sqrt(np.mean((y_raw - preds) ** 2)))

        fs = get_s3fs()
        model_path = f"{SILVER_BUCKET}/models/cpi/{req.model_type}_latest.pkl"
        with fs.open(model_path, "wb") as f:
            pickle.dump({"model_state": model.state_dict(), "model_type": req.model_type,
                         "input_size": input_size, "seq_len": req.seq_len,
                         "horizon": req.horizon, "mae": mae, "rmse": rmse,
                         "trained_at": datetime.utcnow().isoformat()}, f)

        _jobs[job_id] = {"status": "done", "mae": mae, "rmse": rmse}
    except Exception as e:
        _jobs[job_id] = {"status": "failed", "error": str(e)}


@app.post("/train", response_model=TrainResponse)
def train(req: TrainRequest, background_tasks: BackgroundTasks):
    job_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    background_tasks.add_task(_run_dl_training, job_id, req)
    _jobs[job_id] = {"status": "queued"}
    return TrainResponse(job_id=job_id, status="queued", message=f"Training {req.model_type} started")


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return _jobs[job_id]


@app.post("/dataset/push")
def push_dataset(seq_len: int = 12, horizon: int = 3):
    try:
        fs = get_s3fs()
        records = []
        for prefix in [f"{SILVER_BUCKET}/economics/cpi"]:
            try:
                files = fs.glob(f"{prefix}/**/*.parquet")
                for fp in files:
                    with fs.open(fp, "rb") as f:
                        records.append(pd.read_parquet(f))
            except Exception:
                pass

        if not records:
            raise HTTPException(status_code=400, detail="No CPI data available in silver layer")

        df = pd.concat(records).sort_values("date").dropna(subset=["value"])
        values = df["value"].values.astype(np.float32)

        X_seqs, y_seqs = [], []
        for i in range(len(values) - seq_len - horizon + 1):
            X_seqs.append(values[i:i + seq_len].reshape(-1, 1))
            y_seqs.append(values[i + seq_len:i + seq_len + horizon])

        if not X_seqs:
            raise HTTPException(status_code=400, detail="Not enough data for sequences")

        X_arr = np.array(X_seqs, dtype=np.float32)
        y_arr = np.array(y_seqs, dtype=np.float32)

        try:
            ds = deeplake.load(DEEPLAKE_PATH)
            ds.pop_tensors()
        except Exception:
            ds = deeplake.dataset(DEEPLAKE_PATH)

        ds.create_tensor("features", dtype="float32", overwrite=True)
        ds.create_tensor("target", dtype="float32", overwrite=True)

        with ds:
            ds["features"].extend(X_arr)
            ds["target"].extend(y_arr)

        return {"status": "ok", "sequences": len(X_seqs)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("MODEL_API_PORT", 8000)))
