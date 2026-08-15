import json
import numpy as np
import pandas as pd
import joblib
import mlflow.pyfunc


class VarNNRMPyFunc(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.model = joblib.load(context.artifacts["model"])
        self.imputer = joblib.load(context.artifacts["imputer"])
        self.x_scaler = joblib.load(context.artifacts["x_scaler"])
        self.y_scaler = joblib.load(context.artifacts["y_scaler"])
        with open(context.artifacts["feature_columns"], "r", encoding="utf-8") as f:
            self.feature_columns = json.load(f)
        with open(context.artifacts["metadata"], "r", encoding="utf-8") as f:
            self.metadata = json.load(f)

    def predict(self, context, model_input):
        X = pd.DataFrame(model_input).copy()
        X = X[self.feature_columns]
        X_imp = self.imputer.transform(X)
        X_scaled = self.x_scaler.transform(X_imp)
        residual = float(self.metadata.get("last_valid_residual", 0.0))
        preds = []
        for i in range(len(X_scaled)):
            input_vector = np.hstack([X_scaled[i], residual]).reshape(1, -1)
            pred_scaled = self.model.predict(input_vector)[0]
            preds.append(pred_scaled)
        return self.y_scaler.inverse_transform(np.array(preds).reshape(-1, 1)).ravel()
