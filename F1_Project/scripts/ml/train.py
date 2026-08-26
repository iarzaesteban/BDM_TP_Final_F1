import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, f1_score, accuracy_score
from scipy.stats import spearmanr
import xgboost as xgb
import joblib


BASE_DIR   = Path("/app/F1_Project")
MODELS_DIR = BASE_DIR / "models"
ML_DIR     = BASE_DIR / "data_processed" / "ml"

MODELS_DIR.mkdir(parents=True, exist_ok=True)
ML_DIR.mkdir(parents=True, exist_ok=True)
(ML_DIR / "outputs").mkdir(parents=True, exist_ok=True)

TRAIN_CUTOFF = 2021
TEST_START   = 2022
MIN_YEAR     = 1994  # qualifying completo desde 1994

# baseline del test set (2022-2024): grilla sola como predictor de ranking
SPEARMAN_BASELINE = 0.629

FEATURE_COLS = [
    "grid_position",
    "quali_position",
    "best_quali_time_s",
    "started_top3",
    "started_top10",
    "grid_quali_diff",
    "round",
    "constructor_name_freq",
    "driver_nationality_freq",
    "circuit_country_freq",
]

load_dotenv()


def get_engine():
    user     = os.getenv("POSTGRES_USER",     "f1_admin")
    password = os.getenv("POSTGRES_PASSWORD", "f1_pass")
    host     = os.getenv("POSTGRES_HOST",     "postgres")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "f1_dwh")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def load_data(engine):
    schema = os.getenv("DB_SCHEMA", "f1_dw")
    query = f"""
        SELECT
            year, round, circuit_country,
            driver_name, driver_nationality, constructor_name,
            grid_position, finish_position, quali_position,
            best_quali_time_s,
            COALESCE(is_podium::int, 0) AS is_podium
        FROM {schema}.vw_race_analysis
        WHERE year >= {MIN_YEAR}
          AND grid_position IS NOT NULL
          AND grid_position > 0
        ORDER BY year, round, grid_position
    """
    df = pd.read_sql(query, engine)
    logger.info(f"Datos cargados: {len(df):,} filas ({df['year'].min()}–{df['year'].max()})")
    logger.info(f"Podios: {df['is_podium'].sum():,} ({df['is_podium'].mean():.1%})")
    return df


def build_features(df, freq_maps=None, fit=True):
    df = df.copy()

    df["started_top3"]  = (df["grid_position"] <= 3).astype(float)
    df["started_top10"] = (df["grid_position"] <= 10).astype(float)

    # diferencia positiva = piloto salió peor de lo que clasificó (penalización)
    df["grid_quali_diff"] = (
        df["grid_position"] - df["quali_position"].fillna(df["grid_position"])
    )

    cat_cols = ["constructor_name", "driver_nationality", "circuit_country"]
    if fit:
        freq_maps = {}
        for col in cat_cols:
            freq_maps[col] = df[col].value_counts(normalize=True).to_dict()

    for col in cat_cols:
        df[col + "_freq"] = df[col].map(freq_maps[col]).fillna(0.0)

    df["quali_position"] = df["quali_position"].fillna(df["grid_position"])

    year_median = df.groupby("year")["best_quali_time_s"].transform("median")
    df["best_quali_time_s"] = df["best_quali_time_s"].fillna(year_median)
    df["best_quali_time_s"] = df["best_quali_time_s"].fillna(df["best_quali_time_s"].median())

    X = df[FEATURE_COLS].astype(float)
    y = df["is_podium"].astype(int)
    return X, y, freq_maps


def spearman_by_race(df_ctx, y_prob):
    # -finish_position: mayor prob predicha debe correlacionar con mejor posición de llegada
    df = df_ctx[["year", "round", "finish_position"]].copy()
    df["prob"] = y_prob
    df["neg_finish"] = -df["finish_position"].fillna(99)

    rows = []
    for (yr, rnd), grp in df.groupby(["year", "round"]):
        if len(grp) < 3:
            continue
        corr, pval = spearmanr(grp["prob"], grp["neg_finish"])
        rows.append({"year": yr, "round": rnd, "spearman": corr, "pval": pval})

    return pd.DataFrame(rows)


def train_all_models(X_train, y_train):
    pos_weight = float((y_train == 0).sum() / (y_train == 1).sum())

    model_defs = {
        "Logistic Regression": LogisticRegression(
            C=1.0, max_iter=1000, class_weight="balanced",
            solver="lbfgs", random_state=42,
        ),
        "Random Forest": RandomForestClassifier(
            n_estimators=300, max_depth=8, min_samples_leaf=10,
            class_weight="balanced", n_jobs=-1, random_state=42,
        ),
        "SVM RBF": SVC(
            C=1.0, kernel="rbf", gamma="scale",
            class_weight="balanced", probability=True, random_state=42,
        ),
        "XGBoost": xgb.XGBClassifier(
            n_estimators=300, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=pos_weight,
            eval_metric="logloss", n_jobs=-1, random_state=42,
        ),
    }

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)

    trained = {}
    for name, model in model_defs.items():
        logger.info(f"  Entrenando {name}...")
        model.fit(X_scaled, y_train)
        trained[name] = model

    return trained, scaler


def evaluate_model(name, model, scaler, X_test, y_test, df_test):
    X_scaled = scaler.transform(X_test)
    y_pred   = model.predict(X_scaled)
    y_prob   = model.predict_proba(X_scaled)[:, 1]

    auc = roc_auc_score(y_test, y_prob)
    f1  = f1_score(y_test, y_pred, zero_division=0)
    acc = accuracy_score(y_test, y_pred)

    sp_df      = spearman_by_race(df_test, y_prob)
    sp_median  = float(sp_df["spearman"].median())
    sp_mean    = float(sp_df["spearman"].mean())
    sp_sig_pct = float((sp_df["pval"] < 0.05).mean() * 100)

    metrics = {
        "model":                   name,
        "roc_auc":                 round(auc, 4),
        "f1_score":                round(f1,  4),
        "accuracy":                round(acc, 4),
        "spearman_median":         round(sp_median,  4),
        "spearman_mean":           round(sp_mean,    4),
        "spearman_sig_pct":        round(sp_sig_pct, 1),
        "spearman_baseline":       SPEARMAN_BASELINE,
        "spearman_beats_baseline": sp_median > SPEARMAN_BASELINE,
    }

    logger.info(
        f"  {name}: ROC-AUC={auc:.4f}  F1={f1:.4f}  Spearman={sp_median:.4f}"
    )
    return metrics, y_pred, y_prob, sp_df


def main():
    logger.info("Entrenamiento ML — TP Final BDM")

    engine = get_engine()
    df     = load_data(engine)

    df_train = df[df["year"] <= TRAIN_CUTOFF].reset_index(drop=True)
    df_test  = df[df["year"] >= TEST_START].reset_index(drop=True)
    logger.info(f"Train: {len(df_train):,} filas ({df_train['year'].min()}–{df_train['year'].max()})")
    logger.info(f"Test:  {len(df_test):,} filas ({df_test['year'].min()}–{df_test['year'].max()})")
    logger.info(f"Desbalance train: {df_train['is_podium'].mean():.1%} | test: {df_test['is_podium'].mean():.1%}")

    X_train, y_train, freq_maps = build_features(df_train, fit=True)
    X_test,  y_test,  _         = build_features(df_test, freq_maps=freq_maps, fit=False)

    logger.info("Entrenando modelos...")
    trained_models, scaler = train_all_models(X_train, y_train)

    logger.info("Evaluando en test 2022-2024...")
    all_metrics = []
    prob_cols   = {}

    for name, model in trained_models.items():
        metrics, y_pred, y_prob, _ = evaluate_model(
            name, model, scaler, X_test, y_test, df_test
        )
        all_metrics.append(metrics)
        safe = name.lower().replace(" ", "_")
        prob_cols[f"prob_{safe}"] = y_prob
        prob_cols[f"pred_{safe}"] = y_pred

    for name, model in trained_models.items():
        safe = name.lower().replace(" ", "_")
        joblib.dump(model, MODELS_DIR / f"{safe}.pkl")
    joblib.dump(scaler, MODELS_DIR / "scaler.pkl")
    with open(MODELS_DIR / "freq_maps.json", "w") as fh:
        json.dump(freq_maps, fh, indent=2)
    logger.info(f"Modelos guardados en {MODELS_DIR}/")

    results = {
        "generated_at":        datetime.now().isoformat(),
        "train_years":         f"{df_train['year'].min()}–{df_train['year'].max()}",
        "test_years":          f"{df_test['year'].min()}–{df_test['year'].max()}",
        "train_samples":       int(len(df_train)),
        "test_samples":        int(len(df_test)),
        "class_balance_train": round(float(df_train["is_podium"].mean()), 4),
        "class_balance_test":  round(float(df_test["is_podium"].mean()),  4),
        "spearman_baseline":   SPEARMAN_BASELINE,
        "feature_cols":        FEATURE_COLS,
        "models":              all_metrics,
    }
    with open(ML_DIR / "ml_metrics.json", "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)

    fi_rows = []
    for mname in ["Random Forest", "XGBoost"]:
        model = trained_models[mname]
        if hasattr(model, "feature_importances_"):
            for feat, imp in zip(FEATURE_COLS, model.feature_importances_):
                fi_rows.append({"model": mname, "feature": feat, "importance": float(imp)})
    pd.DataFrame(fi_rows).to_csv(ML_DIR / "feature_importance.csv", index=False)

    df_out = df_test[["year", "round", "driver_name", "constructor_name",
                       "grid_position", "finish_position", "is_podium"]].copy()
    for col, vals in prob_cols.items():
        df_out[col] = vals
    df_out.to_csv(ML_DIR / "test_predictions.csv", index=False)

    logger.info("Resultados:")
    for m in all_metrics:
        beats = "supera baseline" if m["spearman_beats_baseline"] else "no supera baseline"
        logger.info(f"  {m['model']}: ROC-AUC={m['roc_auc']} | F1={m['f1_score']} | Spearman={m['spearman_median']} ({beats})")
    logger.info("Listo.")


if __name__ == "__main__":
    main()
