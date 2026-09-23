import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import joblib
from loguru import logger
from sklearn.metrics import roc_auc_score, f1_score, accuracy_score

from train import build_features, spearman_by_race, FEATURE_COLS

BASE_DIR   = Path("/app/F1_Project")
MODELS_DIR = BASE_DIR / "models"
ML_DIR     = BASE_DIR / "data_processed" / "ml"

API_BASE = "https://api.jolpi.ca/ergast/f1"
SEASON   = 2025

# baseline original del test 2022-2024, calculado una sola vez (no se recalcula acá)
SPEARMAN_BASELINE_2022_2024 = 0.629


def time_to_seconds(t):
    if not t:
        return np.nan
    parts = t.split(":")
    try:
        if len(parts) == 2:
            m, s = parts
            return float(m) * 60 + float(s)
        return float(parts[0])
    except (ValueError, TypeError):
        return np.nan


def fetch_json(url, retries=3):
    for attempt in range(retries):
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
        time.sleep(1.5 * (attempt + 1))
    r.raise_for_status()


def get_rounds():
    data = fetch_json(f"{API_BASE}/{SEASON}/races.json?limit=40")
    races = data["MRData"]["RaceTable"]["Races"]
    return [
        {"round": int(r["round"]), "country": r["Circuit"]["Location"]["country"]}
        for r in races
    ]


def fetch_round(round_no, country):
    res_data = fetch_json(f"{API_BASE}/{SEASON}/{round_no}/results.json?limit=40")
    res_races = res_data["MRData"]["RaceTable"]["Races"]
    if not res_races:
        return []

    quali_data = fetch_json(f"{API_BASE}/{SEASON}/{round_no}/qualifying.json?limit=40")
    quali_races = quali_data["MRData"]["RaceTable"]["Races"]
    quali_by_driver = {}
    if quali_races:
        for q in quali_races[0]["QualifyingResults"]:
            times = [time_to_seconds(q.get(k)) for k in ("Q1", "Q2", "Q3")]
            times = [t for t in times if not np.isnan(t)]
            quali_by_driver[q["Driver"]["driverId"]] = {
                "quali_position": int(q["position"]),
                "best_quali_time_s": min(times) if times else np.nan,
            }

    rows = []
    for res in res_races[0]["Results"]:
        drv = res["Driver"]["driverId"]
        q = quali_by_driver.get(drv, {})
        grid = int(res["grid"]) if int(res["grid"]) > 0 else np.nan
        finish_order = int(res["position"])
        rows.append({
            "year": SEASON,
            "round": round_no,
            "driver_name": f"{res['Driver']['givenName']} {res['Driver']['familyName']}",
            "driver_nationality": res["Driver"]["nationality"],
            "constructor_name": res["Constructor"]["name"],
            "circuit_country": country,
            "grid_position": grid,
            "finish_position": finish_order,
            "quali_position": q.get("quali_position", np.nan),
            "best_quali_time_s": q.get("best_quali_time_s", np.nan),
        })
    return rows


def load_2025_data():
    logger.info(f"Descargando temporada {SEASON} desde Jolpica-F1...")
    rounds = get_rounds()
    all_rows = []
    for r in rounds:
        rows = fetch_round(r["round"], r["country"])
        all_rows.extend(rows)
        logger.info(f"  Ronda {r['round']}: {len(rows)} pilotos")
        time.sleep(0.3)  # limite de 200 req/hora sin autenticacion

    df = pd.DataFrame(all_rows)
    df = df.dropna(subset=["grid_position", "finish_position"])
    df["is_podium"] = (df["finish_position"] <= 3).astype(int)
    logger.info(f"Temporada {SEASON} completa: {len(df):,} filas, {df['round'].nunique()} carreras")

    snapshot_path = ML_DIR / f"season_{SEASON}_snapshot.csv"
    df.to_csv(snapshot_path, index=False)
    logger.info(f"Snapshot de la temporada guardado en {snapshot_path}")

    return df


def main():
    logger.info("Validacion prospectiva 2025 — modelos congelados (sin reentrenar)")

    df_2025 = load_2025_data()

    with open(MODELS_DIR / "freq_maps.json") as fh:
        freq_maps = json.load(fh)
    scaler = joblib.load(MODELS_DIR / "scaler.pkl")

    X_2025, y_2025, _ = build_features(df_2025, freq_maps=freq_maps, fit=False)
    X_2025_scaled = scaler.transform(X_2025)

    # baseline 2025: la grilla sola, evaluada con la misma metodologia que los modelos
    grid_as_prob = -df_2025["grid_position"].values
    grid_sp_df = spearman_by_race(df_2025, grid_as_prob)
    baseline_2025 = float(grid_sp_df["spearman"].median())
    logger.info(f"Baseline 2025 (grilla sola, misma metodologia): {baseline_2025:.4f}")

    model_files = {
        "Logistic Regression": "logistic_regression.pkl",
        "Random Forest":       "random_forest.pkl",
        "SVM RBF":             "svm_rbf.pkl",
        "XGBoost":             "xgboost.pkl",
    }

    with open(ML_DIR / "ml_metrics.json") as fh:
        original_metrics = {m["model"]: m for m in json.load(fh)["models"]}

    results = []
    for name, fname in model_files.items():
        model = joblib.load(MODELS_DIR / fname)
        y_prob = model.predict_proba(X_2025_scaled)[:, 1]
        y_pred = model.predict(X_2025_scaled)

        auc = roc_auc_score(y_2025, y_prob)
        f1  = f1_score(y_2025, y_pred, zero_division=0)
        acc = accuracy_score(y_2025, y_pred)
        sp_df = spearman_by_race(df_2025, y_prob)
        sp_median = float(sp_df["spearman"].median())

        orig = original_metrics[name]
        results.append({
            "model": name,
            "roc_auc_2025": round(auc, 4),
            "roc_auc_2022_2024": orig["roc_auc"],
            "f1_2025": round(f1, 4),
            "f1_2022_2024": orig["f1_score"],
            "spearman_2025": round(sp_median, 4),
            "spearman_2022_2024": orig["spearman_median"],
            "supera_baseline_2025": sp_median > baseline_2025,
        })
        logger.info(
            f"  {name}: ROC-AUC={auc:.4f}  F1={f1:.4f}  "
            f"Spearman_2025={sp_median:.4f} (2022-24: {orig['spearman_median']})"
        )

    out = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "season": SEASON,
        "n_races": int(df_2025["round"].nunique()),
        "n_rows": int(len(df_2025)),
        "baseline_2025": round(baseline_2025, 4),
        "baseline_2022_2024": SPEARMAN_BASELINE_2022_2024,
        "models": results,
    }
    out_path = ML_DIR / "validacion_2025.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)
    logger.info(f"Guardado: {out_path}")

    print("\nResumen 2025 vs. 2022-2024 (modelos congelados, sin reentrenar):")
    print(f"{'Modelo':<22}{'Spearman 25':>13}{'Spearman 22-24':>16}{'Supera baseline 25':>21}")
    for r in results:
        print(f"{r['model']:<22}{r['spearman_2025']:>13}{r['spearman_2022_2024']:>16}{str(r['supera_baseline_2025']):>21}")
    print(f"\nBaseline grilla 2025: {baseline_2025:.4f}  (baseline 2022-2024 original: {SPEARMAN_BASELINE_2022_2024})")


if __name__ == "__main__":
    main()
