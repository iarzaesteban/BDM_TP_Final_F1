import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from loguru import logger
from scipy.stats import spearmanr
from sklearn.metrics import (
    roc_auc_score, f1_score,
    confusion_matrix, roc_curve,
)


BASE_DIR = Path("/app/F1_Project")
ML_DIR   = BASE_DIR / "data_processed" / "ml"
OUT_DIR  = ML_DIR / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BG      = "#0d0d0d"
F1_RED  = "#e10600"
F1_GOLD = "#ffd700"
F1_WHT  = "#f5f5f5"

MODEL_COLORS = {
    "Logistic Regression": "#00d4ff",
    "Random Forest":       F1_GOLD,
    "SVM RBF":             "#ff6b35",
    "XGBoost":             "#00ff88",
}
MODEL_NAMES = ["Logistic Regression", "Random Forest", "SVM RBF", "XGBoost"]
SAFE_NAMES  = ["logistic_regression",  "random_forest",  "svm_rbf",  "xgboost"]

plt.rcParams.update({
    "figure.facecolor": BG,
    "axes.facecolor":   BG,
    "axes.edgecolor":   F1_WHT,
    "text.color":       F1_WHT,
    "xtick.color":      F1_WHT,
    "ytick.color":      F1_WHT,
    "axes.labelcolor":  F1_WHT,
    "grid.color":       "#333333",
    "grid.alpha":       0.5,
    "font.family":      "sans-serif",
})

# baseline test set 2022-2024 (grilla sola como predictor de ranking)
BASELINE = 0.629


def load_artifacts():
    preds_df = pd.read_csv(ML_DIR / "test_predictions.csv")
    with open(ML_DIR / "ml_metrics.json", encoding="utf-8") as fh:
        metrics = json.load(fh)
    fi_df = pd.read_csv(ML_DIR / "feature_importance.csv")
    return preds_df, metrics, fi_df


def spearman_per_race(preds_df, safe_name):
    col = f"prob_{safe_name}"
    df  = preds_df[["year", "round", "finish_position", col]].copy()
    df["neg_finish"] = -df["finish_position"].fillna(99)

    rows = []
    for (yr, rnd), grp in df.groupby(["year", "round"]):
        if len(grp) < 3:
            continue
        corr, pval = spearmanr(grp[col], grp["neg_finish"])
        rows.append({"year": yr, "round": rnd, "spearman": corr, "pval": pval})
    return pd.DataFrame(rows)


def savefig(fig, name):
    path = OUT_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    logger.info(f"  {path.name}")


def plot_roc_curves(preds_df, y_true):
    fig, ax = plt.subplots(figsize=(8, 7), facecolor=BG)
    ax.set_facecolor(BG)

    ax.plot([0, 1], [0, 1], "--", color="#555555", lw=1.2, label="Aleatorio (AUC = 0.50)")

    for name, safe in zip(MODEL_NAMES, SAFE_NAMES):
        col = f"prob_{safe}"
        if col not in preds_df.columns:
            continue
        y_prob = preds_df[col].values
        fpr, tpr, _ = roc_curve(y_true, y_prob)
        auc = roc_auc_score(y_true, y_prob)
        ax.plot(fpr, tpr, lw=2.5, color=MODEL_COLORS[name],
                label=f"{name}  (AUC = {auc:.4f})")

    ax.set_xlabel("Tasa de Falsos Positivos (FPR)", fontsize=11)
    ax.set_ylabel("Tasa de Verdaderos Positivos (TPR)", fontsize=11)
    ax.set_title(
        "Curvas ROC — Predicción de Podio F1\nTest: temporadas 2022–2024",
        fontsize=13, pad=14,
    )
    ax.legend(loc="lower right", fontsize=9.5, facecolor="#1a1a1a", edgecolor="#444")
    ax.grid(True, alpha=0.3)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    savefig(fig, "01_roc_curves.png")


def plot_confusion_matrices(preds_df, y_true):
    fig, axes = plt.subplots(2, 2, figsize=(12, 10), facecolor=BG)
    axes = axes.flatten()

    for i, (name, safe) in enumerate(zip(MODEL_NAMES, SAFE_NAMES)):
        pred_col = f"pred_{safe}"
        prob_col = f"prob_{safe}"
        if pred_col not in preds_df.columns:
            continue

        y_pred = preds_df[pred_col].values
        y_prob = preds_df[prob_col].values
        cm     = confusion_matrix(y_true, y_pred)
        cm_pct = cm.astype(float) / cm.sum(axis=1, keepdims=True)

        ax = axes[i]
        ax.set_facecolor(BG)

        sns.heatmap(
            cm_pct, annot=True, fmt=".1%", cmap="RdYlGn",
            ax=ax, cbar=False, linewidths=0.5, linecolor="#333",
            annot_kws={"size": 13, "weight": "bold"},
        )
        for r in range(2):
            for c in range(2):
                ax.text(c + 0.5, r + 0.72, f"n={cm[r, c]:,}",
                        ha="center", va="center", color=F1_WHT, fontsize=8.5)

        auc = roc_auc_score(y_true, y_prob)
        f1  = f1_score(y_true, y_pred, zero_division=0)
        ax.set_title(f"{name}\nROC-AUC={auc:.4f}   F1={f1:.4f}",
                     color=F1_WHT, fontsize=11, pad=8)
        ax.set_xlabel("Predicho", fontsize=10)
        ax.set_ylabel("Real", fontsize=10)
        ax.set_xticklabels(["No Podio", "Podio"], fontsize=9)
        ax.set_yticklabels(["No Podio", "Podio"], fontsize=9, rotation=0)

    fig.suptitle("Matrices de Confusión — Test 2022–2024", fontsize=14, y=1.01)
    plt.tight_layout()
    savefig(fig, "02_confusion_matrices.png")


FEAT_LABELS = {
    "grid_position":           "Posición de grilla",
    "quali_position":          "Posición qualifying",
    "best_quali_time_s":       "Mejor tiempo quali (s)",
    "started_top3":            "Salida top 3 (flag)",
    "started_top10":           "Salida top 10 (flag)",
    "grid_quali_diff":         "Diferencia grilla-quali",
    "round":                   "Número de ronda",
    "constructor_name_freq":   "Historial constructor",
    "driver_nationality_freq": "Historial nacionalidad",
    "circuit_country_freq":    "Historial circuito",
}


def plot_feature_importance(fi_df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), facecolor=BG)

    for i, mname in enumerate(["Random Forest", "XGBoost"]):
        df_fi = fi_df[fi_df["model"] == mname].sort_values("importance", ascending=True)
        if df_fi.empty:
            continue

        ax = axes[i]
        ax.set_facecolor(BG)
        labels = [FEAT_LABELS.get(f, f) for f in df_fi["feature"]]
        vals   = df_fi["importance"].values
        color  = MODEL_COLORS[mname]

        bars = ax.barh(labels, vals, color=color, alpha=0.85, edgecolor="#333")
        for bar, v in zip(bars, vals):
            ax.text(v + 0.003, bar.get_y() + bar.get_height() / 2,
                    f"{v:.3f}", va="center", ha="left", fontsize=8.5)

        ax.set_title(f"Importancia de Features\n{mname}", fontsize=12, pad=10)
        ax.set_xlabel("Importancia (impureza de Gini)", fontsize=10)
        ax.grid(True, axis="x", alpha=0.3)

    plt.tight_layout()
    savefig(fig, "03_feature_importance.png")


def plot_spearman_distribution(preds_df):
    fig, axes = plt.subplots(2, 2, figsize=(13, 9), facecolor=BG)
    axes = axes.flatten()

    for i, (name, safe) in enumerate(zip(MODEL_NAMES, SAFE_NAMES)):
        if f"prob_{safe}" not in preds_df.columns:
            continue

        sp_df  = spearman_per_race(preds_df, safe)
        median = sp_df["spearman"].median()
        color  = MODEL_COLORS[name]

        ax = axes[i]
        ax.set_facecolor(BG)

        ax.hist(sp_df["spearman"], bins=30, color=color, alpha=0.78, edgecolor="#333")
        ax.axvline(median,   color=F1_WHT, lw=2.2, ls="-",
                   label=f"Mediana modelo: {median:.4f}")
        ax.axvline(BASELINE, color=F1_RED,  lw=2.2, ls="--",
                   label=f"Baseline (grilla): {BASELINE}")

        tag  = "Supera baseline" if median > BASELINE else "No supera baseline"
        tcol = "#00ff88" if median > BASELINE else F1_RED
        ax.set_title(f"{name}\n{tag}", color=tcol, fontsize=11, pad=8)
        ax.set_xlabel("Spearman por carrera", fontsize=10)
        ax.set_ylabel("Frecuencia", fontsize=10)
        ax.legend(fontsize=8.5, facecolor="#1a1a1a", edgecolor="#444")
        ax.grid(True, alpha=0.3)

    fig.suptitle(
        "Distribución del Spearman por Carrera\n"
        "(Ranking de probabilidades predichas vs. ranking real de llegada)",
        fontsize=13, y=1.01,
    )
    plt.tight_layout()
    savefig(fig, "04_spearman_distribution.png")


def plot_metrics_comparison(metrics_data):
    models_list = metrics_data["models"]
    names  = [m["model"]           for m in models_list]
    aucs   = [m["roc_auc"]         for m in models_list]
    f1s    = [m["f1_score"]        for m in models_list]
    spears = [m["spearman_median"] for m in models_list]

    x     = np.arange(len(names))
    width = 0.24

    fig, ax = plt.subplots(figsize=(11, 6), facecolor=BG)
    ax.set_facecolor(BG)

    b1 = ax.bar(x - width, aucs,   width, label="ROC-AUC",         color="#00d4ff", alpha=0.85, edgecolor="#333")
    b2 = ax.bar(x,         f1s,    width, label="F1-Score",         color=F1_GOLD,  alpha=0.85, edgecolor="#333")
    b3 = ax.bar(x + width, spears, width, label="Spearman mediano", color="#00ff88", alpha=0.85, edgecolor="#333")

    ax.axhline(BASELINE, color=F1_RED, lw=1.5, ls="--",
               label=f"Baseline grilla (Spearman = {BASELINE})")

    for bar in list(b1) + list(b2) + list(b3):
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + 0.006,
                f"{h:.3f}", ha="center", va="bottom", fontsize=8)

    ax.set_xlabel("Modelo", fontsize=12)
    ax.set_ylabel("Valor de la métrica", fontsize=12)
    ax.set_title(
        "Comparación de Métricas por Modelo — Test 2022–2024\n"
        f"Baseline a superar: Spearman = {BASELINE} (posición de grilla sola)",
        fontsize=12, pad=14,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(names, fontsize=10)
    ax.set_ylim(0, 1.05)
    ax.legend(fontsize=9, facecolor="#1a1a1a", edgecolor="#444")
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    savefig(fig, "05_metrics_comparison.png")


def plot_spearman_over_time(preds_df):
    fig, ax = plt.subplots(figsize=(13, 5), facecolor=BG)
    ax.set_facecolor(BG)

    race_index = None

    for name, safe in zip(MODEL_NAMES, SAFE_NAMES):
        if f"prob_{safe}" not in preds_df.columns:
            continue

        sp_df = (spearman_per_race(preds_df, safe)
                 .sort_values(["year", "round"])
                 .reset_index(drop=True))
        sp_df["race_idx"] = range(len(sp_df))

        if race_index is None:
            race_index = sp_df[["race_idx", "year", "round"]].copy()

        color = MODEL_COLORS[name]
        ax.plot(sp_df["race_idx"], sp_df["spearman"],
                color=color, alpha=0.30, lw=1)
        ax.plot(sp_df["race_idx"],
                sp_df["spearman"].rolling(5, min_periods=1, center=True).median(),
                color=color, lw=2.5, label=f"{name} (mediana móvil 5)")

    ax.axhline(BASELINE, color=F1_RED, lw=1.8, ls="--", label=f"Baseline ({BASELINE})")
    ax.axhline(0, color="#444", lw=0.8)

    if race_index is not None:
        for yr in [2022, 2023, 2024]:
            first = race_index[race_index["year"] == yr]["race_idx"]
            if not first.empty:
                ax.axvline(first.iloc[0], color="#555", lw=1, ls=":")
                ax.text(first.iloc[0] + 0.4, -0.18, str(yr), color="#888", fontsize=9)

    ax.set_xlabel("Carrera (test set 2022–2024, ordenado cronológicamente)", fontsize=10)
    ax.set_ylabel("Spearman por carrera", fontsize=10)
    ax.set_title(
        "Evolución del Spearman Carrera a Carrera — Test 2022–2024\n"
        "(Línea suavizada: mediana móvil de 5 carreras)",
        fontsize=12, pad=12,
    )
    ax.legend(fontsize=9.5, facecolor="#1a1a1a", edgecolor="#444")
    ax.grid(True, alpha=0.3)
    ax.set_ylim(-0.25, 1.05)

    plt.tight_layout()
    savefig(fig, "06_spearman_over_time.png")


def main():
    logger.info("Evaluación ML — TP Final BDM")

    preds_df, metrics_data, fi_df = load_artifacts()
    y_true = preds_df["is_podium"].values
    logger.info(f"Test set: {len(preds_df):,} registros | {y_true.mean():.1%} positivos")

    logger.info("Generando visualizaciones...")
    plot_roc_curves(preds_df, y_true)
    plot_confusion_matrices(preds_df, y_true)
    plot_feature_importance(fi_df)
    plot_spearman_distribution(preds_df)
    plot_metrics_comparison(metrics_data)
    plot_spearman_over_time(preds_df)

    logger.info(f"Listo. Outputs en {OUT_DIR}")


if __name__ == "__main__":
    main()
