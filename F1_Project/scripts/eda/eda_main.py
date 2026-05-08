"""
EDA - Análisis Exploratorio de Datos | F1 Data Warehouse (1950-2024)
====================================================================
TP Final - Bases de Datos Masivas - UNLu

¿Qué es el EDA?
    El Análisis Exploratorio de Datos (EDA) es el proceso de examinar,
    resumir y visualizar un dataset antes de aplicar modelos predictivos.
    Permite entender la distribución de variables, detectar outliers,
    identificar correlaciones y validar la calidad de los datos del DW.

Preguntas de investigación:
  1. ¿Qué factores influyen más en llegar al podio?
  2. ¿Cómo evolucionó el dominio de equipos y pilotos a lo largo de las décadas?
  3. ¿Qué tan determinante es la posición de clasificación (grilla) sobre el resultado final?
  4. ¿Cómo impactan los pit stops en el resultado?
  5. Matriz de correlación general entre variables numéricas
  6. Análisis de ranking: grilla predicha vs. posición real

Salida:
    F1_Project/data_processed/eda/outputs/  →  15 PNGs + eda_summary.txt
"""

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from scipy import stats
from sqlalchemy import create_engine
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

DB_USER = os.getenv("DB_USER", "f1_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "f1_pass")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "f1_dwh")
DB_SCHEMA = os.getenv("DB_SCHEMA", "f1_dw")

OUT_DIR = os.path.join(
    os.getenv("PROC_DIR", "/app/F1_Project/data_processed").replace(
        "data_processed", "data_processed/eda/outputs"
    )
)
os.makedirs(OUT_DIR, exist_ok=True)

plt.rcParams.update(
    {
        "figure.dpi": 150,
        "figure.facecolor": "#0f0f0f",
        "axes.facecolor": "#1a1a1a",
        "axes.edgecolor": "#444",
        "axes.labelcolor": "#e0e0e0",
        "axes.titlecolor": "#ffffff",
        "axes.titlesize": 13,
        "axes.labelsize": 11,
        "xtick.color": "#aaaaaa",
        "ytick.color": "#aaaaaa",
        "text.color": "#e0e0e0",
        "grid.color": "#2e2e2e",
        "grid.linestyle": "--",
        "legend.facecolor": "#1a1a1a",
        "legend.edgecolor": "#444",
        "font.family": "DejaVu Sans",
    }
)

F1_RED = "#e10600"
F1_GOLD = "#ffd700"
F1_SILVER = "#c0c0c0"
F1_WHITE = "#f5f5f5"
PALETTE = [
    F1_RED,
    "#00d2be",
    "#0067ff",
    "#ff8700",
    "#12098a",
    F1_GOLD,
    F1_SILVER,
    "#dc0000",
    "#006f62",
]


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def load_data(engine) -> dict:
    print("Cargando datos desde el DW...")

    dfs = {}

    # Vista analítica principal
    dfs["main"] = pd.read_sql(f"SELECT * FROM {DB_SCHEMA}.vw_race_analysis", engine)

    # Pit stops agregados por piloto/carrera
    dfs["pits"] = pd.read_sql(
        f"""
        SELECT
            race_key,
            driver_key,
            COUNT(*)            AS total_stops,
            AVG(pit_duration_s) AS avg_duration_s,
            SUM(pit_duration_s) AS total_pit_time_s,
            MIN(pit_duration_s) AS min_stop_s,
            MAX(pit_duration_s) AS max_stop_s
        FROM {DB_SCHEMA}.FactPitStops
        GROUP BY race_key, driver_key
    """,
        engine,
    )

    # Qualifying vs resultado
    dfs["quali"] = pd.read_sql(
        f"""
        SELECT
            fq.race_key,
            fq.driver_key,
            fq.quali_position,
            fq.best_quali_time_s,
            fr.finish_position,
            fr.grid_position,
            fr.is_podium,
            fr.points,
            dr.year
        FROM {DB_SCHEMA}.FactQualifying fq
        JOIN {DB_SCHEMA}.factraceresults fr
            ON fq.race_key = fr.race_key AND fq.driver_key = fr.driver_key
        JOIN {DB_SCHEMA}.DimRace dr ON fr.race_key = dr.race_key
        WHERE fq.quali_position IS NOT NULL
          AND fr.finish_position IS NOT NULL
    """,
        engine,
    )

    # Dataset para análisis de ranking
    dfs["ranking"] = pd.read_sql(
        f"""
        SELECT
            dr.race_id,
            dr.year,
            dr.round,
            dr.race_name,
            dv.full_name        AS driver_name,
            co.constructor_name,
            fq.quali_position,
            fr.grid_position,
            fr.finish_position,
            fr.position_order,
            fr.is_podium,
            fr.points
        FROM {DB_SCHEMA}.factraceresults fr
        JOIN {DB_SCHEMA}.DimRace        dr ON fr.race_key        = dr.race_key
        JOIN {DB_SCHEMA}.DimDriver      dv ON fr.driver_key      = dv.driver_key
        JOIN {DB_SCHEMA}.DimConstructor co ON fr.constructor_key = co.constructor_key
        LEFT JOIN {DB_SCHEMA}.FactQualifying fq
            ON fq.race_key = fr.race_key AND fq.driver_key = fr.driver_key
        WHERE fr.finish_position IS NOT NULL
          AND fr.grid_position   IS NOT NULL
          AND dr.year >= 1994
    """,
        engine,
    )

    print(f"Datos cargados: {len(dfs['main']):,} registros en vista principal.")
    return dfs


def save(fig, name: str):
    path = os.path.join(OUT_DIR, f"{name}.png")
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Guardado: {name}.png")


def section(title: str):
    print(f"\n{'=' * 55}\n  {title}\n{'=' * 55}")


def eda_overview(df: pd.DataFrame, summary_lines: list):
    section("0. Resumen general del dataset")

    lines = [
        "=" * 55,
        "  EDA - F1 Data Warehouse (1950-2024)",
        "=" * 55,
        f"Total registros (piloto x carrera):  {len(df):,}",
        f"Temporadas cubiertas:                {df['year'].nunique()} ({int(df['year'].min())}–{int(df['year'].max())})",
        f"Circuitos únicos:                    {df['circuit_name'].nunique()}",
        f"Pilotos únicos:                      {df['driver_name'].nunique()}",
        f"Constructores únicos:                {df['constructor_name'].nunique()}",
        f"Carreras totales:                    {df[['year', 'round']].drop_duplicates().shape[0]}",
        f"Registros con podio:                 {df['is_podium'].sum():,} ({df['is_podium'].mean() * 100:.1f}%)",
        f"Registros con pit stop data:         {df['total_stops'].notna().sum():,}",
        "",
        "── Nulos por columna clave ──",
    ]
    for col in [
        "grid_position",
        "finish_position",
        "quali_position",
        "best_quali_time_s",
        "total_stops",
        "points",
    ]:
        if col in df.columns:
            n = df[col].isna().sum()
            pct = n / len(df) * 100
            lines.append(f"  {col:<28} {n:>7,}  ({pct:.1f}%)")

    for l in lines:
        print(l)
    summary_lines.extend(lines)


# FACTORES QUE INFLUYEN EN EL PODIO
def eda_podium_factors(df: pd.DataFrame):
    section("1. Factores que influyen en llegar al podio")

    df = df.copy()
    df["grid_bin"] = pd.cut(
        df["grid_position"],
        bins=[0, 1, 3, 5, 10, 20, 30],
        labels=["P1", "P2-3", "P4-5", "P6-10", "P11-20", "P21+"],
    )

    # Tasa de podio por posición de grilla
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle("Tasa de podio según posición de grilla de largada", fontweight="bold")
    podio_grid = (
        df.dropna(subset=["grid_bin"])
        .groupby("grid_bin", observed=True)["is_podium"]
        .mean()
        .reset_index()
    )
    bars = ax.bar(
        podio_grid["grid_bin"].astype(str),
        podio_grid["is_podium"] * 100,
        color=[F1_RED, F1_GOLD, F1_SILVER, "#555", "#333", "#222"],
        edgecolor="#000",
        linewidth=0.5,
    )
    ax.set_ylabel("% Carreras con podio")
    ax.set_xlabel("Posición de grilla")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.grid(axis="y")
    for bar in bars:
        h = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            h + 0.5,
            f"{h:.1f}%",
            ha="center",
            va="bottom",
            fontsize=9,
            color=F1_WHITE,
        )
    save(fig, "01a_podio_por_grilla")

    # Top 15 constructores por tasa de podio
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.suptitle(
        "Top 15 constructores con mayor tasa de podio (min. 100 carreras)",
        fontweight="bold",
    )
    cons_podio = (
        df.groupby("constructor_name")
        .agg(carreras=("result_id", "count"), podios=("is_podium", "sum"))
        .query("carreras >= 100")
        .assign(tasa=lambda x: x["podios"] / x["carreras"] * 100)
    )

    cons_podio["tasa"] = pd.to_numeric(cons_podio["tasa"], errors="coerce")
    cons_podio = cons_podio.nlargest(15, "tasa").reset_index()

    bars = ax.barh(
        cons_podio["constructor_name"],
        cons_podio["tasa"],
        color=F1_RED,
        edgecolor="#000",
        linewidth=0.4,
    )
    ax.set_xlabel("% Podios")
    ax.invert_yaxis()
    ax.grid(axis="x")
    for bar, val in zip(bars, cons_podio["tasa"]):
        ax.text(
            val + 0.3,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}%",
            va="center",
            fontsize=8,
            color=F1_WHITE,
        )
    save(fig, "01b_podio_por_constructor")

    # Distribución de puntos: podio vs no podio
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle("Distribucion de puntos: podio vs no podio", fontweight="bold")
    for ax, (label, grp) in zip(axes, df.groupby("is_podium")):
        color = F1_GOLD if label else F1_SILVER
        title = "Podio" if label else "Fuera de podio"
        ax.hist(
            grp["points"].dropna(),
            bins=30,
            color=color,
            edgecolor="#000",
            linewidth=0.3,
        )
        ax.set_title(title)
        ax.set_xlabel("Puntos")
        ax.set_ylabel("Frecuencia")
        ax.grid(axis="y")
    save(fig, "01c_puntos_podio_vs_no")

    # Tasa de podio por nacionalidad (top 10)
    top_nat = df["driver_nationality"].value_counts().nlargest(10).index
    df_nat = df[df["driver_nationality"].isin(top_nat)]
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle("Tasa de podio por nacionalidad de piloto (top 10)", fontweight="bold")
    nat_data = (
        df_nat.groupby("driver_nationality")["is_podium"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )
    ax.bar(
        nat_data["driver_nationality"],
        nat_data["is_podium"] * 100,
        color=F1_RED,
        edgecolor="#000",
        linewidth=0.4,
    )
    ax.set_ylabel("% Podios")
    ax.set_xlabel("Nacionalidad")
    ax.grid(axis="y")
    plt.xticks(rotation=20, ha="right")
    save(fig, "01d_podio_por_nacionalidad")

    print("Seccion 1 completada.")


# DOMINIO HISTÓRICO POR DÉCADAS
def eda_dominio_decadas(df: pd.DataFrame):
    section("2. Dominio de equipos y pilotos por decadas")

    df = df.copy()
    df["decade"] = (df["year"] // 10) * 10

    # Victorias por constructor por década
    fig, ax = plt.subplots(figsize=(14, 6))
    fig.suptitle("Victorias por constructor segun decada", fontweight="bold")
    wins = (
        df[df["is_winner"].fillna(False)]
        .groupby(["decade", "constructor_name"])
        .size()
        .reset_index(name="wins")
    )
    top_cons = wins.groupby("constructor_name")["wins"].sum().nlargest(8).index
    wins = wins[wins["constructor_name"].isin(top_cons)]
    pivot = wins.pivot(
        index="decade", columns="constructor_name", values="wins"
    ).fillna(0)
    pivot.plot(
        kind="bar",
        stacked=True,
        ax=ax,
        color=PALETTE[: len(pivot.columns)],
        edgecolor="#000",
        linewidth=0.3,
    )
    ax.set_xlabel("Decada")
    ax.set_ylabel("Victorias")
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    ax.grid(axis="y")
    plt.xticks(rotation=0)
    save(fig, "02a_victorias_constructor_decada")

    # Top piloto por década (tabla)
    top_drivers = (
        df[df["is_winner"].fillna(False)]
        .groupby(["decade", "driver_name"])
        .size()
        .reset_index(name="wins")
        .sort_values(["decade", "wins"], ascending=[True, False])
        .groupby("decade")
        .first()
        .reset_index()
    )
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle("Piloto con mas victorias por decada", fontweight="bold")
    ax.axis("off")
    table_data = [["Decada", "Piloto", "Victorias"]] + top_drivers[
        ["decade", "driver_name", "wins"]
    ].values.tolist()
    tbl = ax.table(
        cellText=table_data[1:],
        colLabels=table_data[0],
        cellLoc="center",
        loc="center",
        bbox=[0, 0, 1, 1],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_facecolor("#1a1a1a" if r > 0 else F1_RED)
        cell.set_edgecolor("#444")
        cell.set_text_props(color=F1_WHITE)
    save(fig, "02b_top_piloto_por_decada")

    # Evolución de puntos por temporada top 5 constructores
    top5 = df.groupby("constructor_name")["points"].sum().nlargest(5).index
    pts_year = (
        df[df["constructor_name"].isin(top5)]
        .groupby(["year", "constructor_name"])["points"]
        .sum()
        .reset_index()
    )
    fig, ax = plt.subplots(figsize=(14, 6))
    fig.suptitle(
        "Evolucion de puntos por temporada - Top 5 constructores", fontweight="bold"
    )
    for i, cons in enumerate(top5):
        sub = pts_year[pts_year["constructor_name"] == cons]
        ax.plot(sub["year"], sub["points"], label=cons, color=PALETTE[i], linewidth=1.8)
    ax.set_xlabel("Anio")
    ax.set_ylabel("Puntos totales")
    ax.legend(fontsize=9)
    ax.grid(True)
    save(fig, "02c_evolucion_puntos_constructores")

    # Top 10 pilotos históricos por victorias
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle("Top 10 pilotos por victorias totales (1950-2024)", fontweight="bold")
    top10 = (
        df[df["is_winner"].fillna(False)]
        .groupby("driver_name")
        .size()
        .nlargest(10)
        .reset_index(name="wins")
        .sort_values("wins")
    )
    bars = ax.barh(
        top10["driver_name"],
        top10["wins"],
        color=F1_GOLD,
        edgecolor="#000",
        linewidth=0.4,
    )
    ax.set_xlabel("Victorias")
    ax.grid(axis="x")
    for bar, val in zip(bars, top10["wins"]):
        ax.text(
            val + 0.3,
            bar.get_y() + bar.get_height() / 2,
            str(val),
            va="center",
            fontsize=9,
            color=F1_WHITE,
        )
    save(fig, "02d_top10_pilotos_victorias")

    print("Seccion 2 completada.")


# GRILLA VS RESULTADO FINAL
def eda_grilla_vs_resultado(df: pd.DataFrame, quali_df: pd.DataFrame):
    section("3. Posicion de grilla vs resultado final")

    sample = df.dropna(subset=["grid_position", "finish_position"]).sample(
        min(8000, len(df)), random_state=42
    )
    fig, ax = plt.subplots(figsize=(9, 8))
    fig.suptitle(
        "Posicion de grilla vs posicion final (muestra 8.000 carreras)",
        fontweight="bold",
    )
    ax.scatter(
        sample["grid_position"],
        sample["finish_position"],
        c=sample["is_podium"].map({True: F1_GOLD, False: "#444444"}),
        alpha=0.35,
        s=12,
        edgecolors="none",
    )
    ax.plot([1, 30], [1, 30], color=F1_RED, linestyle="--", linewidth=1.2)
    from matplotlib.lines import Line2D

    ax.legend(
        handles=[
            Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor=F1_GOLD,
                markersize=8,
                label="Podio",
            ),
            Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor="#444",
                markersize=8,
                label="No podio",
            ),
        ],
        fontsize=9,
    )
    ax.set_xlabel("Posicion de grilla")
    ax.set_ylabel("Posicion final")
    ax.grid(True)
    save(fig, "03a_scatter_grilla_vs_resultado")

    df2 = df.dropna(subset=["grid_position", "finish_position"]).copy()
    df2["mejoro"] = df2["finish_position"] <= df2["grid_position"]
    conv = (
        df2.groupby(
            pd.cut(
                df2["grid_position"],
                bins=[0, 1, 3, 5, 10, 20, 30],
                labels=["P1", "P2-3", "P4-5", "P6-10", "P11-20", "P21+"],
            ),
            observed=True,
        )["mejoro"].mean()
        * 100
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    fig.suptitle(
        "% de pilotos que mantuvieron o mejoraron posicion vs grilla", fontweight="bold"
    )
    ax.bar(
        conv.index.astype(str),
        conv.values,
        color=F1_RED,
        edgecolor="#000",
        linewidth=0.4,
    )
    ax.set_ylabel("% mantuvieron/mejoraron")
    ax.set_xlabel("Posicion de grilla")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.grid(axis="y")
    for i, v in enumerate(conv.values):
        ax.text(i, v + 0.5, f"{v:.1f}%", ha="center", fontsize=9, color=F1_WHITE)
    save(fig, "03b_conversion_grilla_resultado")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        "Correlacion grilla vs resultado: era clasica vs moderna", fontweight="bold"
    )
    for ax, (era_label, era_df) in zip(
        axes,
        [
            ("Era clasica (1950-1999)", df2[df2["year"] < 2000]),
            ("Era moderna (2000-2024)", df2[df2["year"] >= 2000]),
        ],
    ):
        sample_era = era_df.sample(min(4000, len(era_df)), random_state=0)
        ax.scatter(
            sample_era["grid_position"],
            sample_era["finish_position"],
            alpha=0.3,
            s=10,
            color=F1_RED,
            edgecolors="none",
        )
        corr = era_df[["grid_position", "finish_position"]].corr().iloc[0, 1]
        ax.set_title(f"{era_label}\nCorrelacion de Pearson: {corr:.3f}")
        ax.set_xlabel("Grilla")
        ax.set_ylabel("Posicion final")
        ax.plot([1, 30], [1, 30], "--", color=F1_GOLD, linewidth=1)
        ax.grid(True)
    save(fig, "03c_correlacion_por_era")

    prob = (
        df2.groupby("grid_position")["is_podium"]
        .mean()
        .reset_index()
        .query("grid_position <= 20")
    )
    px = pd.to_numeric(prob["grid_position"], errors="coerce")
    py = pd.to_numeric(prob["is_podium"], errors="coerce") * 100

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle("Probabilidad de podio segun posicion de grilla", fontweight="bold")

    ax.plot(
        px,
        py,
        color=F1_GOLD,
        linewidth=2.5,
        marker="o",
        markersize=5,
    )
    ax.fill_between(px, py, alpha=0.15, color=F1_GOLD)

    ax.set_xlabel("Posicion de grilla")
    ax.set_ylabel("Probabilidad de podio (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.grid(True)
    save(fig, "03d_probabilidad_podio_grilla")

    print("Seccion 3 completada.")


# IMPACTO DE LOS PIT STOPS
def eda_pit_stops(df: pd.DataFrame, pits_df: pd.DataFrame):
    section("4. Impacto de los pit stops en el resultado")

    merged = df.merge(pits_df, on=["race_key", "driver_key"], how="inner").dropna(
        subset=["finish_position", "total_pit_stops"]
    )

    merged["era"] = pd.cut(
        merged["year"],
        bins=[1949, 1993, 2010, 2024],
        labels=[
            "Pre-reabastecimiento (<1994)",
            "Reabastecimiento (1994-2010)",
            "Estrategia moderna (2011-2024)",
        ],
    )
    fig, ax = plt.subplots(figsize=(11, 5))
    fig.suptitle("Distribucion de cantidad de pit stops por era", fontweight="bold")
    for i, (era, grp) in enumerate(
        merged.dropna(subset=["era"]).groupby("era", observed=True)
    ):
        ax.hist(
            grp["total_pit_stops"],
            bins=range(0, 12),
            alpha=0.65,
            label=str(era),
            color=PALETTE[i],
            edgecolor="#000",
            linewidth=0.3,
            density=True,
        )
    ax.set_xlabel("Cantidad de pit stops")
    ax.set_ylabel("Densidad")
    ax.legend(fontsize=9)
    ax.grid(axis="y")
    save(fig, "04a_pitstops_por_era")

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle(
        "Posicion final promedio segun cantidad de pit stops", fontweight="bold"
    )
    avg_pos = (
        merged.query("total_pit_stops <= 6")
        .groupby("total_pit_stops")["finish_position"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    ax.errorbar(
        avg_pos["total_pit_stops"],
        avg_pos["mean"],
        yerr=avg_pos["std"],
        fmt="o-",
        color=F1_RED,
        linewidth=2,
        markersize=8,
        capsize=5,
        capthick=1.5,
    )
    ax.set_xlabel("Cantidad de pit stops")
    ax.set_ylabel("Posicion final promedio")
    ax.invert_yaxis()
    ax.grid(True)
    ax.set_xticks(avg_pos["total_pit_stops"])
    save(fig, "04b_posicion_vs_pitstops")

    fig, ax = plt.subplots(figsize=(13, 5))
    fig.suptitle(
        "Evolucion de la duracion promedio del pit stop (segundos)", fontweight="bold"
    )
    dur_year = (
        merged.dropna(subset=["avg_duration_s"])
        .groupby("year")["avg_duration_s"]
        .median()
        .reset_index()
        .query("year >= 1994")  # Comienzan los pits stops OJOOOO
    )
    x = pd.to_numeric(dur_year["year"], errors="coerce")
    y = pd.to_numeric(dur_year["avg_duration_s"], errors="coerce")

    # Graficamos usando las variables limpias
    ax.plot(x, y, color=F1_GOLD, linewidth=2)
    ax.fill_between(x, y, alpha=0.12, color=F1_GOLD)

    ax.set_xlabel("Anio")
    ax.set_ylabel("Duracion mediana (s)")
    ax.grid(True)
    save(fig, "04c_duracion_pitstop_por_anio")

    moderna = merged[merged["year"] >= 2011].copy()
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle(
        "Tasa de podio segun cantidad de pit stops (era moderna 2011-2024)",
        fontweight="bold",
    )
    podio_stops = (
        moderna.query("total_pit_stops <= 5")
        .groupby("total_pit_stops")["is_podium"]
        .mean()
        .reset_index()
    )
    bars = ax.bar(
        podio_stops["total_pit_stops"].astype(str),
        podio_stops["is_podium"] * 100,
        color=F1_RED,
        edgecolor="#000",
        linewidth=0.4,
    )
    ax.set_xlabel("Cantidad de pit stops")
    ax.set_ylabel("% con podio")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.grid(axis="y")
    for bar, val in zip(bars, podio_stops["is_podium"] * 100):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val + 0.3,
            f"{val:.1f}%",
            ha="center",
            fontsize=9,
            color=F1_WHITE,
        )
    save(fig, "04d_podio_vs_cantidad_stops")

    print("Seccion 4 completada.")


# CORRELACIÓN GENERAL (HEATMAP)
def eda_correlacion(df: pd.DataFrame):
    section("5. Matriz de correlacion entre variables numericas")

    num_cols = [
        "grid_position",
        "finish_position",
        "points",
        "laps_completed",
        "fastest_lap_time_s",
        "fastest_lap_speed",
        "quali_position",
        "best_quali_time_s",
        "total_stops",
        "avg_pit_duration_s",
    ]
    available = [c for c in num_cols if c in df.columns]
    corr = df[available].corr()

    fig, ax = plt.subplots(figsize=(12, 10))
    fig.suptitle(
        "Matriz de correlacion - Variables numericas del DW", fontweight="bold"
    )
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(
        corr,
        mask=mask,
        ax=ax,
        cmap="RdYlGn_r",
        center=0,
        vmin=-1,
        vmax=1,
        annot=True,
        fmt=".2f",
        annot_kws={"size": 8},
        linewidths=0.5,
        linecolor="#333",
        cbar_kws={"shrink": 0.8},
    )
    ax.set_xticklabels(ax.get_xticklabels(), rotation=35, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=9)
    save(fig, "05_heatmap_correlacion")
    print("Seccion 5 completada.")


# ANÁLISIS DE RANKING
def eda_ranking_analysis(ranking_df: pd.DataFrame, summary_lines: list):
    """
    Analiza que tan bien la posicion de grilla (proxy del modelo mas simple posible)
    predice el ranking final de pilotos dentro de cada carrera.
    Establece el baseline de Spearman que el modelo ML debera superar.
    """
    section("6. Analisis de ranking: grilla vs. posicion real (puente hacia ML)")

    df = ranking_df.dropna(subset=["grid_position", "finish_position"]).copy()

    # Calcular Spearman por carrera
    spearman_rows = []
    for race_id, group in df.groupby("race_id"):
        if len(group) < 5:
            continue
        corr, pval = stats.spearmanr(
            group["grid_position"].values, group["finish_position"].values
        )
        spearman_rows.append(
            {
                "race_id": race_id,
                "year": group["year"].iloc[0],
                "race_name": group["race_name"].iloc[0],
                "n_drivers": len(group),
                "spearman": corr,
                "pval": pval,
            }
        )

    spearman_df = pd.DataFrame(spearman_rows)
    median_sp = spearman_df["spearman"].median()
    mean_sp = spearman_df["spearman"].mean()
    sig_pct = (spearman_df["pval"] < 0.05).mean() * 100

    lines = [
        "",
        "── Ranking Analysis ──",
        f"  Carreras analizadas:                {len(spearman_df):,}",
        f"  Spearman mediano (grilla->finish):   {median_sp:.3f}",
        f"  Spearman medio   (grilla->finish):   {mean_sp:.3f}",
        f"  % carreras con correlacion sig.:     {sig_pct:.1f}%  (p < 0.05)",
        "  Interpretacion: este es el baseline que el modelo ML",
        "  debe superar en Spearman rank correlation.",
    ]
    for l in lines:
        print(l)
    summary_lines.extend(lines)

    # Distribución del Spearman por carrera
    fig, ax = plt.subplots(figsize=(11, 5))
    fig.suptitle(
        "Distribucion del coeficiente de Spearman (grilla -> posicion final)\n"
        "Baseline para el modelo ML",
        fontweight="bold",
    )
    ax.hist(
        spearman_df["spearman"].dropna(),
        bins=40,
        color=F1_RED,
        edgecolor="#000",
        linewidth=0.4,
        alpha=0.85,
    )
    ax.axvline(
        median_sp,
        color=F1_GOLD,
        linewidth=2,
        linestyle="--",
        label=f"Mediana: {median_sp:.3f}",
    )
    ax.axvline(0, color=F1_SILVER, linewidth=1, linestyle=":", alpha=0.6)
    ax.set_xlabel("Coeficiente de Spearman")
    ax.set_ylabel("Cantidad de carreras")
    ax.legend(fontsize=10)
    ax.grid(axis="y")
    save(fig, "06a_spearman_grilla_vs_finish")

    # Evolución del Spearman por temporada
    spearman_year = spearman_df.groupby("year")["spearman"].median().reset_index()
    fig, ax = plt.subplots(figsize=(13, 5))
    fig.suptitle(
        "Evolucion del Spearman (grilla -> posicion final) por temporada\n"
        "La grilla predice mejor el resultado en la era moderna?",
        fontweight="bold",
    )
    ax.plot(
        spearman_year["year"],
        spearman_year["spearman"],
        color=F1_GOLD,
        linewidth=2,
        marker="o",
        markersize=4,
    )
    ax.fill_between(
        spearman_year["year"], spearman_year["spearman"], alpha=0.12, color=F1_GOLD
    )
    ax.axhline(
        median_sp,
        color=F1_RED,
        linewidth=1.2,
        linestyle="--",
        label=f"Mediana global: {median_sp:.3f}",
    )
    ax.set_xlabel("Temporada")
    ax.set_ylabel("Spearman mediano")
    ax.set_ylim(0, 1)
    ax.legend(fontsize=9)
    ax.grid(True)
    save(fig, "06b_spearman_por_temporada")

    # Carreras más y menos predecibles
    top_pred = spearman_df.nlargest(10, "spearman")[["year", "race_name", "spearman"]]
    bot_pred = spearman_df.nsmallest(10, "spearman")[["year", "race_name", "spearman"]]

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle(
        "Carreras mas y menos predecibles por la grilla (Spearman)", fontweight="bold"
    )
    for ax, data, title, color in [
        (axes[0], top_pred.sort_values("spearman"), "Top 10 mas predecibles", F1_GOLD),
        (
            axes[1],
            bot_pred.sort_values("spearman", ascending=False),
            "Top 10 menos predecibles",
            F1_RED,
        ),
    ]:
        labels = [f"{int(r.year)} {r.race_name[:20]}" for r in data.itertuples()]
        ax.barh(
            labels,
            data["spearman"].values,
            color=color,
            edgecolor="#000",
            linewidth=0.4,
        )
        ax.set_xlabel("Spearman")
        ax.set_title(title)
        ax.grid(axis="x")
        ax.set_xlim(-0.2, 1.05)
        ax.axvline(0, color=F1_SILVER, linewidth=0.8, linestyle=":")
    save(fig, "06c_carreras_predecibles")

    print("Seccion 6 completada.")
    return spearman_df


def main():
    print("\n" + "=" * 55)
    print("EDA - F1 Data Warehouse (1950-2024)")
    print("TP Final - Bases de Datos Masivas - UNLu")
    print("=" * 55)

    engine = get_engine()
    dfs = load_data(engine)
    main_df = dfs["main"]
    pits_df = dfs["pits"]
    quali_df = dfs["quali"]
    ranking_df = dfs["ranking"]

    # Enriquecemos vista principal con pit stop data
    main_df = main_df.merge(
        pits_df.rename(
            columns={
                "total_stops": "total_stops",
                "avg_duration_s": "avg_pit_duration_s",
            }
        ),
        on=["race_key", "driver_key"],
        how="left",
    )

    summary_lines = []

    eda_overview(main_df, summary_lines)
    eda_podium_factors(main_df)
    eda_dominio_decadas(main_df)
    eda_grilla_vs_resultado(main_df, quali_df)
    eda_pit_stops(main_df, pits_df)
    eda_correlacion(main_df)
    eda_ranking_analysis(ranking_df, summary_lines)

    # Guardamos resumen textual
    summary_path = os.path.join(OUT_DIR, "eda_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    print(f"\n{'=' * 55}")
    print("EDA completado.")
    print(f"Resultados en: {OUT_DIR}")
    print("Graficos generados: 15 PNGs")
    print("Resumen estadistico: eda_summary.txt")
    print(f"{'=' * 55}\n")


if __name__ == "__main__":
    main()
