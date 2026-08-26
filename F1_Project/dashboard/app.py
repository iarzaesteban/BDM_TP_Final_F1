import os
import json
import base64
from pathlib import Path

import numpy as np
import pandas as pd
from dash import Dash, dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from scipy.stats import spearmanr
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = Path("/app/F1_Project")
ML_DIR   = BASE_DIR / "data_processed" / "ml"
OUT_DIR  = ML_DIR / "outputs"

load_dotenv()

def get_engine():
    user     = os.getenv("POSTGRES_USER",     "f1_admin")
    password = os.getenv("POSTGRES_PASSWORD", "f1_pass")
    host     = os.getenv("POSTGRES_HOST",     "postgres")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "f1_dwh")
    schema   = os.getenv("DB_SCHEMA",         "f1_dw")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        connect_args={"options": f"-csearch_path={schema}"},
    )

BG      = "#0d0d0d"
BG2     = "#1a1a1a"
BG3     = "#252525"
F1_RED  = "#e10600"
F1_GOLD = "#ffd700"
F1_WHT  = "#f5f5f5"
F1_GRY  = "#888888"
ACCENT  = "#00d4ff"

PLOTLY_BASE = dict(
    paper_bgcolor=BG2,
    plot_bgcolor=BG3,
    font=dict(color=F1_WHT, size=11),
    xaxis=dict(gridcolor="#2a2a2a", zerolinecolor="#444", tickfont=dict(color=F1_GRY)),
    yaxis=dict(gridcolor="#2a2a2a", zerolinecolor="#444", tickfont=dict(color=F1_GRY)),
    legend=dict(bgcolor=BG2, bordercolor="#333"),
    margin=dict(l=50, r=20, t=50, b=50),
)

# Spearman baseline del test set 2022-2024
SPEARMAN_BASELINE_TEST = 0.6293
SPEARMAN_BASELINE_EDA  = 0.788

def load_all_data():
    engine = get_engine()
    schema = os.getenv("DB_SCHEMA", "f1_dw")

    df = pd.read_sql(f"""
        SELECT year, round, race_name, circuit_name, circuit_country,
               driver_name, driver_nationality, constructor_name,
               grid_position, finish_position, points,
               COALESCE(is_podium::int, 0) AS is_podium,
               COALESCE(is_winner::int, 0) AS is_winner
        FROM {schema}.vw_race_analysis
        WHERE year >= 1994 AND grid_position IS NOT NULL AND grid_position > 0
        ORDER BY year, round, grid_position
    """, engine)

    df_drv = pd.read_sql(f"""
        SELECT year, driver_name, constructor_name, races,
               total_points, wins, podiums,
               ROUND(avg_grid::numeric, 1) AS avg_grid
        FROM {schema}.vw_driver_season
        ORDER BY year, total_points DESC
    """, engine)

    df_con = pd.read_sql(f"""
        SELECT year, constructor_name, entries, total_points, wins, podiums
        FROM {schema}.vw_constructor_season
        ORDER BY year, total_points DESC
    """, engine)

    df_preds = pd.read_csv(ML_DIR / "test_predictions.csv")

    with open(ML_DIR / "ml_metrics.json", encoding="utf-8") as fh:
        metrics_json = json.load(fh)

    fi_df = pd.read_csv(ML_DIR / "feature_importance.csv")

    return df, df_drv, df_con, df_preds, metrics_json, fi_df


print("Cargando datos desde PostgreSQL y archivos ML...")
df, df_drv, df_con, df_preds, metrics_json, fi_df = load_all_data()
print(f"  DW: {len(df):,} registros | Predicciones ML: {len(df_preds):,}")

# Derivados
TEST_YEARS   = sorted(df_preds["year"].unique())
ALL_DRIVERS  = sorted(df_drv["driver_name"].unique())
ALL_CONSTR   = sorted(df_con["constructor_name"].unique())

race_names = (
    df[["year", "round", "race_name"]]
    .drop_duplicates()
    .set_index(["year", "round"])["race_name"]
    .to_dict()
)

def _img(name: str) -> str:
    p = OUT_DIR / name
    if not p.exists():
        return ""
    with open(p, "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()

IMG = {
    "roc":     _img("01_roc_curves.png"),
    "cm":      _img("02_confusion_matrices.png"),
    "fi":      _img("03_feature_importance.png"),
    "spear":   _img("04_spearman_distribution.png"),
    "metrics": _img("05_metrics_comparison.png"),
    "sptime":  _img("06_spearman_over_time.png"),
}

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.FONT_AWESOME],
    title="F1 · BDM Dashboard · UNLu",
    suppress_callback_exceptions=True,
)
server = app.server


def kpi(label, value, icon="fa-flag-checkered", color=F1_RED):
    return dbc.Card(dbc.CardBody([
        html.I(className=f"fa {icon} fa-2x mb-2", style={"color": color}),
        html.H3(value, className="fw-bold mb-0"),
        html.Small(label, style={"color": F1_GRY}),
    ], className="text-center py-3"),
    style={"background": BG2, "border": f"1px solid {color}44", "borderRadius": "8px"})


def stitle(text):
    return html.H6(
        text,
        className="mt-4 mb-2 pb-1",
        style={"color": F1_WHT, "borderBottom": "1px solid #333"},
    )


def tab_resumen():
    n_races   = int(df.groupby(["year", "round"]).ngroups)
    n_drivers = int(df["driver_name"].nunique())
    n_constr  = int(df["constructor_name"].nunique())
    n_seasons = int(df["year"].nunique())

    top10 = (
        df.groupby("driver_name")["is_winner"].sum()
          .nlargest(10).reset_index()
          .rename(columns={"is_winner": "Victorias"})
    )
    fig_top = go.Figure(go.Bar(
        x=top10["Victorias"], y=top10["driver_name"],
        orientation="h", marker_color=F1_RED,
        text=top10["Victorias"], textposition="outside",
        hovertemplate="<b>%{y}</b><br>Victorias: %{x}<extra></extra>",
    ))
    fig_top.update_layout(
        **{**PLOTLY_BASE, "yaxis": dict(autorange="reversed", **PLOTLY_BASE["yaxis"])},
        title="Top 10 Pilotos por Victorias (1994–2024)",
        height=360, xaxis_title="Victorias",
    )

    df_dec = df.copy()
    df_dec["decade"] = (df_dec["year"] // 10 * 10).astype(str) + "s"
    top6c = df_dec.groupby("constructor_name")["is_winner"].sum().nlargest(6).index
    dec_wins = (
        df_dec[df_dec["constructor_name"].isin(top6c)]
        .groupby(["decade", "constructor_name"])["is_winner"].sum()
        .reset_index().rename(columns={"is_winner": "Victorias"})
    )
    fig_dec = px.bar(
        dec_wins, x="decade", y="Victorias", color="constructor_name",
        barmode="stack", text_auto=True,
        color_discrete_sequence=["#e10600","#ffd700","#00d4ff","#00ff88","#ff6b35","#c0c0c0"],
        labels={"constructor_name": "Constructor", "decade": "Década"},
    )
    fig_dec.update_layout(**PLOTLY_BASE, title="Dominio por Constructor y Década (1994–2024)", height=360)

    return dbc.Container([
        dbc.Row([
            dbc.Col(kpi("Carreras",        f"{n_races:,}",   "fa-flag-checkered", F1_RED),   width=3),
            dbc.Col(kpi("Pilotos únicos",  f"{n_drivers}",   "fa-user",           ACCENT),   width=3),
            dbc.Col(kpi("Constructores",   f"{n_constr}",    "fa-car",            "#00ff88"),width=3),
            dbc.Col(kpi("Temporadas",      f"{n_seasons}",   "fa-calendar",       F1_GOLD),  width=3),
        ], className="mb-4 mt-2 g-3"),
        dbc.Row([
            dbc.Col([stitle("Top 10 Pilotos"), dcc.Graph(figure=fig_top, config={"displayModeBar": False})], width=5),
            dbc.Col([stitle("Constructor por Década"), dcc.Graph(figure=fig_dec, config={"displayModeBar": False})], width=7),

        ]),
    ], fluid=True)


MODEL_OPTS = [
    {"label": "Random Forest",       "value": "random_forest"},
    {"label": "Logistic Regression", "value": "logistic_regression"},
    {"label": "XGBoost",             "value": "xgboost"},
    {"label": "SVM RBF",             "value": "svm_rbf"},
]

def tab_predictor():
    return dbc.Container([
        dbc.Card(dbc.CardBody(dbc.Row([
            dbc.Col([
                html.Label("Temporada", className="small text-secondary"),
                dcc.Dropdown(
                    [{"label": str(y), "value": y} for y in TEST_YEARS],
                    value=TEST_YEARS[-1], id="pred-year", clearable=False,
                ),
            ], width=2),
            dbc.Col([
                html.Label("Gran Premio", className="small text-secondary"),
                dcc.Dropdown(id="pred-round", placeholder="Seleccioná una carrera…"),
            ], width=5),
            dbc.Col([
                html.Label("Modelo ML", className="small text-secondary"),
                dcc.Dropdown(MODEL_OPTS, value="random_forest", id="pred-model", clearable=False),
            ], width=4),
        ], className="g-3")), style={"background": BG2, "border": "1px solid #333"}, className="mb-3 mt-2"),

        html.Div(id="pred-header", className="mb-3"),

        dbc.Row([
            dbc.Col([
                stitle("Probabilidad de Podio Predicha vs. Resultado Real"),
                html.Small(
                    "Barras rojas = podio real  ·  Etiqueta = posición de llegada  ·  Línea punteada = umbral 0.5",
                    style={"color": F1_GRY},
                ),
                dcc.Graph(id="pred-chart", config={"displayModeBar": False}),
            ], width=8),
            dbc.Col([
                stitle("Tabla de Resultados"),
                html.Div(id="pred-table"),
            ], width=4),
        ]),

        dbc.Alert([
            html.Strong("Nota metodológica: "),
            "El modelo fue entrenado con datos 1994–2021 y se evalúa en 2022–2024 (split temporal estricto). ",
            f"Baseline Spearman grilla sola en test set: {SPEARMAN_BASELINE_TEST:.4f}. ",
            f"Random Forest mediano: 0.6795 — supera el baseline.",
        ], color="dark", className="mt-3 small", style={"border": f"1px solid {F1_GRY}33"}),
    ], fluid=True)


DEFAULT_DRIVERS = [d for d in ["Lewis Hamilton", "Max Verstappen", "Sebastian Vettel", "Fernando Alonso"] if d in ALL_DRIVERS]
DEFAULT_CONSTR  = [c for c in ["Mercedes", "Red Bull", "Ferrari", "McLaren"] if c in ALL_CONSTR]

def tab_evolucion():
    return dbc.Container([
        dbc.Row([
            dbc.Col([
                stitle("Puntos por Temporada — Pilotos"),
                dcc.Dropdown(ALL_DRIVERS, DEFAULT_DRIVERS, id="evo-drivers", multi=True, placeholder="Seleccioná pilotos…"),
                dcc.Graph(id="evo-driver-pts", config={"displayModeBar": False}),
            ], width=6),
            dbc.Col([
                stitle("Puntos por Temporada — Constructores"),
                dcc.Dropdown(ALL_CONSTR, DEFAULT_CONSTR, id="evo-constrs", multi=True, placeholder="Seleccioná constructores…"),
                dcc.Graph(id="evo-constr-pts", config={"displayModeBar": False}),
            ], width=6),
        ]),
        dbc.Row([
            dbc.Col([
                stitle("Victorias Acumuladas — Top 8 Pilotos (1994–2024)"),
                dcc.Graph(id="evo-wins-cum", config={"displayModeBar": False}),
            ], width=12),
        ]),
    ], fluid=True)


def tab_modelo():
    rows = []
    for m in metrics_json["models"]:
        beats = "Sí" if m["spearman_median"] > SPEARMAN_BASELINE_TEST else "No"
        rows.append({
            "Modelo":            m["model"],
            "ROC-AUC":           f"{m['roc_auc']:.4f}",
            "F1-Score":          f"{m['f1_score']:.4f}",
            "Accuracy":          f"{m['accuracy']:.4f}",
            "Spearman Med.":     f"{m['spearman_median']:.4f}",
            "% sig. (p<0.05)":   f"{m['spearman_sig_pct']:.1f}%",
            "Supera baseline":   beats,
        })

    def img_card(src, title, w=6):
        if not src:
            return dbc.Col(dbc.Alert("Imagen no disponible", color="warning"), width=w)
        return dbc.Col([
            html.P(title, className="small text-secondary text-center mb-1"),
            html.Img(src=src, style={"width": "100%", "borderRadius": "6px", "border": "1px solid #333"}),
        ], width=w, className="mb-3")

    return dbc.Container([
        stitle("Métricas de Evaluación — Test 2022–2024"),
        dbc.Row([dbc.Col([
            html.Small(
                f"Baseline grilla (test set 2022-2024): Spearman = {SPEARMAN_BASELINE_TEST} | "
                f"Baseline grilla (EDA histórico 1994-2024): Spearman = {SPEARMAN_BASELINE_EDA}",
                style={"color": F1_GRY},
            ),
            html.Div(dash_table.DataTable(
                data=rows,
                columns=[{"name": c, "id": c} for c in rows[0]],
                style_header={"backgroundColor": BG3, "color": F1_GOLD, "fontWeight": "bold"},
                style_data={"backgroundColor": BG2, "color": F1_WHT},
                style_cell={"textAlign": "center", "padding": "8px", "fontSize": "0.85rem"},
                style_data_conditional=[
                    {"if": {"filter_query": '{Supera baseline} = "Sí"'},
                     "backgroundColor": "#1a2e00", "color": "#00ff88"},
                ],
            ), className="mt-2"),
        ], width=12)]),

        stitle("Visualizaciones Generadas por evaluate.py"),
        dbc.Row([
            img_card(IMG["roc"],     "Curvas ROC — 4 modelos"),
            img_card(IMG["cm"],      "Matrices de Confusión"),
            img_card(IMG["fi"],      "Importancia de Features"),
            img_card(IMG["spear"],   "Distribución Spearman por Carrera"),
            img_card(IMG["metrics"], "Comparación de Métricas"),
            img_card(IMG["sptime"],  "Evolución Spearman en el Tiempo"),
        ]),
    ], fluid=True)


NAVBAR = dbc.Navbar(dbc.Container([
    html.Span("🏎", style={"fontSize": "1.6rem"}),
    dbc.NavbarBrand(
        "F1 data warehouse - BDM TP Final",
        className="ms-2 fw-bold",
        style={"fontSize": "1rem"},
    ),
    dbc.Badge("Dash 2026", color="danger", className="ms-auto"),
], fluid=True), color=BG2, dark=True,
style={"borderBottom": f"3px solid {F1_RED}", "padding": "0.6rem 1rem"})

app.layout = html.Div([
    NAVBAR,
    dbc.Tabs([
        dbc.Tab(tab_resumen(),   label="Resumen",      tab_id="tab-1", className="p-3"),
        dbc.Tab(tab_predictor(), label="Predictor ML", tab_id="tab-2", className="p-3"),
        dbc.Tab(tab_evolucion(), label="Evolución",    tab_id="tab-3", className="p-3"),
        dbc.Tab(tab_modelo(),    label="Modelo ML",    tab_id="tab-4", className="p-3"),
    ], active_tab="tab-1",
       style={"background": BG, "borderBottom": "1px solid #222"}),
], style={"background": BG, "minHeight": "100vh"})


@app.callback(
    Output("pred-round", "options"),
    Output("pred-round", "value"),
    Input("pred-year",   "value"),
)
def cb_rounds(year):
    if year is None:
        return [], None
    rounds = sorted(df_preds[df_preds["year"] == year]["round"].unique())
    opts = [
        {"label": f"Ronda {r} — {race_names.get((year, r), '')}", "value": int(r)}
        for r in rounds
    ]
    return opts, opts[0]["value"] if opts else None


@app.callback(
    Output("pred-header",  "children"),
    Output("pred-chart",   "figure"),
    Output("pred-table",   "children"),
    Input("pred-year",     "value"),
    Input("pred-round",    "value"),
    Input("pred-model",    "value"),
)
def cb_predictor(year, round_, model_safe):
    empty = go.Figure()
    empty.update_layout(**PLOTLY_BASE, title="← Seleccioná una carrera")
    if not (year and round_ and model_safe):
        return "", empty, ""

    prob_col = f"prob_{model_safe}"
    pred_col = f"pred_{model_safe}"
    race = df_preds[(df_preds["year"] == year) & (df_preds["round"] == round_)].copy()
    if race.empty or prob_col not in race.columns:
        return "", empty, "Sin datos"

    # Rank predictions
    race["rank_pred"]   = race[prob_col].rank(ascending=False, method="min").astype(int)
    race["rank_actual"] = race["finish_position"].fillna(99).rank(method="first").astype(int)

    # Spearman
    corr, pval = spearmanr(race[prob_col], -race["finish_position"].fillna(99))
    sig  = "p<0.001" if pval < 0.001 else f"p={pval:.3f}"
    beats = corr > SPEARMAN_BASELINE_TEST
    badge_color = "success" if beats else "warning"
    badge_text  = f"Spearman = {corr:.4f}  ({sig})  {'supera baseline' if beats else f'baseline={SPEARMAN_BASELINE_TEST}'}"

    gp_name = race_names.get((year, round_), f"Ronda {round_}")

    header = dbc.Row([
        dbc.Col(html.H5(f"{year} — {gp_name}", style={"color": F1_WHT, "margin": 0}), width="auto"),
        dbc.Col(dbc.Badge(badge_text, color=badge_color, className="ms-2 fs-6 align-self-center"), width="auto"),
    ], align="center")

    race_sorted = race.sort_values(prob_col, ascending=True)

    bar_colors = [
        F1_RED   if row["is_podium"] == 1
        else ACCENT if row[pred_col] == 1
        else "#444"
        for _, row in race_sorted.iterrows()
    ]

    finish_labels = [
        str(int(row["finish_position"])) if pd.notna(row["finish_position"]) else "DNF"
        for _, row in race_sorted.iterrows()
    ]

    fig = go.Figure(go.Bar(
        x=race_sorted[prob_col],
        y=race_sorted["driver_name"],
        orientation="h",
        marker_color=bar_colors,
        text=finish_labels,
        textposition="outside",
        customdata=np.column_stack([
            race_sorted["finish_position"].fillna(99).values,
            race_sorted["grid_position"].values,
            race_sorted["is_podium"].values,
        ]),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "P(podio) predicha: %{x:.4f}<br>"
            "Llegada real: %{customdata[0]:.0f}<br>"
            "Grilla: %{customdata[1]:.0f}<extra></extra>"
        ),
    ))
    fig.update_layout(
        **{**PLOTLY_BASE,
           "xaxis": dict(title="P(podio) predicha", range=[0, 1.05], **PLOTLY_BASE["xaxis"]),
           "yaxis": dict(title="", **PLOTLY_BASE["yaxis"])},
        title=f"Probabilidad de Podio — {gp_name} {year}",
        height=max(380, len(race) * 22),
        shapes=[dict(
            type="line", x0=0.5, x1=0.5, y0=-0.5, y1=len(race) - 0.5,
            line=dict(color=ACCENT, width=1.5, dash="dot"),
        )],
        annotations=[dict(
            x=0.52, y=len(race) - 1,
            text="umbral 0.5", showarrow=False,
            font=dict(color=ACCENT, size=9),
        )],
    )

    fig.add_annotation(
        x=0.98, y=0.02, xref="paper", yref="paper", xanchor="right",
        text="<span style='color:#e10600'>■</span> Podio real  "
             "<span style='color:#00d4ff'>■</span> Pred. podio  "
             "<span style='color:#444'>■</span> Pred. fuera",
        showarrow=False, font=dict(size=9, color=F1_GRY),
        bgcolor=BG2, bordercolor="#333",
    )

    tbl = race.sort_values("rank_pred")[[
        "driver_name", "grid_position", "finish_position",
        prob_col, "rank_pred", "rank_actual",
    ]].rename(columns={
        "driver_name":    "Piloto",
        "grid_position":  "Grilla",
        "finish_position":"Llegada",
        prob_col:         "P(podio)",
        "rank_pred":      "Rank Pred.",
        "rank_actual":    "Rank Real",
    })
    tbl["P(podio)"]  = tbl["P(podio)"].apply(lambda x: f"{x:.3f}")
    tbl["Llegada"]   = tbl["Llegada"].apply(lambda x: str(int(x)) if pd.notna(x) and x != 99 else "DNF")

    table = dash_table.DataTable(
        data=tbl.to_dict("records"),
        columns=[{"name": c, "id": c} for c in tbl.columns],
        style_header={"backgroundColor": BG3, "color": F1_GOLD,
                      "fontWeight": "bold", "fontSize": "0.72rem", "padding": "4px"},
        style_data={"backgroundColor": BG2, "color": F1_WHT, "fontSize": "0.75rem"},
        style_cell={"textAlign": "center", "padding": "3px 5px", "whiteSpace": "normal"},
        style_data_conditional=[
            {"if": {"filter_query": "{Llegada} = '1' || {Llegada} = '2' || {Llegada} = '3'"},
             "backgroundColor": "#2a1f00", "color": F1_GOLD},
        ],
        page_size=25,
        sort_action="native",
    )

    return header, fig, table


SEQ_COLORS = [F1_RED, F1_GOLD, ACCENT, "#00ff88", "#ff6b35", "#c0c0c0", "#aa44ff", "#ff9900"]


@app.callback(Output("evo-driver-pts", "figure"), Input("evo-drivers", "value"))
def cb_drv_pts(drivers):
    fig = go.Figure()
    if not drivers:
        fig.update_layout(**PLOTLY_BASE, title="Seleccioná pilotos")
        return fig
    for i, d in enumerate(drivers):
        row = df_drv[df_drv["driver_name"] == d].sort_values("year")
        fig.add_trace(go.Scatter(
            x=row["year"], y=row["total_points"], mode="lines+markers",
            name=d, line=dict(width=2, color=SEQ_COLORS[i % len(SEQ_COLORS)]),
            hovertemplate=f"<b>{d}</b><br>%{{x}}: %{{y}} pts<extra></extra>",
        ))
    fig.update_layout(
        **PLOTLY_BASE, title="Puntos por Temporada",
        xaxis_title="Año", yaxis_title="Puntos", height=320,
    )
    return fig


@app.callback(Output("evo-constr-pts", "figure"), Input("evo-constrs", "value"))
def cb_con_pts(constrs):
    fig = go.Figure()
    if not constrs:
        fig.update_layout(**PLOTLY_BASE, title="Seleccioná constructores")
        return fig
    for i, c in enumerate(constrs):
        row = df_con[df_con["constructor_name"] == c].sort_values("year")
        fig.add_trace(go.Scatter(
            x=row["year"], y=row["total_points"], mode="lines+markers",
            name=c, line=dict(width=2, color=SEQ_COLORS[i % len(SEQ_COLORS)]),
            hovertemplate=f"<b>{c}</b><br>%{{x}}: %{{y}} pts<extra></extra>",
        ))
    fig.update_layout(
        **PLOTLY_BASE, title="Puntos por Temporada (Constructores)",
        xaxis_title="Año", yaxis_title="Puntos", height=320,
    )
    return fig


@app.callback(Output("evo-wins-cum", "figure"), Input("evo-drivers", "value"))
def cb_wins_cum(_):
    top8 = (
        df.groupby("driver_name")["is_winner"].sum()
          .nlargest(8).index.tolist()
    )
    fig = go.Figure()
    for i, d in enumerate(top8):
        sub = df[df["driver_name"] == d].sort_values(["year", "round"])
        by_year = sub.groupby("year")["is_winner"].sum().cumsum().reset_index()
        fig.add_trace(go.Scatter(
            x=by_year["year"], y=by_year["is_winner"], mode="lines",
            name=d, line=dict(width=2.5, color=SEQ_COLORS[i % len(SEQ_COLORS)]),
        ))
    fig.update_layout(
        **PLOTLY_BASE, title="Victorias Acumuladas — Top 8 Pilotos (1994–2024)",
        xaxis_title="Año", yaxis_title="Victorias acumuladas", height=380,
    )
    return fig


if __name__ == "__main__":
    port  = int(os.getenv("DASH_PORT",  8050))
    debug = os.getenv("DASH_DEBUG", "false").lower() == "true"
    print(f"Dashboard corriendo en http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=debug)
