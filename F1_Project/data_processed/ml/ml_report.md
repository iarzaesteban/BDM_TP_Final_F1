# Resultados ML — Predicción de Podio F1
### TP Final BDM · UNLu · Junio 2026

---

## Configuración del experimento

| Parámetro | Valor |
|---|---|
| Período de datos | 1994–2024 |
| Entrenamiento | 1994–2021 (10.909 registros) |
| Test | 2022–2024 (1.319 registros) |
| Variable objetivo | `is_podium` (top 3) — clasificación binaria |
| Clases train | 14.0% podios / 86.0% no podios |
| Clases test | 15.5% podios / 84.5% no podios |
| Manejo de desbalance | `class_weight='balanced'` (LR, RF, SVM) · `scale_pos_weight` (XGBoost) |
| Baseline a superar | Spearman mediano = 0.629 (posición de grilla sola, test 2022–2024) |

### Features — solo variables pre-carrera

| Feature | Descripción |
|---|---|
| `grid_position` | Posición de largada en la grilla |
| `quali_position` | Posición en clasificación (nulos imputados con `grid_position`) |
| `best_quali_time_s` | Mejor tiempo en qualy en segundos (nulos imputados con mediana por año) |
| `started_top3` | Flag: salida desde posición 1–3 |
| `started_top10` | Flag: salida desde posición 1–10 |
| `grid_quali_diff` | Diferencia grilla − quali (positivo = penalización de grilla) |
| `round` | Número de carrera en la temporada |
| `constructor_name_freq` | Frecuencia histórica del constructor (proxy de calidad del equipo) |
| `driver_nationality_freq` | Frecuencia histórica de la nacionalidad del piloto |
| `circuit_country_freq` | Frecuencia histórica del país del circuito |

Variables post-carrera excluidas para evitar data leakage: `finish_position`, `points`, `laps_completed`, `race_time_ms`, `status`.

---

## Modelos entrenados

| Modelo | Hiperparámetros principales |
|---|---|
| Logistic Regression | `C=1.0`, `class_weight='balanced'`, `solver='lbfgs'` |
| Random Forest | `n_estimators=300`, `max_depth=8`, `min_samples_leaf=10`, `class_weight='balanced'` |
| SVM RBF | `C=1.0`, `kernel='rbf'`, `gamma='scale'`, `class_weight='balanced'`, `probability=True` |
| XGBoost | `n_estimators=300`, `max_depth=5`, `lr=0.05`, `scale_pos_weight=6.1` |

Todos los modelos entrenados con features normalizadas (`StandardScaler`).

---

## Resultados en el test set (2022–2024)

| Modelo | ROC-AUC | F1-Score | Accuracy | Spearman | Supera baseline |
|---|---|---|---|---|---|
| Logistic Regression | 0.9170 | 0.6341 | 0.8408 | 0.6540 | si |
| Random Forest | 0.9126 | 0.6503 | 0.8597 | 0.6795 | si |
| SVM RBF | 0.8884 | 0.6295 | 0.8438 | 0.4994 | no |
| XGBoost | 0.9090 | 0.6409 | 0.8590 | 0.6549 | si |
| **Baseline (grilla sola, test 2022–2024)** | — | — | — | **0.629** | — |

El Spearman se calcula por carrera: para cada GP se rankea a los pilotos según la probabilidad predicha y se correlaciona ese ranking con el ranking real de llegada.

**Random Forest** obtuvo el mejor Spearman (0.6795) y el mejor F1 (0.6503). Logistic Regression y XGBoost también superaron el baseline. **SVM RBF** no logró superar el baseline en ranking.

---

## Importancia de features

### Random Forest (impureza de Gini)

| Feature | Importancia |
|---|---|
| `quali_position` | 0.3232 |
| `grid_position` | 0.2738 |
| `started_top10` | 0.1239 |
| `started_top3` | 0.1071 |
| `constructor_name_freq` | 0.0708 |
| `best_quali_time_s` | 0.0328 |
| `driver_nationality_freq` | 0.0263 |
| `circuit_country_freq` | 0.0186 |
| `round` | 0.0169 |
| `grid_quali_diff` | 0.0065 |

### XGBoost (ganancia)

| Feature | Importancia |
|---|---|
| `started_top10` | 0.3372 |
| `quali_position` | 0.2198 |
| `grid_position` | 0.1628 |
| `started_top3` | 0.1344 |
| `constructor_name_freq` | 0.0318 |
| `circuit_country_freq` | 0.0246 |
| `best_quali_time_s` | 0.0245 |
| `driver_nationality_freq` | 0.0228 |
| `round` | 0.0215 |
| `grid_quali_diff` | 0.0206 |

---

## Visualizaciones generadas

| Archivo | Descripción |
|---|---|
| `01_roc_curves.png` | Curvas ROC de los 4 modelos |
| `02_confusion_matrices.png` | Matrices de confusión (porcentaje y conteos absolutos) |
| `03_feature_importance.png` | Importancia de variables (Random Forest y XGBoost) |
| `04_spearman_distribution.png` | Distribución del Spearman por carrera por modelo |
| `05_metrics_comparison.png` | Comparación de ROC-AUC, F1 y Spearman |
| `06_spearman_over_time.png` | Evolución del Spearman durante el test 2022–2024 |

---

## Análisis

El Spearman mediano es la métrica principal porque el objetivo es producir un **ranking de pilotos por carrera**, no solo una clasificación binaria. El baseline de 0.629 se calculó sobre el mismo período de evaluación usando únicamente la posición de grilla como predictor.

El período 2022–2024 coincide con la introducción del nuevo reglamento de efecto suelo, que generó mayor variabilidad en los resultados respecto a temporadas anteriores. El modelo fue entrenado en la era Mercedes (hasta 2021) y evaluado en la era Verstappen/Red Bull, lo que hace el test deliberadamente más exigente.

La clase positiva (podio) representa el 15.5% del test set. Un clasificador trivial que siempre prediga "no podio" tendría ~84.5% de accuracy, por lo que esa métrica sola no es representativa. Se priorizan ROC-AUC y F1 junto con el Spearman.

Algo que llama la atención: Logistic Regression superó a XGBoost en ROC-AUC (0.917 vs 0.909), lo que sugiere que la relación entre los features y el podio es bastante lineal. SVM tardó considerablemente más en entrenar que los otros tres modelos — dado que además queda último en Spearman, no justifica el costo computacional.

Limitaciones: la frecuencia histórica del constructor captura dominio acumulado, no el estado actual del equipo — Red Bull 2022-2024 se beneficia de historial positivo aunque no era el dominante cuando se entrenó. Variables de rendimiento en carrera (pit stops, tiempos de vuelta) podrían mejorar el modelo pero implicarían data leakage en un sistema pre-carrera.
