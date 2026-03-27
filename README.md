# BDM_TP_Final_F1

# Análisis y Predicción del Rendimiento en la Fórmula 1
### Data Warehouse · Aprendizaje Automático · Visualización Interactiva

**Trabajo Final Integrador — Bases de Datos Masivas (11088)**  
Universidad Nacional de Luján · Departamento de Ciencias Básicas  
Licenciatura en Sistemas de Información · Plan 17.13

---

## Descripción del proyecto

Este trabajo propone el desarrollo de una solución analítica integral sobre datos históricos de la Fórmula 1 (1950–2024). A partir de la construcción de un **Data Warehouse** con esquema estrella sobre PostgreSQL, se implementa un pipeline completo que incluye ETL, análisis exploratorio, modelado predictivo con ranking de pilotos y un dashboard interactivo desarrollado con Dash (Python).

El eje central del trabajo es el **forecasting de resultados de carrera**: el modelo estima la probabilidad de podio de cada piloto dado el contexto previo a la carrera (circuito, grilla de largada, constructor, historial), produciendo un ranking predicho que se compara con el resultado real mediante correlación de Spearman.

---

## Objetivos

1. Integrar y estructurar datos históricos de F1 en un Data Warehouse (esquema estrella, PostgreSQL).
2. Realizar análisis exploratorio respondiendo preguntas de investigación concretas sobre rendimiento en F1.
3. Desarrollar modelos de aprendizaje automático para **ranking predictivo de pilotos** por carrera.
4. Evaluar la correlación entre el ranking predicho y el ranking real (Spearman rank correlation).
5. Construir un dashboard interactivo que visualice el podio predicho vs. real por carrera.

---

## Dataset

Kaggle - Formula 1 World Championship
https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020

Ergast Motor Racing API
https://ergast.com/mrd/

## Arquitectura

```
[ Kaggle CSV / Ergast API ]
          │
          ▼
  ┌───────────────┐
  │  ETL Fase 1   │  limpieza, estandarización, conversión de tipos
  │  (Python)     │
  └──────┬────────┘
         │
         ▼
  ┌───────────────┐
  │  ETL Fase 2   │  carga al DW, mapeo de claves surrogadas
  │  (SQLAlchemy) │
  └──────┬────────┘
         │
         ▼
  ┌───────────────────────────────────────────┐
  │         DATA WAREHOUSE (PostgreSQL)        │
  │  Esquema estrella · Schema: f1_dw          │
  │                                            │
  │  Hechos: FactRaceResults                   │
  │          FactQualifying                    │
  │          FactPitStops                      │
  │                                            │
  │  Dims:   DimDriver · DimConstructor        │
  │          DimCircuit · DimRace              │
  │          DimDate · DimStatus               │
  └──────┬────────────────────────────────────┘
         │
         ├──────────────┬─────────────────────┐
         ▼              ▼                     ▼
    [ EDA ]        [ ML Models ]        [ Dashboard ]
  14 gráficos    LR · RF · SVM · XGB    Dash + Plotly
  matplotlib       Ranking predicho      localhost:8050
  seaborn          Spearman correlation
```

---

## Metodología

### Fase 1 — ETL: Limpieza y preprocesamiento
- Estandarización de columnas, reemplazo de nulos, conversión de tipos
- Conversión de tiempos (`1:23.456` → segundos), generación de columnas derivadas
- Salida: 13 CSV limpios en `data_processed/`

### Fase 2 — Data Warehouse
- Esquema estrella con 3 tablas de hechos y 6 dimensiones
- Columnas calculadas en BD: `is_podium`, `is_winner`, `best_quali_time_s`
- 5 vistas analíticas para EDA y ML

### Fase 3 — EDA
Preguntas de investigación:
1. ¿Qué factores influyen más en llegar al podio?
2. ¿Cómo evolucionó el dominio de equipos y pilotos a lo largo de las dŕcadas?
3. ¿Qué tan determinante es la posición de clasificación sobre el resultado final?
4. ¿Cómo impactan los pit stops en el resultados?

### Fase 4 — Modelado Predictivo
- **Variable objetivo:** probabilidad de podio por piloto (`P(podio | condiciones)`)
- **Enfoque:** ranking de pilotos por carrera ordenado por probabilidad predicha
- **Modelos:** Regresión Logística (baseline), Random Forest, SVM, XGBoost
- **Evaluación:** ROC-AUC, F1-Score, Spearman rank correlation (ranking predicho vs. real)
- **Split:** temporal — entrenamiento ≤2021, test 2022–2024

### Fase 5 — Dashboard Interactivo
- Filtros por temporada, piloto y constructor
- Visualización **podio predicho vs. podio real** por carrera
- Evolución de puntos, análisis de circuitos, historial de pit stops

---

## Instalación y uso

### Prerrequisitos
- Docker Engine + Docker Compose v2
- Make

### Inicio rápido

```bash
# 1. Clonar el repositorio
git clone https://github.com/tu_usuario/BDM_TP_Final_F1.git
cd BDM_TP_Final_F1

# 2. Configurar variables de entorno
cp .env.example .env

# 3. Levantar los contenedores y crea estructura de la DW
make up

# 4. Aplicamos la fase 1 de limpieza y procesamiento de datos
make etl_clean
```

### Comandos disponibles

```bash
make help          # Lista todos los comandos disponibles
make up            # Levanta todos los contenedores
make rebuild       # Recrea el entorno desde cero

```

---

## Referencias bibliográficas

1. Patil, A., & Jain, N. (2022). *A Data-Driven Analysis of Formula 1 Car Races Outcome*. AICS 2022. Springer.
2. van Kesteren, E.-J., & Bergkamp, T. (2022). *Bayesian Analysis of Formula One Race Results: Disentangling Driver Skill and Constructor Advantage*. arXiv.
3. Nimmala, R., & Nimmala, J. (2024). *Racing into the Data Age: Sensor Intelligence, Advanced Analytics, and Kafka in Formula 1 Race Car*. IJAIML.
4. *Automation of Data Analysis in Formula 1* (2023). Comparativa Tableau, Power BI y Python Dash. Cal Poly Digital Commons.
5. Urdhwareshe, A. (2025). *The Use of Machine Learning in Predicting Formula 1 Race Outcomes*. Pre-print, Sciety.
6. Pedregosa, F. et al. (2011). *Scikit-learn: Machine Learning in Python*. JMLR, 12, 2825–2830.
7. Chen, T., & Guestrin, C. (2016). *XGBoost: A Scalable Tree Boosting System*. KDD '16. ACM.
8. Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit* (3rd ed.). Wiley.

---