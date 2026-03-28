"""
ETL Final - Carga al Data Warehouse F1
=======================================
Carga los CSV limpios (output de la Fase 1) al esquema f1_dw en PostgreSQL.

Requisitos:
    pip install pandas sqlalchemy psycopg2-binary tqdm

Uso:
    Ajustar las variables de conexión en la sección CONFIG y ejecutar:
    python etl_load_dw.py
"""

import os
import numpy as np
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine


DB_USER = os.getenv("DB_USER", "f1_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "f1_pass")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "f1_dwh")
DB_SCHEMA = os.getenv("DB_SCHEMA", "f1_dw")

PROC_DIR = os.getenv("PROC_DIR", "/app/F1_Project/data_processed")


def get_engine():
    """
    Nos conectamos a la DB
    """
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(url, echo=False)
    return engine


def load_clean(name: str) -> pd.DataFrame:
    """
    Leemos el .csv de acuerdo con al name
    """
    path = os.path.join(PROC_DIR, f"{name}_clean.csv")
    df = pd.read_csv(path, low_memory=False)
    df = df.replace(["\\N", "None", "nan", "NaN", ""], np.nan)
    return df


def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def time_to_seconds(t):
    """
    Convertimos la hora a segundos
    '1:23.456' → 83.456
    """
    try:
        if pd.isna(t):
            return None
        t = str(t)
        if ":" in t:
            m, s = t.split(":")
            return round(float(m) * 60 + float(s), 4)
        return round(float(t), 4)
    except Exception:
        return None


def load_dim_date(engine):
    print(">> DimDate ...")
    rows = []
    start = date(1950, 1, 1)
    end = date(2024, 12, 31)
    day = start
    MONTHS = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    while day <= end:
        rows.append(
            {
                "full_date": day,
                "day": day.day,
                "month": day.month,
                "month_name": MONTHS[day.month - 1],
                "quarter": (day.month - 1) // 3 + 1,
                "year": day.year,
                "decade": (day.year // 10) * 10,
                "is_weekend": day.weekday() >= 5,
            }
        )
        day += timedelta(days=1)
    df = pd.DataFrame(rows)
    df.to_sql(
        "dimdate",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"{len(df)} fechas cargadas.")


def load_dim_circuit(engine):
    print(">> DimCircuit ...")
    df = load_clean("circuits")
    mapping = {
        "circuitid": "circuit_id",
        "circuitref": "circuit_ref",
        "name": "circuit_name",
        "location": "location",
        "country": "country",
        "lat": "latitude",
        "lng": "longitude",
        "alt": "altitude_m",
        "url": "url",
    }
    df = df.rename(columns=mapping)[list(mapping.values())]
    df["circuit_id"] = df["circuit_id"].apply(safe_int)
    df.to_sql(
        "dimcircuit",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(df)} circuitos cargados.")


def load_dim_driver(engine):
    print(">> DimDriver ...")
    df = load_clean("drivers")
    df["full_name"] = df["forename"].fillna("") + " " + df["surname"].fillna("")
    df["full_name"] = df["full_name"].str.strip()
    df["dob"] = pd.to_datetime(df["dob"], errors="coerce")
    mapping = {
        "driverid": "driver_id",
        "driverref": "driver_ref",
        "number": "driver_number",
        "code": "driver_code",
        "full_name": "full_name",
        "dob": "date_of_birth",
        "nationality": "nationality",
        "url": "url",
    }
    df = df.rename(columns=mapping)[list(mapping.values())]
    df["driver_id"] = df["driver_id"].apply(safe_int)
    df.to_sql(
        "dimdriver",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(df)} pilotos cargados.")


def load_dim_constructor(engine):
    print(">> DimConstructor ...")
    df = load_clean("constructors")
    mapping = {
        "constructorid": "constructor_id",
        "constructorref": "constructor_ref",
        "name": "constructor_name",
        "nationality": "nationality",
        "url": "url",
    }
    df = df.rename(columns=mapping)[list(mapping.values())]
    df["constructor_id"] = df["constructor_id"].apply(safe_int)
    df.to_sql(
        "dimconstructor",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(df)} constructores cargados.")


STATUS_GROUPS = {
    "Finished": "Finished",
    "Disqualified": "Other",
    "Accident": "Accident",
    "Collision": "Accident",
    "Engine": "Mechanical",
    "Gearbox": "Mechanical",
    "Transmission": "Mechanical",
    "Clutch": "Mechanical",
    "Hydraulics": "Mechanical",
    "Electrical": "Mechanical",
    "Suspension": "Mechanical",
    "Brakes": "Mechanical",
    "Retired": "Other",
    "Withdrew": "Other",
}


def classify_status(s: str) -> str:
    for key, group in STATUS_GROUPS.items():
        if key.lower() in str(s).lower():
            return group
    return "Other"


def load_dim_status(engine):
    print(">> DimStatus ...")
    df = load_clean("status")
    df = df.rename(columns={"statusid": "status_id", "status": "status"})
    df["status_id"] = df["status_id"].apply(safe_int)
    df["status_group"] = df["status"].apply(classify_status)
    df.to_sql(
        "dimstatus",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(df)} estados cargados.")


def load_dim_race(engine):
    print(">> DimRace ...")
    races = load_clean("races")

    # Obtenemos mapeos de claves
    with engine.connect() as con:
        date_map = pd.read_sql("SELECT date_key, full_date FROM f1_dw.DimDate", con)
        date_map["full_date"] = pd.to_datetime(date_map["full_date"]).dt.date
        circ_map = pd.read_sql(
            "SELECT circuit_key, circuit_id FROM f1_dw.DimCircuit", con
        )

    races["date_parsed"] = pd.to_datetime(
        races["date"], dayfirst=True, errors="coerce"
    ).dt.date
    races = races.merge(
        date_map, left_on="date_parsed", right_on="full_date", how="left"
    )
    races = races.merge(
        circ_map, left_on="circuitid", right_on="circuit_id", how="left"
    )

    cols = {
        "raceid": "race_id",
        "year": "year",
        "round": "round",
        "name": "race_name",
        "date_key": "date_key",
        "circuit_key": "circuit_key",
        "fp1_date": "fp1_date",
        "fp2_date": "fp2_date",
        "fp3_date": "fp3_date",
        "quali_date": "quali_date",
        "sprint_date": "sprint_date",
    }
    out = races.rename(columns=cols)
    out = out[[c for c in cols.values() if c in out.columns]]
    out["race_id"] = out["race_id"].apply(safe_int)
    out.to_sql(
        "dimrace",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(out)} carreras cargadas.")


def load_fact_race_results(engine):
    print(">> FactRaceResults ...")
    results = load_clean("results")

    with engine.connect() as con:
        race_map = pd.read_sql(
            "SELECT race_key, race_id, date_key FROM f1_dw.DimRace", con
        )
        drv_map = pd.read_sql("SELECT driver_key, driver_id FROM f1_dw.DimDriver", con)
        con_map = pd.read_sql(
            "SELECT constructor_key, constructor_id FROM f1_dw.DimConstructor", con
        )
        sta_map = pd.read_sql("SELECT status_key, status_id FROM f1_dw.DimStatus", con)

    df = results.copy()
    df["raceid"] = df["raceid"].apply(safe_int)
    df["driverid"] = df["driverid"].apply(safe_int)
    df["constructorid"] = df["constructorid"].apply(safe_int)
    df["statusid"] = df["statusid"].apply(safe_int)

    df = df.merge(race_map, left_on="raceid", right_on="race_id", how="left")
    df = df.merge(drv_map, left_on="driverid", right_on="driver_id", how="left")
    df = df.merge(
        con_map, left_on="constructorid", right_on="constructor_id", how="left"
    )
    df = df.merge(sta_map, left_on="statusid", right_on="status_id", how="left")

    # Tiempo vuelta rápida a segundos
    if "fastestlaptime" in df.columns:
        df["fastest_lap_time_s"] = df["fastestlaptime"].apply(time_to_seconds)

    out = df.rename(
        columns={
            "resultid": "result_id",
            "grid": "grid_position",
            "position": "finish_position",
            "positionorder": "position_order",
            "positiontext": "position_text",
            "points": "points",
            "laps": "laps_completed",
            "milliseconds": "race_time_ms",
            "fastestlap": "fastest_lap",
            "rank": "fastest_lap_rank",
            "fastestlapspeed": "fastest_lap_speed",
        }
    )

    keep = [
        "result_id",
        "race_key",
        "driver_key",
        "constructor_key",
        "status_key",
        "date_key",
        "grid_position",
        "finish_position",
        "position_order",
        "position_text",
        "points",
        "laps_completed",
        "race_time_ms",
        "fastest_lap",
        "fastest_lap_rank",
        "fastest_lap_time_s",
        "fastest_lap_speed",
    ]

    out = out[[c for c in keep if c in out.columns]]

    # Convertimos lso numéricos
    for col in [
        "grid_position",
        "finish_position",
        "position_order",
        "laps_completed",
        "fastest_lap",
        "fastest_lap_rank",
        "race_time_ms",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out.to_sql(
        "factraceresults",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(out)} resultados cargados.")


def load_fact_qualifying(engine):
    print(">> FactQualifying ...")
    quali = load_clean("qualifying")

    with engine.connect() as con:
        race_map = pd.read_sql(
            "SELECT race_key, race_id, date_key FROM f1_dw.DimRace", con
        )
        drv_map = pd.read_sql("SELECT driver_key, driver_id FROM f1_dw.DimDriver", con)
        con_map = pd.read_sql(
            "SELECT constructor_key, constructor_id FROM f1_dw.DimConstructor", con
        )

    df = quali.copy()
    df["raceid"] = df["raceid"].apply(safe_int)
    df["driverid"] = df["driverid"].apply(safe_int)
    df["constructorid"] = df["constructorid"].apply(safe_int)

    df = df.merge(race_map, left_on="raceid", right_on="race_id", how="left")
    df = df.merge(drv_map, left_on="driverid", right_on="driver_id", how="left")
    df = df.merge(
        con_map, left_on="constructorid", right_on="constructor_id", how="left"
    )

    for seg in ["q1", "q2", "q3"]:
        if seg in df.columns:
            df[f"{seg}_time_s"] = df[seg].apply(time_to_seconds)

    out = df.rename(columns={"qualifyid": "qualify_id", "position": "quali_position"})

    keep = [
        "qualify_id",
        "race_key",
        "driver_key",
        "constructor_key",
        "date_key",
        "quali_position",
        "q1_time_s",
        "q2_time_s",
        "q3_time_s",
    ]
    out = out[[c for c in keep if c in out.columns]]

    out.to_sql(
        "factqualifying",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(out)} registros de qualifying cargados.")


def load_fact_pit_stops(engine):
    print(">> FactPitStops ...")
    pits = load_clean("pit_stops")

    with engine.connect() as con:
        race_map = pd.read_sql(
            "SELECT race_key, race_id, date_key FROM f1_dw.DimRace", con
        )
        drv_map = pd.read_sql("SELECT driver_key, driver_id FROM f1_dw.DimDriver", con)

    df = pits.copy()
    df["raceid"] = df["raceid"].apply(safe_int)
    df["driverid"] = df["driverid"].apply(safe_int)

    df = df.merge(race_map, left_on="raceid", right_on="race_id", how="left")
    df = df.merge(drv_map, left_on="driverid", right_on="driver_id", how="left")

    if "pit_duration_s" not in df.columns and "duration" in df.columns:
        df["pit_duration_s"] = df["duration"].apply(time_to_seconds)

    out = df.rename(
        columns={
            "stop": "stop_number",
            "lap": "lap_number",
            "time": "pit_time_of_day",
            "milliseconds": "pit_duration_ms",
        }
    )

    keep = [
        "race_key",
        "driver_key",
        "date_key",
        "stop_number",
        "lap_number",
        "pit_time_of_day",
        "pit_duration_s",
        "pit_duration_ms",
    ]
    out = out[[c for c in keep if c in out.columns]]

    out.to_sql(
        "factpitstops",
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"{len(out)} pit stops cargados.")


if __name__ == "__main__":
    engine = get_engine()

    print("=" * 55)
    print("ETL LOAD - F1 Data Warehouse")
    print("=" * 55)
    print(f"Base de datos : {DB_NAME}")
    print(f"Schema        : {DB_SCHEMA}")
    print(f"Datos limpios : {PROC_DIR}")
    print("=" * 55)

    # Primero tratamos las dimensiones, dspués los hechos
    load_dim_date(engine)
    load_dim_circuit(engine)
    load_dim_driver(engine)
    load_dim_constructor(engine)
    load_dim_status(engine)
    load_dim_race(engine)
    load_fact_race_results(engine)
    load_fact_qualifying(engine)
    load_fact_pit_stops(engine)

    print("Carga completa al Data Warehouse.")
    print(f"Vista disponible: {DB_SCHEMA}.vw_race_analysis")
