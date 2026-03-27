import os
import pandas as pd
import glob
import numpy as np

pd.set_option("display.max_columns", 200)

WORK_PATH = "F1_Project"
RAW_DIR = os.path.expanduser(f"{WORK_PATH}/data_raw/Championship1950_2024")
PROC_DIR = os.path.expanduser(f"{WORK_PATH}/data_processed")
os.makedirs(PROC_DIR, exist_ok=True)

files = glob.glob(os.path.join(RAW_DIR, "*.csv"))
files

races = pd.read_csv(os.path.join(RAW_DIR, "races.csv"))
races.head()
races.info()
races.shape


def resumen(df):
    print("Shape:", df.shape)
    print("\nColumns:\n", df.columns.tolist())
    print("\nDtypes:\n", df.dtypes)
    print("\nNulls:\n", df.isnull().sum().sort_values(ascending=False).head(20))
    print("\nDuplicados:", df.duplicated().sum())


resumen(races)


def clean_cols(df):
    """Estandarizamos los nombres de columnas"""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )
    return df


def to_datetime_safe(df, cols):
    """Convertimos columnas a datetime"""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def replace_nulls(df):
    """Reemplazamos strings vacíos, None o parecidos por NaN"""
    df = df.replace(["\\N", "None", "", "NULL", "NaN", "nan"], np.nan)
    return df


def trim_strings(df):
    """Quitamos espacios extra en columnas tipo string."""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def time_to_seconds(time_str):
    """Convertimos tiempos tipo '1:23.456' a segundos float"""
    try:
        if pd.isna(time_str):
            return np.nan
        parts = time_str.split(":")
        if len(parts) == 2:
            m, s = parts
            return float(m) * 60 + float(s)
        else:
            return float(parts[0])
    except Exception:
        return np.nan


def clean_races(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
        df["date"] = df["date"].dt.strftime("%d/%m/%Y")

    if "time" in df.columns:
        df["time"] = pd.to_datetime(
            df["time"], format="%H:%M:%S", errors="coerce"
        ).dt.time

    df["year_round"] = df["year"].astype(str) + "_" + df["round"].astype(str)
    return df


def clean_drivers(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    df["full_name"] = df["forename"] + " " + df["surname"]
    df = df.drop_duplicates(subset="driverid", keep="first")
    return df


def clean_constructors(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    df = df.drop_duplicates(subset="constructorid", keep="first")
    return df


def clean_results(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    numeric_cols = [
        "points",
        "laps",
        "positionorder",
        "milliseconds",
        "fastestlap",
        "rank",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def clean_pit_stops(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    if "duration" in df.columns:
        df["pit_duration_s"] = df["duration"].apply(time_to_seconds)
    df = df.drop_duplicates()
    return df


def clean_lap_times(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    if "time" in df.columns:
        df["lap_time_s"] = df["time"].apply(time_to_seconds)
    df = df.drop_duplicates()
    return df


def clean_qualifying(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    for col in ["q1", "q2", "q3"]:
        if col in df.columns:
            df[col + "_s"] = df[col].apply(time_to_seconds)
    df = df.drop_duplicates()
    return df


def clean_circuits(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    df = df.drop_duplicates(subset="circuitid", keep="first")
    return df


def clean_status(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    return df


def clean_seasons(df):
    df = clean_cols(df)
    df = replace_nulls(df)
    df = trim_strings(df)
    return df


CLEANERS = {
    "races": clean_races,
    "drivers": clean_drivers,
    "constructors": clean_constructors,
    "results": clean_results,
    "pit_stops": clean_pit_stops,
    "lap_times": clean_lap_times,
    "qualifying": clean_qualifying,
    "circuits": clean_circuits,
    "status": clean_status,
    "seasons": clean_seasons,
}


cleaned_dfs = {}

for csv_path in files:
    name = os.path.basename(csv_path).replace(".csv", "").lower()
    print(f"Procesando {name}")
    df = pd.read_csv(csv_path)
    df = clean_cols(df)
    cleaner = CLEANERS.get(name)
    if cleaner:
        df = cleaner(df)
    else:
        df = replace_nulls(trim_strings(df))
    cleaned_dfs[name] = df
    out_path = os.path.join(PROC_DIR, f"{name}_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Guardado: {out_path} ({df.shape[0]} filas x {df.shape[1]} cols)")


print(f"Archivos limpios disponibles en: {PROC_DIR}")
