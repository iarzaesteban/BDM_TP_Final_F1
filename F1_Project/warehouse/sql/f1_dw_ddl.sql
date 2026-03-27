-- =============================================================
--  DATA WAREHOUSE - Fórmula 1 (1950-2024)
--  TP Final Integrador - Bases de Datos Masivas
--  Universidad Nacional de Luján - Lic. en Sistemas de Información
--  Motor: PostgreSQL
--  Esquema: Estrella (Star Schema)
-- =============================================================


CREATE SCHEMA IF NOT EXISTS f1_dw;
SET search_path TO f1_dw;


-- =============================================================
--  DIMENSIONES
-- =============================================================

CREATE TABLE IF NOT EXISTS DimDate (
    date_key        SERIAL          PRIMARY KEY,
    full_date       DATE            NOT NULL UNIQUE,
    day             SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    month_name      VARCHAR(20)     NOT NULL,
    quarter         SMALLINT        NOT NULL,
    year            SMALLINT        NOT NULL,
    decade          SMALLINT        NOT NULL,
    is_weekend      BOOLEAN         NOT NULL
);
COMMENT ON TABLE DimDate IS 'Dimensión temporal. Generada sintéticamente para el rango 1950-2024.';


CREATE TABLE IF NOT EXISTS DimCircuit (
    circuit_key     SERIAL          PRIMARY KEY,
    circuit_id      INT             NOT NULL UNIQUE,
    circuit_ref     VARCHAR(50)     NOT NULL,
    circuit_name    VARCHAR(100)    NOT NULL,
    location        VARCHAR(100),
    country         VARCHAR(100),
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    altitude_m      INT,
    url             VARCHAR(255)
);
COMMENT ON TABLE DimCircuit IS 'Circuitos del campeonato. Incluye coordenadas geográficas.';


CREATE TABLE IF NOT EXISTS DimDriver (
    driver_key      SERIAL          PRIMARY KEY,
    driver_id       INT             NOT NULL UNIQUE,
    driver_ref      VARCHAR(50)     NOT NULL,
    driver_number   VARCHAR(10),
    driver_code     VARCHAR(5),
    full_name       VARCHAR(100)    NOT NULL,
    date_of_birth   DATE,
    nationality     VARCHAR(60),
    url             VARCHAR(255)
);
COMMENT ON TABLE DimDriver IS 'Pilotos que participaron en el campeonato entre 1950 y 2024.';


CREATE TABLE IF NOT EXISTS DimConstructor (
    constructor_key     SERIAL          PRIMARY KEY,
    constructor_id      INT             NOT NULL UNIQUE,
    constructor_ref     VARCHAR(50)     NOT NULL,
    constructor_name    VARCHAR(100)    NOT NULL,
    nationality         VARCHAR(60),
    url                 VARCHAR(255)
);
COMMENT ON TABLE DimConstructor IS 'Escuderías / constructores participantes.';


CREATE TABLE IF NOT EXISTS DimStatus (
    status_key      SERIAL          PRIMARY KEY,
    status_id       INT             NOT NULL UNIQUE,
    status          VARCHAR(60)     NOT NULL,

    status_group    VARCHAR(30)     NOT NULL 
);
COMMENT ON TABLE DimStatus IS 'Estado final del piloto en la carrera. Incluye agrupamiento para análisis.';



CREATE TABLE IF NOT EXISTS DimRace (
    race_key        SERIAL          PRIMARY KEY,
    race_id         INT             NOT NULL UNIQUE,
    year            SMALLINT        NOT NULL,
    round           SMALLINT        NOT NULL,
    race_name       VARCHAR(100)    NOT NULL,
    date_key        INT             REFERENCES DimDate(date_key),
    circuit_key     INT             REFERENCES DimCircuit(circuit_key),

    fp1_date        DATE,
    fp2_date        DATE,
    fp3_date        DATE,
    quali_date      DATE,
    sprint_date     DATE
);
COMMENT ON TABLE DimRace IS 'Evento de carrera. Conecta con dimensión temporal y de circuito.';

CREATE INDEX idx_dimrace_year   ON DimRace(year);
CREATE INDEX idx_dimrace_circuit ON DimRace(circuit_key);


-- =============================================================
--  TABLAS DE HECHOS
-- =============================================================
CREATE TABLE IF NOT EXISTS FactRaceResults (
    result_key          SERIAL          PRIMARY KEY,

    race_key            INT             NOT NULL REFERENCES DimRace(race_key),
    driver_key          INT             NOT NULL REFERENCES DimDriver(driver_key),
    constructor_key     INT             NOT NULL REFERENCES DimConstructor(constructor_key),
    status_key          INT             NOT NULL REFERENCES DimStatus(status_key),
    date_key            INT             NOT NULL REFERENCES DimDate(date_key),

    result_id           INT             NOT NULL UNIQUE,

    grid_position       SMALLINT,
    finish_position     SMALLINT,
    position_order      SMALLINT,
    position_text       VARCHAR(6),
    points              NUMERIC(5,2),
    laps_completed      SMALLINT,
    race_time_ms        BIGINT,             
    fastest_lap         SMALLINT,            
    fastest_lap_rank    SMALLINT,
    fastest_lap_time_s  NUMERIC(10,4),
    fastest_lap_speed   NUMERIC(8,3),

    is_podium           BOOLEAN GENERATED ALWAYS AS (finish_position <= 3) STORED,
    is_winner           BOOLEAN GENERATED ALWAYS AS (finish_position = 1) STORED,
    is_points_finish    BOOLEAN GENERATED ALWAYS AS (points > 0) STORED,
    finished_race       BOOLEAN GENERATED ALWAYS AS (finish_position IS NOT NULL) STORED
);
COMMENT ON TABLE FactRaceResults IS
  'Tabla de hechos principal. Granularidad: piloto x carrera. Incluye columnas derivadas para ML.';

CREATE INDEX idx_frr_race       ON FactRaceResults(race_key);
CREATE INDEX idx_frr_driver     ON FactRaceResults(driver_key);
CREATE INDEX idx_frr_constructor ON FactRaceResults(constructor_key);
CREATE INDEX idx_frr_podium     ON FactRaceResults(is_podium);
CREATE INDEX idx_frr_date       ON FactRaceResults(date_key);


CREATE TABLE IF NOT EXISTS FactQualifying (
    qualify_key         SERIAL          PRIMARY KEY,
    qualify_id          INT             NOT NULL UNIQUE,
    race_key            INT             NOT NULL REFERENCES DimRace(race_key),
    driver_key          INT             NOT NULL REFERENCES DimDriver(driver_key),
    constructor_key     INT             NOT NULL REFERENCES DimConstructor(constructor_key),
    date_key            INT             NOT NULL REFERENCES DimDate(date_key),

    quali_position      SMALLINT,

    q1_time_s           NUMERIC(10,4),
    q2_time_s           NUMERIC(10,4),
    q3_time_s           NUMERIC(10,4),

    best_quali_time_s   NUMERIC(10,4) GENERATED ALWAYS AS (
                            LEAST(
                                COALESCE(q3_time_s, 9999),
                                COALESCE(q2_time_s, 9999),
                                COALESCE(q1_time_s, 9999)
                            )
                        ) STORED
);
COMMENT ON TABLE FactQualifying IS
  'Tiempos de clasificación. Clave para el modelo predictivo: grilla de largada vs resultado.';

CREATE INDEX idx_fq_race    ON FactQualifying(race_key);
CREATE INDEX idx_fq_driver  ON FactQualifying(driver_key);



CREATE TABLE IF NOT EXISTS FactPitStops (
    pitstop_key         SERIAL          PRIMARY KEY,
    race_key            INT             NOT NULL REFERENCES DimRace(race_key),
    driver_key          INT             NOT NULL REFERENCES DimDriver(driver_key),
    date_key            INT             NOT NULL REFERENCES DimDate(date_key),

    stop_number         SMALLINT        NOT NULL,
    lap_number          SMALLINT        NOT NULL,
    pit_time_of_day     TIME,
    pit_duration_s      NUMERIC(8,4),
    pit_duration_ms     BIGINT
);
COMMENT ON TABLE FactPitStops IS
  'Estrategia de pit stops. Permite analizar número de paradas y tiempos por carrera.';

CREATE INDEX idx_fps_race   ON FactPitStops(race_key);
CREATE INDEX idx_fps_driver ON FactPitStops(driver_key);



CREATE OR REPLACE VIEW vw_race_analysis AS
SELECT

    fr.result_id,
    dr.year,
    dr.round,
    dr.race_name,
    dc.full_date           AS race_date,

    ci.circuit_name,
    ci.country             AS circuit_country,

    dv.full_name           AS driver_name,
    dv.nationality         AS driver_nationality,

    co.constructor_name,
    co.nationality         AS constructor_nationality,

    st.status,
    st.status_group,

    fr.grid_position,
    fr.finish_position,
    fr.position_order,
    fr.points,
    fr.laps_completed,
    fr.race_time_ms,
    fr.fastest_lap_time_s,
    fr.fastest_lap_speed,

    fr.is_podium,
    fr.is_winner,
    fr.is_points_finish,
    fr.finished_race,

    fq.quali_position,
    fq.best_quali_time_s,

    ps.total_pit_stops,
    ps.avg_pit_duration_s
FROM FactRaceResults fr
JOIN DimRace        dr  ON fr.race_key        = dr.race_key
JOIN DimDate        dc  ON fr.date_key         = dc.date_key
JOIN DimCircuit     ci  ON dr.circuit_key      = ci.circuit_key
JOIN DimDriver      dv  ON fr.driver_key       = dv.driver_key
JOIN DimConstructor co  ON fr.constructor_key  = co.constructor_key
JOIN DimStatus      st  ON fr.status_key       = st.status_key
LEFT JOIN FactQualifying fq
    ON  fq.race_key   = fr.race_key
    AND fq.driver_key = fr.driver_key
LEFT JOIN (
    SELECT
        race_key,
        driver_key,
        COUNT(*)            AS total_pit_stops,
        AVG(pit_duration_s) AS avg_pit_duration_s
    FROM FactPitStops
    GROUP BY race_key, driver_key
) ps ON ps.race_key   = fr.race_key
     AND ps.driver_key = fr.driver_key;

COMMENT ON VIEW vw_race_analysis IS
  'Vista analítica desnormalizada. Combina todas las tablas del DW. Usar para EDA y exportar features a ML.';

