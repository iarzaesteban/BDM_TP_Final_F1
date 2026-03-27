-- =============================================================
--  F1 Data Warehouse - Vistas Analíticas
--  TP Final - Bases de Datos Masivas - UNLu
-- =============================================================

SET search_path TO f1_dw;

-- -------------------------------------------------------------
-- Vista 1: Análisis completo por carrera (ya incluida en DDL,
--          se recrea aquí con OR REPLACE para idempotencia)
-- -------------------------------------------------------------
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
    SELECT race_key, driver_key,
           COUNT(*)            AS total_pit_stops,
           AVG(pit_duration_s) AS avg_pit_duration_s
    FROM FactPitStops
    GROUP BY race_key, driver_key
) ps ON ps.race_key = fr.race_key AND ps.driver_key = fr.driver_key;

COMMENT ON VIEW vw_race_analysis IS 'Vista analítica principal. Une todas las tablas del DW.';


-- -------------------------------------------------------------
-- Vista 2: Resumen por piloto y temporada
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW vw_driver_season AS
SELECT
    dr.year,
    dv.full_name        AS driver_name,
    dv.nationality      AS driver_nationality,
    co.constructor_name,
    COUNT(*)            AS races,
    SUM(fr.points)      AS total_points,
    SUM(fr.is_winner::int)  AS wins,
    SUM(fr.is_podium::int)  AS podiums,
    ROUND(AVG(fr.grid_position)::numeric, 2)   AS avg_grid,
    ROUND(AVG(fr.finish_position)::numeric, 2) AS avg_finish,
    MIN(fr.finish_position) AS best_result
FROM FactRaceResults fr
JOIN DimRace        dr ON fr.race_key        = dr.race_key
JOIN DimDriver      dv ON fr.driver_key       = dv.driver_key
JOIN DimConstructor co ON fr.constructor_key  = co.constructor_key
GROUP BY dr.year, dv.full_name, dv.nationality, co.constructor_name;

COMMENT ON VIEW vw_driver_season IS 'Resumen estadístico por piloto y temporada.';


-- -------------------------------------------------------------
-- Vista 3: Resumen por constructor y temporada
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW vw_constructor_season AS
SELECT
    dr.year,
    co.constructor_name,
    co.nationality      AS constructor_nationality,
    COUNT(*)            AS entries,
    SUM(fr.points)      AS total_points,
    SUM(fr.is_winner::int)  AS wins,
    SUM(fr.is_podium::int)  AS podiums,
    COUNT(DISTINCT fr.driver_key) AS drivers_used
FROM FactRaceResults fr
JOIN DimRace        dr ON fr.race_key       = dr.race_key
JOIN DimConstructor co ON fr.constructor_key = co.constructor_key
GROUP BY dr.year, co.constructor_name, co.nationality;

COMMENT ON VIEW vw_constructor_season IS 'Resumen estadístico por constructor y temporada.';


-- -------------------------------------------------------------
-- Vista 4: Análisis de qualifying vs resultado
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW vw_qualifying_vs_race AS
SELECT
    dr.year,
    dr.race_name,
    dv.full_name        AS driver_name,
    co.constructor_name,
    fq.quali_position,
    fr.grid_position,
    fr.finish_position,
    (fr.grid_position - fq.quali_position)    AS grid_penalty,
    (fr.grid_position - fr.finish_position)   AS positions_gained,
    fr.is_podium,
    fq.best_quali_time_s
FROM FactQualifying fq
JOIN FactRaceResults fr ON fq.race_key = fr.race_key AND fq.driver_key = fr.driver_key
JOIN DimRace        dr ON fr.race_key       = dr.race_key
JOIN DimDriver      dv ON fr.driver_key      = dv.driver_key
JOIN DimConstructor co ON fr.constructor_key = co.constructor_key
WHERE fq.quali_position IS NOT NULL;

COMMENT ON VIEW vw_qualifying_vs_race IS 'Compara posición de clasificación con resultado de carrera.';


-- -------------------------------------------------------------
-- Vista 5: Estadísticas de pit stops por carrera
-- -------------------------------------------------------------
CREATE OR REPLACE VIEW vw_pitstop_strategy AS
SELECT
    dr.year,
    dr.race_name,
    ci.circuit_name,
    dv.full_name        AS driver_name,
    co.constructor_name,
    fr.finish_position,
    fr.is_podium,
    COUNT(ps.pitstop_key)       AS total_stops,
    ROUND(AVG(ps.pit_duration_s)::numeric, 3) AS avg_stop_s,
    ROUND(SUM(ps.pit_duration_s)::numeric, 3) AS total_pit_time_s,
    MIN(ps.pit_duration_s)      AS fastest_stop_s
FROM FactPitStops ps
JOIN FactRaceResults fr ON ps.race_key = fr.race_key AND ps.driver_key = fr.driver_key
JOIN DimRace        dr ON fr.race_key       = dr.race_key
JOIN DimCircuit     ci ON dr.circuit_key    = ci.circuit_key
JOIN DimDriver      dv ON fr.driver_key      = dv.driver_key
JOIN DimConstructor co ON fr.constructor_key = co.constructor_key
GROUP BY dr.year, dr.race_name, ci.circuit_name,
         dv.full_name, co.constructor_name,
         fr.finish_position, fr.is_podium;

COMMENT ON VIEW vw_pitstop_strategy IS 'Estrategia de pit stops por piloto y carrera.';