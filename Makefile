# =============================================================
#  Makefile - F1 Data Warehouse
#  TP Final - Bases de Datos Masivas - UNLu
#
#  Uso: make <comando>
#  Ej:  make up, make down, make rebuild, etc
# =============================================================

# --- Cargamos variables de entorno ---
include .env
export

# --- Variables internas ---
COMPOSE       = docker compose
APP           = f1_app
POSTGRES      = f1_postgres
APP_EXEC      = $(COMPOSE) exec app
PSQL          = $(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
PYTHON        = $(APP_EXEC) python
SCRIPTS_DIR   = /app/F1_Project
SQL_DIR       = $(SCRIPTS_DIR)/warehouse/sql

# Colores para la terminal
GREEN  = \033[0;32m
YELLOW = \033[1;33m
CYAN   = \033[0;36m
RESET  = \033[0m

.PHONY: help up down rebuild restart logs status \
        etl-clean


#  CONTENEDORES

## Levanta todos los contenedores en background
up:
	@echo "$(GREEN)▶ Levantando contenedores...$(RESET)"
	$(COMPOSE) up -d --build
	@echo "$(GREEN)✔ Contenedores activos. Esperando que Postgres esté listo...$(RESET)"
	@sleep 3
	@make status

## Detiene y elimina contenedores (preserva volumen de datos)
down:
	@echo "$(YELLOW)■ Deteniendo contenedores...$(RESET)"
	$(COMPOSE) down
	@echo "$(YELLOW)✔ Contenedores detenidos.$(RESET)"

## Recrea TODO desde cero: borra volúmenes, imágenes y datos
rebuild:
	@echo "$(YELLOW)⚠  Esto borrará TODOS los datos del DW. Continuar? [y/N]$(RESET)" && read ans && [ $${ans:-N} = y ]
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d
	@echo "$(GREEN)✔ Entorno recreado desde cero.$(RESET)"
	@sleep 5
	@make db-create-all

## Reinicia los contenedores sin borrar datos
restart:
	@echo "$(YELLOW)↺ Reiniciando contenedores...$(RESET)"
	$(COMPOSE) restart
	@make status

## Logs en tiempo real de todos los servicios
logs:
	$(COMPOSE) logs -f

## Logs solo del contenedor app
logs-app:
	$(COMPOSE) logs -f app

## Logs solo de postgres
logs-db:
	$(COMPOSE) logs -f postgres

## Estado de los contenedores
status:
	@echo "$(CYAN)── Estado de los contenedores ──$(RESET)"
	$(COMPOSE) ps


#  ACCESO A CONTENEDORES

## Shell interactivo en el contenedor Python
shell-app:
	@echo "$(GREEN)▶ Entrando al contenedor app...$(RESET)"
	$(COMPOSE) exec app bash

## Shell interactivo en el contenedor Postgres
shell-db:
	@echo "$(GREEN)▶ Entrando al contenedor postgres...$(RESET)"
	$(COMPOSE) exec postgres bash

## Consola psql interactiva
db-psql:
	@echo "$(GREEN)▶ Abriendo consola psql...$(RESET)"
	$(PSQL)



#  BASE DE DATOS - DDL

## Crea el schema f1_dw
db-create-schema:
	@echo "$(GREEN)▶ Creando schema f1_dw...$(RESET)"
	$(PSQL) -c "CREATE SCHEMA IF NOT EXISTS f1_dw;"
	@echo "$(GREEN)✔ Schema creado.$(RESET)"

## Ejecuta el DDL completo (dimensiones + hechos + índices)
db-create-tables:
	@echo "$(GREEN)▶ Creando tablas del DW...$(RESET)"
	$(PSQL) -f $(SQL_DIR)/f1_dw_ddl.sql
	@echo "$(GREEN)✔ Tablas creadas.$(RESET)"

## Crea las vistas analíticas
db-create-views:
	@echo "$(GREEN)▶ Creando vistas analíticas...$(RESET)"
	$(PSQL) -f $(SQL_DIR)/f1_dw_views.sql
	@echo "$(GREEN)✔ Vistas creadas.$(RESET)"

## Ejecuta todo el DDL: schema + tablas + vistas
db-create-all: db-create-schema db-create-tables db-create-views
	@echo "$(GREEN)✔ DW completo creado.$(RESET)"

## Borra y recrea el schema (¡DESTRUCTIVO!)
db-reset:
	@echo "$(YELLOW)⚠  Esto borrará TODOS los datos del schema f1_dw. Continuar? [y/N]$(RESET)" && read ans && [ $${ans:-N} = y ]
	$(PSQL) -c "DROP SCHEMA IF EXISTS f1_dw CASCADE;"
	@make db-create-all
	@echo "$(GREEN)✔ Schema reseteado y recreado.$(RESET)"

## Muestra tablas existentes y cantidad de filas
db-info:
	@echo "$(CYAN)── Tablas en f1_dw ──$(RESET)"
	$(PSQL) -c "\dt f1_dw.*"
	@echo "$(CYAN)── Conteo de filas ──$(RESET)"
	$(PSQL) -c "\
		SELECT schemaname, tablename, \
		       (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I.%I', schemaname, tablename), false, true, '')))[1]::text::int AS row_count \
		FROM pg_tables \
		WHERE schemaname = 'f1_dw' \
		ORDER BY tablename;"

## Backup del DW completo
db-backup:
	@echo "$(GREEN)▶ Generando backup...$(RESET)"
	@mkdir -p ./backups
	$(COMPOSE) exec postgres pg_dump -U $(POSTGRES_USER) -d $(POSTGRES_DB) -n f1_dw \
		> ./backups/f1_dw_backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✔ Backup guardado en ./backups/$(RESET)"

## Restaura desde backup: make db-restore FILE=./backups/archivo.sql
db-restore:
	@echo "$(YELLOW)▶ Restaurando desde $(FILE)...$(RESET)"
	$(PSQL) < $(FILE)
	@echo "$(GREEN)✔ Restauración completada.$(RESET)"


# Fases

## Fase 1: limpiezamos CSVs crudos
etl-clean:
	@echo "$(GREEN)▶ Ejecutando ETL - Fase 1: Limpieza...$(RESET)"
	$(PYTHON) $(SCRIPTS_DIR)/scripts/etl_clean.py
	@echo "$(GREEN)✔ Limpieza completada. Archivos en data_processed/$(RESET)"