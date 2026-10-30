# d8a-de-workbench
#
#   make data CONTEXT=app                   download the company's source tables
#   make up CONTEXT=app                     postgres + the context's simulated source system
#   make up CONTEXT=app PROFILE="airflow"   plus Airflow (P4); "airflow redpanda" (P5 app); "minio" (P5 operations)
#   make check                              what runs, what it holds, green when ready
#   make down / make reset                  stop (keep data) / stop and delete the volumes
#   make psql                               a psql shell on the warehouse
#   make dbt ARGS="build"                   run dbt in the dbt container
#   make stats CONTEXT=app [DATE=2026-11-03] the simulator's reference counts for a day (today by default; HOUR= for operations)

CONTEXT ?= app
PROFILE ?=
COMPOSE := docker compose $(foreach p,$(CONTEXT) $(PROFILE),--profile $(p))
DATASETS := https://d8a.academy/datasets
PG := docker compose exec -T postgres psql -U $${POSTGRES_USER:-d8a} -d $${POSTGRES_DB:-warehouse}

.PHONY: data up down reset check psql dbt logs stats lint test

data:
	@mkdir -p data/$(CONTEXT)
	@case "$(CONTEXT)" in \
	  app) for f in customers orders products order_items; do curl -fsSL $(DATASETS)/da/$$f.csv -o data/app/$$f.csv; done; \
	       curl -fsSL $(DATASETS)/de/app/events.csv -o data/app/events_sample.csv ;; \
	  finance) for f in branches customers accounts transactions loans; do curl -fsSL $(DATASETS)/da/finance/$$f.csv -o data/finance/$$f.csv; done; \
	       for f in ledger_entries processor_statement; do curl -fsSL $(DATASETS)/de/finance/$$f.csv -o data/finance/$$f.csv; done ;; \
	  marketing) for f in campaigns ad_spend_daily leads customers; do curl -fsSL $(DATASETS)/da/marketing/$$f.csv -o data/marketing/$$f.csv; done; \
	       curl -fsSL $(DATASETS)/de/marketing/platform_export.csv -o data/marketing/platform_export.csv ;; \
	  operations) for f in depots vehicles trips depot_scans telemetry_sample; do curl -fsSL $(DATASETS)/de/operations/$$f.csv -o data/operations/$$f.csv; done ;; \
	  *) echo "unknown CONTEXT $(CONTEXT) (app | finance | marketing | operations)"; exit 1 ;; \
	esac
	@echo "data/$(CONTEXT): $$(ls data/$(CONTEXT) | wc -l | tr -d ' ') files"

up:
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory check

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v --remove-orphans
	rm -rf data/app_events data/eod data/lake

check:
	@echo "── d8a-de-workbench · context $(CONTEXT) · profiles: $(PROFILE)"
	@for i in $$(seq 1 30); do $(PG) -c "select 1" >/dev/null 2>&1 && break; sleep 2; done
	@$(PG) -c "select 1" >/dev/null 2>&1 && echo "✔ postgres        warehouse reachable on :$${POSTGRES_PORT:-5432}" || { echo "✘ postgres        not reachable"; exit 1; }
	@$(PG) -tAc "select string_agg(schema_name, ', ' order by schema_name) from information_schema.schemata where schema_name in ('raw','staging','marts','app')" | sed 's/^/✔ schemas         /'
	@$(PG) -tAc "select coalesce(string_agg(table_name || ' (' || n || ')', ', ' order by table_name), 'none: run make data CONTEXT=app then make reset && make up') from (select c.relname as table_name, c.reltuples::bigint as n from pg_class c join pg_namespace s on s.oid = c.relnamespace where s.nspname = 'app' and c.relkind = 'r') t" | sed 's/^/✔ app tables      /'
	@if docker compose ps --status running --services 2>/dev/null | grep -q '^sources-'; then echo "✔ sources         $$(docker compose ps --status running --services | grep '^sources-' | tr '\n' ' ')"; else echo "· sources         none running (make up CONTEXT=...)"; fi
	@if docker compose ps --status running --services 2>/dev/null | grep -q '^airflow-webserver'; then echo "✔ airflow         http://localhost:$${AIRFLOW_PORT:-8081}  (admin / admin)"; fi
	@if docker compose ps --status running --services 2>/dev/null | grep -q '^redpanda$$'; then echo "✔ redpanda        kafka on localhost:9092, console http://localhost:8082"; fi
	@if docker compose ps --status running --services 2>/dev/null | grep -q '^minio$$'; then echo "✔ minio           s3 on http://localhost:9000, console http://localhost:9001 (bucket lake)"; fi
	@echo "── green. Next: open the project step that brought you here."

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-d8a} -d $${POSTGRES_DB:-warehouse}

dbt:
	docker compose --profile tools run --rm dbt $(ARGS)

logs:
	$(COMPOSE) logs -f --tail=100

stats:
	@case "$(CONTEXT)" in \
	  app) python3 sources/app_events.py --stats $(if $(DATE),--date $(DATE)) ;; \
	  finance) python3 sources/eod_files.py --stats $(if $(DATE),--date $(DATE)) ;; \
	  marketing) python3 sources/saas_api.py --stats ;; \
	  operations) python3 sources/telemetry.py --stats $(if $(DATE),--date $(DATE)) $(if $(HOUR),--hour $(HOUR)) ;; \
	esac

lint:
	ruff check pipeline tests sources || true

test:
	python3 -m pytest -q tests
