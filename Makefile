START ?= 2024-01-01
DAYS ?= 7
END ?= 2024-01-07
# Everything runs inside the Airflow scheduler container (see docker-compose.yml)
EXEC = docker compose exec -T airflow-scheduler
AIRFLOW = $(EXEC) airflow

.PHONY: up down reset logs shell data backfill rerun unpause inspect test test-fast clean \
	analytics analytics-refresh analytics-down

# ---------------------------------------------------------------- Airflow (Docker)
up:                 ## build image and start Airflow: http://localhost:8080 (airflow / airflow)
	mkdir -p logs data && AIRFLOW_UID=$$(id -u) docker compose up -d --build

ALL_PROFILES = --profile analytics --profile minio

down:               ## stop all containers, incl. analytics (run history is kept)
	docker compose $(ALL_PROFILES) down

reset:              ## stop containers and delete Airflow/Superset history (Docker volumes)
	docker compose $(ALL_PROFILES) down -v
	mkdir -p data/trino-metastore && find data/trino-metastore -mindepth 1 -delete

logs:               ## follow scheduler logs
	docker compose logs -f airflow-scheduler

shell:              ## open a shell in the scheduler container
	docker compose exec airflow-scheduler bash

# ---------------------------------------------------------------- pipeline
data:               ## generate DAYS days of synthetic landing files into data/landing
	$(EXEC) python -m ecommerce.datagen --start-date $(START) --days $(DAYS) --clean

backfill:           ## process START..END through Airflow (re-runs days already processed)
	$(AIRFLOW) dags backfill ecommerce_daily_batch -s $(START) -e $(END) --reset-dagruns -y

rerun:              ## re-run one day through Airflow (idempotency demo): make rerun DATE=2024-01-03
	$(AIRFLOW) dags backfill ecommerce_daily_batch -s $(DATE) -e $(DATE) --reset-dagruns -y

unpause:            ## turn on the daily schedule
	$(AIRFLOW) dags unpause ecommerce_daily_batch

inspect:            ## print row counts, daily revenue, SCD2 history, quarantine reasons, metrics
	$(EXEC) python scripts/inspect_lake.py

test:               ## all tests incl. end-to-end (~6 min)
	$(EXEC) python -m pytest -p no:cacheprovider

test-fast:          ## unit tests + DAG check (~1 min)
	$(EXEC) python -m pytest -p no:cacheprovider -m "not slow"

# Empty the folders instead of removing them, so running containers keep their mounts.
clean:              ## delete processed data (data/lake); landing CSVs are kept
	mkdir -p data/lake data/trino-metastore
	find data/lake data/trino-metastore -mindepth 1 -delete

# ---------------------------------------------------------------- analytics (Trino + Superset)
analytics:          ## start Trino + Superset, register Gold tables, build the dashboard
	mkdir -p data/lake data/trino-metastore
	docker compose --profile analytics up -d --build trino superset
	$(MAKE) analytics-refresh

analytics-refresh:  ## re-register new tables in Trino and rebuild the Superset dashboard
	docker compose exec -T trino sh /etc/trino/scripts/register_tables.sh
	docker compose exec -T superset /app/.venv/bin/python /app/provision/provision_dashboard.py

analytics-down:     ## stop only Trino + Superset
	docker compose --profile analytics stop trino superset
