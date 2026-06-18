# kokedb — local dev workflow.
#
#   make up                # start PostgreSQL (meta + test source) in background
#   make run               # run the kokedb MySQL server on the host (needs `make up`)
#   make mysql             # open a mysql client against the running server
#   make test              # DB-free unit tests
#   make integration-test  # spin up the DB + run the DB-backed tests against it
#   make ps / down / reset # lifecycle
#
# Run `make help` to list every target.

COMPOSE         ?= docker compose --project-directory . -f deploy/docker-compose.yml
PG_SERVICE      ?= postgres
PG_PORT         ?= 25432
MYSQL_PORT      ?= 3306
# Exported so docker-compose can interpolate ${PG_PORT} for the host port.
export PG_PORT

# Connection strings. PG_META_DSN points the server at the meta `kokedb` DB;
# KOKEDB_TEST_DSN points the integration tests at the seeded `postgres` DB.
PG_META_DSN     ?= postgresql://postgres:123456@127.0.0.1:$(PG_PORT)/kokedb
KOKEDB_TEST_DSN ?= postgresql://postgres:123456@127.0.0.1:$(PG_PORT)/postgres

# pyo3 links libpython; the server binary and any test binary need it on the
# loader path at runtime. Derive the dir from python3.12 (overridable).
PYTHON_LIBDIR   ?= $(shell python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))' 2>/dev/null)
export DYLD_FALLBACK_LIBRARY_PATH := $(PYTHON_LIBDIR):$(DYLD_FALLBACK_LIBRARY_PATH)
export LD_LIBRARY_PATH := $(PYTHON_LIBDIR):$(LD_LIBRARY_PATH)

# Local on-disk state the server writes (wiped by `make reset`).
CACHE_DIRS      ?= /tmp/kokedb-cache /tmp/remote_catalog

.DEFAULT_GOAL := help
.PHONY: help up down reset restart ps logs psql wait-db build run server mysql \
        test integration-test itest clean

help: ## Show this help
	@printf "kokedb local-dev targets:\n\n"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z][a-zA-Z_-]*:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf "\nDB:     127.0.0.1:$(PG_PORT)   Server (after make run): 127.0.0.1:$(MYSQL_PORT)\n"

up: ## Start PostgreSQL (meta + test source) in the background and wait until healthy
	$(COMPOSE) up -d
	@$(MAKE) --no-print-directory wait-db
	@printf "\nPostgreSQL ready on 127.0.0.1:$(PG_PORT).\n"
	@printf "Start the server:  make run\n"

down: ## Stop and remove the DB container (KEEPS the data volume)
	$(COMPOSE) down

reset: ## Stop, wipe the DB volume AND local cache/parquet dirs (fresh state)
	$(COMPOSE) down -v --remove-orphans
	@rm -rf $(CACHE_DIRS)
	@printf "Reset complete: DB volume + %s removed.\n" "$(CACHE_DIRS)"

restart: ## Restart the DB container (no rebuild)
	$(COMPOSE) restart $(PG_SERVICE)

ps: ## Show container + healthcheck status
	$(COMPOSE) ps

logs: ## Tail DB logs (Ctrl-C to detach)
	$(COMPOSE) logs -f --tail=200 $(PG_SERVICE)

psql: ## Open a psql shell on the meta `kokedb` database
	$(COMPOSE) exec $(PG_SERVICE) psql -U postgres -d kokedb

wait-db: ## Block until the DB healthcheck reports healthy
	@printf "Waiting for PostgreSQL"
	@cid=$$($(COMPOSE) ps -q $(PG_SERVICE)); \
	for i in $$(seq 1 60); do \
		status=$$(docker inspect -f '{{.State.Health.Status}}' $$cid 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then printf " ready\n"; exit 0; fi; \
		printf "."; sleep 1; \
	done; \
	printf "\nDB did not become healthy in time. Try: make logs\n"; exit 1

build: ## Build the workspace (debug)
	cargo build --workspace

run: server ## Alias for `make server`
server: ## Run the kokedb MySQL server on the host (needs `make up` first)
	PG_META_DSN="$(PG_META_DSN)" cargo run

mysql: ## Open a mysql client against the running kokedb server
	mysql -h 127.0.0.1 -P $(MYSQL_PORT) -u root

test: ## Run DB-free unit tests (integration tests are #[ignore]d)
	cargo test --workspace

integration-test: up ## Bring up the DB and run the DB-backed (#[ignore]d) tests against it
	PG_META_DSN="$(PG_META_DSN)" KOKEDB_TEST_DSN="$(KOKEDB_TEST_DSN)" \
		cargo test --workspace -- --include-ignored
itest: integration-test ## Alias for `make integration-test`

clean: ## cargo clean + stop/wipe the DB stack
	cargo clean
	$(COMPOSE) down -v --remove-orphans
