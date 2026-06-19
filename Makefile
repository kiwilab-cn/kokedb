# kokedb — local dev workflow (fully containerized: PostgreSQL + server).
#
#   make up                # build (if needed) + start the whole stack
#   make mysql             # mysql client against the running server
#   make logs              # tail server logs
#   make ps / down / reset # lifecycle
#   make test              # DB-free unit tests (host)
#   make integration-test  # start the DB + run DB-backed tests against it (host)
#
# Run `make help` to list every target.

COMPOSE         ?= docker compose --project-directory . -f deploy/docker-compose.yml
DB_SERVICE      ?= postgres
SERVER_SERVICE  ?= kokedb-server
PG_PORT         ?= 25432
MYSQL_PORT      ?= 3306
METRICS_PORT    ?= 9090
# Exported so docker-compose can interpolate the host port bindings.
export PG_PORT
export MYSQL_PORT
export METRICS_PORT

# Connection strings for HOST-side tests (containers use in-network DSNs).
PG_META_DSN     ?= postgresql://postgres:123456@127.0.0.1:$(PG_PORT)/kokedb
KOKEDB_TEST_DSN ?= postgresql://postgres:123456@127.0.0.1:$(PG_PORT)/postgres

# pyo3 links libpython; host test binaries need it on the loader path.
PYTHON_LIBDIR   ?= $(shell python3.12 -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))' 2>/dev/null)
export DYLD_FALLBACK_LIBRARY_PATH := $(PYTHON_LIBDIR):$(DYLD_FALLBACK_LIBRARY_PATH)
export LD_LIBRARY_PATH := $(PYTHON_LIBDIR):$(LD_LIBRARY_PATH)

# Local on-disk state wiped by `make reset` (server cache now lives in a volume).
CACHE_DIRS      ?= /tmp/kokedb-cache /tmp/remote_catalog

.DEFAULT_GOAL := help
.PHONY: help up up-db down reset restart ps logs logs-db psql sh mysql build rebuild \
        wait-db test integration-test itest bench clean

help: ## Show this help
	@printf "kokedb local-dev targets:\n\n"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z][a-zA-Z_-]*:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf "\nDB: 127.0.0.1:$(PG_PORT)   Server: 127.0.0.1:$(MYSQL_PORT)\n"

up: ## Build (if needed) + start the whole stack (postgres + server)
	$(COMPOSE) up -d --build
	@printf "\nStack starting. Server on 127.0.0.1:$(MYSQL_PORT) once healthy.\n"
	@printf "Watch:  make logs        Status: make ps        Client: make mysql\n"

up-db: ## Start ONLY postgres (for host-side dev / tests)
	$(COMPOSE) up -d $(DB_SERVICE)
	@$(MAKE) --no-print-directory wait-db

down: ## Stop and remove containers (KEEPS volumes)
	$(COMPOSE) down

reset: ## Stop, wipe volumes AND local cache dirs (fresh state)
	$(COMPOSE) down -v --remove-orphans
	@rm -rf $(CACHE_DIRS)
	@printf "Reset complete: volumes + %s removed.\n" "$(CACHE_DIRS)"

restart: ## Restart the server container (no rebuild)
	$(COMPOSE) restart $(SERVER_SERVICE)

ps: ## Show containers + healthcheck status
	$(COMPOSE) ps

logs: ## Tail server logs (Ctrl-C to detach)
	$(COMPOSE) logs -f --tail=200 $(SERVER_SERVICE)

logs-db: ## Tail DB logs
	$(COMPOSE) logs -f --tail=200 $(DB_SERVICE)

psql: ## Open a psql shell on the meta `kokedb` database
	$(COMPOSE) exec $(DB_SERVICE) psql -U postgres -d kokedb

sh: ## Open a shell inside the running server container
	$(COMPOSE) exec $(SERVER_SERVICE) bash

mysql: ## Open a mysql client against the running kokedb server
	mysql -h 127.0.0.1 -P $(MYSQL_PORT) -u root

build: ## Build the server image
	$(COMPOSE) build $(SERVER_SERVICE)

rebuild: ## Build the server image with no cache
	$(COMPOSE) build --no-cache $(SERVER_SERVICE)

wait-db: ## Block until the DB healthcheck reports healthy
	@printf "Waiting for PostgreSQL"
	@cid=$$($(COMPOSE) ps -q $(DB_SERVICE)); \
	for i in $$(seq 1 60); do \
		status=$$(docker inspect -f '{{.State.Health.Status}}' $$cid 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then printf " ready\n"; exit 0; fi; \
		printf "."; sleep 1; \
	done; \
	printf "\nDB did not become healthy in time. Try: make logs-db\n"; exit 1

test: ## Run DB-free unit tests on the host (integration tests are #[ignore]d)
	cargo test --workspace

integration-test: up-db ## Start the DB and run the DB-backed (#[ignore]d) tests against it (host)
	# --test-threads=1: DB tests share one database and run concurrent DDL
	# (init_db), so they must be serialized.
	PG_META_DSN="$(PG_META_DSN)" KOKEDB_TEST_DSN="$(KOKEDB_TEST_DSN)" \
		cargo test --workspace -- --include-ignored --test-threads=1
itest: integration-test ## Alias for `make integration-test`

bench: ## Run the query-acceleration benchmark (see doc/benchmark.md)
	./scripts/benchmark.sh

clean: ## cargo clean + stop/wipe the whole stack
	cargo clean
	$(COMPOSE) down -v --remove-orphans
