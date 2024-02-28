
export DOCKER_BUILDKIT = 1

DOCKER_RUN_CMD = docker compose run --rm --user `id -u`:`id -g` tester

LAST_TAG = $(shell git describe --abbrev=0 --tags)

default: help

.PHONY: help
help: ## Display this help message
	@echo "Usage: make <target>"
	@echo
	@echo "Possible targets:"
	@grep -Eh '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "    %-20s%s\n", $$1, $$2}'

build: ## Build docker images
build: docker-build-publisher

dist:
	docker compose run --rm --user `id -u`:`id -g` tester sh -c "cd /src && pyinstaller --clean /src/cli.spec"
	chmod +x reims_publisher/dist/cli

package: dist
	rm -rf reims_publisher-$(LAST_TAG)-linux-amd64/
	cp -r reims_publisher/dist/ reims_publisher-$(LAST_TAG)-linux-amd64/
	tar -czvf reims_publisher-$(LAST_TAG)-linux-amd64.tar.gz reims_publisher-$(LAST_TAG)-linux-amd64

docker-build-publisher:
	docker build -t camptocamp/reims_publisher:latest reims_publisher

checks: ## Run linting
	$(DOCKER_RUN_CMD) flake8 /src/reims_publisher
	$(DOCKER_RUN_CMD) black --check /src/reims_publisher

black: ## Run black formatter
	$(DOCKER_RUN_CMD) black /src/reims_publisher

up: ## Start the composition
	docker compose up -d

reinit: ## Drop databases and restart composition
	docker compose down -v -t1 && docker compose up -d

tests-debug: ## Run automated tests
	docker compose exec -T --user `id -u`:`id -g` tester pytest --trace -vv /src/tests

tests: ## Run automated tests
	docker compose exec -T --user `id -u`:`id -g` tester pytest /src/tests

clean: ## Stop composition, remove containers and images
	docker compose down -v -t1 --remove-orphans
	docker rmi camptocamp/reims_publisher:latest || true
	rm -rf reims_publisher/build
	rm -rf reims_publisher/dist

cli: ## Starts the cli
	docker compose exec --user `id -u`:`id -g` tester python /app/reims_publisher/cli.py

generate_dummy_src_data: ## Create a schema and tables, views
	docker compose exec -T src_db psql -U reims reims < ./db/dummy_data/dummy_sql.sql
