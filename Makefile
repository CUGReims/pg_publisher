
export DOCKER_BUILDKIT = 1

DOCKER_RUN_CMD = docker-compose run --rm --user `id -u`:`id -g` tester

default: help

.PHONY: help
help: ## Display this help message
	@echo "Usage: make <target>"
	@echo
	@echo "Possible targets:"
	@grep -Eh '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "    %-20s%s\n", $$1, $$2}'

build: ## Build docker images
build: docker-build-publisher

docker-build-publisher:
	docker build -t camptocamp/reims_publisher:latest reims_publisher

checks: ## Run linting
	$(DOCKER_RUN_CMD) flake8 /src/reims_publisher
	$(DOCKER_RUN_CMD) black --check /src/reims_publisher

black: ## Run black formatter
	$(DOCKER_RUN_CMD) black /src/reims_publisher

up: ## Start the composition
	docker-compose up -d

tests-debug: ## Run automated tests
	docker-compose exec -T --user `id -u`:`id -g` tester pytest --trace -vv /src/tests

tests: ## Run automated tests
	docker-compose exec -T --user `id -u`:`id -g` tester pytest /src/tests

clean: ## Stop composition, remove containers and images
	docker-compose down -v -t1 --remove-orphans
	docker rmi camptocamp/reims_publisher:latest || true

cli: ## Stop composition, remove containers and images
	docker-compose exec -T --user `id -u`:`id -g` tester python /app/reims_publisher/cli.py
