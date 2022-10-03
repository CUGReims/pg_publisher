
export DOCKER_BUILDKIT=1

build: docker-build-publisher

docker-build-publisher:
	docker build -t camptocamp/reims_publisher:latest reims_publisher

up:
	docker-compose up -d

tests:
	docker-compose exec --user `id -u`:`id -g` tester pytest /src/tests -vv

clean:
	docker-compose down -v -t1
	docker rmi camptocamp/reims_publisher:latest || true
