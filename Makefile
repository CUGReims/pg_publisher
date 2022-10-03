
export DOCKER_BUILDKIT=1

build: docker-build-publisher

docker-build-publisher:
	docker build -t camptocamp/reims_publisher:latest reims_publisher
