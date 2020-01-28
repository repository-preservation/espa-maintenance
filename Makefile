
docker-deps-up:
	docker network create backend
	docker-compose -f setup/docker-compose.yml up -d

docker-deps-up-nodaemon:
	docker-compose -f setup/docker-compose.yml up

docker-deps-down:
	docker-compose -f setup/docker-compose.yml down
	docker network rm backend
	docker image rm setup_postgres:latest

runtests: 
	. ./test_env.sh && nose2

tests: docker-deps-up runtests docker-deps-down

