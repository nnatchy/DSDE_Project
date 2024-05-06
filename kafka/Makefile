.PHONY: run docker_build down clean

run:
	docker-compose -f docker-compose.kafka.yaml up

down:
	docker-compose -f docker-compose.kafka.yaml down

docker_build:
	docker-compose -f docker-compose.kafka.yaml build

clean:
	docker-compose -f docker-compose.kafka.yaml down --rmi all --volumes --remove-orphans

logs:
	docker-compose -f docker-compose.kafka.yaml logs -f

restart:
	docker-compose -f docker-compose.kafka.yaml restart

ps:
	docker-compose -f docker-compose.kafka.yaml ps

rebuild: clean docker_build run
