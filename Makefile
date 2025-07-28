.PHONY: reset-mongo generateKey set-permissions insert up down

insert:
	docker exec -it cdc-app node insert.js

up:
	docker compose up -d --build

down:
	docker compose down -v