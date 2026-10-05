.PHONY: up connector stream generate verify queries evolve down clean
up:        ## start Postgres, Kafka, Debezium
	docker compose up -d postgres kafka connect
connector: ## register the Debezium connector
	./scripts/register_connector.sh
stream:    ## start the Spark streaming job
	docker compose up -d spark-stream
generate:  ## RATE=100 DURATION=300 make generate
	docker compose run --rm generator
verify:    ## reconcile Postgres vs Iceberg
	docker compose run --rm verify
queries:
	docker compose run --rm spark-sql -f /opt/sql/queries.sql
evolve:
	./scripts/evolve_schema.sh
down:
	docker compose --profile tools down
clean:
	docker compose --profile tools down -v
