COMPOSE = docker compose
RUN = $(COMPOSE) run --rm -T tools python -m

.PHONY: jar build up down reset seed load reconcile chaos evolve schema-resolve maintain bench-latency bench-maintenance report upgrade logs trino

jar:
	docker run --rm -v cdc_m2:/root/.m2 -v "$(CURDIR)/flink/job:/src" -w /src maven:3.9.16-eclipse-temurin-17 mvn -q -B package -DskipTests

build: jar
	$(COMPOSE) build

up: build
	mkdir -p .state
	$(COMPOSE) up -d
	$(RUN) tools.wait

seed:
	$(RUN) generator.generator bootstrap
	$(RUN) tools.connector register
	$(RUN) chaos.reconcile --wait 900

load:
	$(RUN) generator.generator run --rate 100 --probe

reconcile:
	$(RUN) chaos.reconcile --full

chaos:
	$(RUN) chaos.chaos

evolve:
	$(COMPOSE) run --rm -T tools python schema-evolution/evolve.py

schema-resolve:
	$(RUN) controller.resolve $(TABLE) $(ACTION) $(ARGS)

maintain:
	$(RUN) maintenance.maintain --once

bench-latency:
	$(RUN) bench.sweep

bench-maintenance:
	$(RUN) bench.maintenance

report:
	$(RUN) tools.report

upgrade: jar
	$(RUN) tools.upgrade

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v --remove-orphans
	rm -rf .state flink/job.args

logs:
	$(COMPOSE) logs -f --tail=100 controller

trino:
	$(COMPOSE) exec trino trino --catalog iceberg --schema cdc
