.DEFAULT_GOAL := help
SHELL := /bin/bash
API_KEY ?= desafio-2026

help: ## mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

seed: ## (re)gera os dados sinteticos (events.jsonl + SQL do Postgres)
	python3 seed/generate_data.py

up: seed ## sobe todo o ambiente
	docker compose up -d --build
	@echo ""
	@echo "Aguardando os servicos ficarem prontos..."
	@sleep 25
	@$(MAKE) --no-print-directory check

down: ## derruba os containers (mantem os dados)
	docker compose down

clean: ## derruba tudo e APAGA os volumes (MinIO, Postgres, Iceberg)
	docker compose down -v
	rm -f mock-api/data/.batch_state

restart: clean up ## ambiente do zero

ps: ## status dos servicos
	docker compose ps

logs: ## logs de todos os servicos (ctrl+c para sair)
	docker compose logs -f --tail=100

check: ## valida se o ambiente esta de pe (rapido)
	@bash scripts/verify_env.sh

rebuild-spark: ## reconstroi a imagem do Spark (adiciona hadoop-aws para o s3a://)
	docker compose build --no-cache --progress=plain spark
	docker compose up -d --force-recreate spark
	@echo "aguardando o Spark aceitar comandos (ate 90s)..."
	@for i in $$(seq 1 45); do \
		if docker compose exec -T spark true >/dev/null 2>&1; then break; fi; \
		sleep 2; \
	done
	@if ! docker compose exec -T spark true >/dev/null 2>&1; then \
		echo ""; \
		echo "O container do Spark nao subiu. Diagnostico:"; \
		echo "  docker compose ps spark"; \
		echo "  docker compose logs --tail=40 spark"; \
		exit 1; \
	fi
	@echo ""
	@echo "Imagem em uso:"
	@docker compose images spark
	@echo ""
	@echo "JARs de acesso a S3 no classpath:"
	@if docker compose exec -T spark sh -c "ls /opt/spark/jars | grep -E 'hadoop-aws|aws-java-sdk-bundle|^bundle-2'"; then \
		echo ""; \
		echo "OK. Agora rode: python3 scripts/test_infra.py"; \
	else \
		echo "  NENHUMA encontrada."; \
		echo ""; \
		echo "O container nao esta usando a imagem construida. Tente:"; \
		echo "  docker compose down spark && docker compose build --no-cache spark && docker compose up -d spark"; \
		exit 1; \
	fi

test-infra: ## validacao COMPLETA da infra (round-trip Spark/Iceberg/Trino)
	@python3 scripts/test_infra.py

test-infra-fast: ## validacao completa, sem o round-trip do Spark
	@python3 scripts/test_infra.py --skip-spark

batch2: ## simula o tempo passando: libera o batch 2 (API + Postgres)
	@curl -s -X POST -H "X-API-Key: $(API_KEY)" http://localhost:8000/admin/advance-batch | python3 -m json.tool
	@docker compose exec -T postgres psql -U app -d crm -f /seed/03_batch2.sql > /dev/null && echo "Postgres: batch 2 aplicado"

reset-batch: ## volta as fontes para o estado do batch 1 (so a API; o Postgres exige make clean)
	@curl -s -X POST -H "X-API-Key: $(API_KEY)" http://localhost:8000/admin/reset | python3 -m json.tool

batch-state: ## em qual batch as fontes estao
	@curl -s -H "X-API-Key: $(API_KEY)" http://localhost:8000/admin/state | python3 -m json.tool

spark: ## abre um shell dentro do container do Spark (repo montado em /home/iceberg/work)
	docker compose exec spark bash

pyspark: ## abre o PySpark shell ja conectado ao catalogo lakehouse
	docker compose exec spark pyspark

spark-sql: ## abre o spark-sql
	docker compose exec spark spark-sql

trino: ## abre o CLI do Trino
	docker compose exec trino trino --catalog lakehouse

psql: ## abre o psql na base crm
	docker compose exec postgres psql -U app -d crm

airflow: ## sobe o Airflow (perfil opcional) em http://localhost:8081
	docker compose --profile airflow up -d
	@echo "Airflow subindo. Usuario: admin | senha: veja 'docker compose logs airflow | grep Password'"

api-docs: ## lembra a URL da documentacao da Mock API
	@echo "http://localhost:8000/docs  (header X-API-Key: $(API_KEY))"

.PHONY: help seed up down clean restart ps logs check rebuild-spark test-infra test-infra-fast batch2 reset-batch batch-state spark pyspark spark-sql trino psql airflow api-docs
