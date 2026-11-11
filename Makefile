# Atalhos. Requer Python 3.12+ e Docker.
export PYTHONPATH := src
export DYNAMODB_ENDPOINT_URL ?= http://localhost:8000
export SQS_ENDPOINT_URL ?= http://localhost:9324
export AWS_REGION ?= us-east-1

.PHONY: install test up setup worker simulate integration down

install:
	pip install -r requirements-dev.txt

test:            ## testes unitários e de concorrência (não precisam de Docker nem de boto3)
	python -m unittest discover -s tests -v

up:              ## sobe os emuladores (DynamoDB Local + ElasticMQ)
	docker compose up -d dynamodb sqs

setup:           ## cria tabela, filas e leilão de exemplo
	python scripts/setup_local.py

worker:          ## consumidor da fila FIFO (deixe rodando em outro terminal)
	python scripts/run_worker.py

simulate:        ## gera carga concorrente e confere as garantias
	python scripts/simulate.py --bids 5000

integration:     ## testes contra os emuladores
	RUN_INTEGRATION=1 python -m unittest tests.test_integration_aws_local -v

down:
	docker compose down -v
