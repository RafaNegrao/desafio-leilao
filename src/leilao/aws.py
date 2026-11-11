"""Criação dos clientes boto3 e nomes dos recursos (configuráveis por variáveis de ambiente).

Local: DYNAMODB_ENDPOINT_URL / SQS_ENDPOINT_URL apontam para DynamoDB Local e ElasticMQ
(ou AWS_ENDPOINT_URL para um emulador único, como Floci ou LocalStack).
Na AWS real, basta não definir os endpoints: valem as credenciais e a região padrão.
"""
from __future__ import annotations

import os

TABLE_NAME = os.getenv("TABLE_NAME", "auctions")
QUEUE_NAME = os.getenv("QUEUE_NAME", "bids.fifo")
DLQ_NAME = os.getenv("DLQ_NAME", "bids-dlq.fifo")


def client(service: str):
    import boto3  # importado aqui para o núcleo e os testes unitários não dependerem do boto3

    endpoint = os.getenv(f"{service.upper()}_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    if endpoint:  # emulador local: credenciais fictícias
        kwargs.update(endpoint_url=endpoint,
                      aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "local"),
                      aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "local"))
    return boto3.client(service, **kwargs)


def queue_url(sqs, name: str = QUEUE_NAME) -> str:
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]
