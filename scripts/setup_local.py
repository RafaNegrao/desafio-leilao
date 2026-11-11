"""Cria a tabela, as filas (FIFO + DLQ FIFO) e um leilão de exemplo nos emuladores locais."""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from leilao import aws  # noqa: E402
from leilao.domain import new_auction  # noqa: E402
from leilao.ingest import now_ms  # noqa: E402
from leilao.repo_dynamodb import DynamoRepository  # noqa: E402


def esperar(fn, descricao, tentativas=60):
    for i in range(tentativas):
        try:
            return fn()
        except Exception as exc:  # emulador ainda subindo
            if i == tentativas - 1:
                raise
            print(f"aguardando {descricao}... ({exc.__class__.__name__})")
            time.sleep(1)


def main():
    ddb, sqs = aws.client("dynamodb"), aws.client("sqs")

    existentes = esperar(lambda: ddb.list_tables()["TableNames"], "DynamoDB")
    if aws.TABLE_NAME not in existentes:
        ddb.create_table(
            TableName=aws.TABLE_NAME, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}])
    print(f"tabela pronta: {aws.TABLE_NAME}")

    dlq_url = esperar(lambda: sqs.create_queue(QueueName=aws.DLQ_NAME,
                                               Attributes={"FifoQueue": "true"})["QueueUrl"], "SQS")
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.create_queue(QueueName=aws.QUEUE_NAME, Attributes={
        "FifoQueue": "true",
        "ContentBasedDeduplication": "false",   # a chave de idempotência é enviada explicitamente
        "VisibilityTimeout": "30",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})})
    print(f"filas prontas: {aws.QUEUE_NAME} (DLQ: {aws.DLQ_NAME})")

    repo = DynamoRepository(ddb, aws.TABLE_NAME)
    if repo.get_state("leilao-1") is None:
        repo.create_auction(new_auction("leilao-1", start_price_cents=10_000, min_increment_cents=100,
                                        ends_at_ms=now_ms() + 3_600_000,
                                        snipe_window_ms=30_000, snipe_extension_ms=60_000))
    print("leilão de exemplo pronto: leilao-1")


if __name__ == "__main__":
    main()
