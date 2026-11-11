"""Consumidores da fila FIFO: um loop local (docker-compose) e o handler Lambda equivalente.

No SQS FIFO, enquanto uma mensagem de um grupo está em processamento, as seguintes do
mesmo grupo não são entregues a outro consumidor. É isso que dá o "escritor único por
leilão" sem lock explícito; para escalar, basta rodar mais consumidores.
"""
from __future__ import annotations

import json
import sys
from typing import Callable, Optional

from .processor import BidProcessor


def run_worker(sqs, queue_url: str, processor: BidProcessor, wait_seconds: int = 2,
               max_idle_polls: Optional[int] = None, log: Callable[[str], None] = print) -> None:
    idle = 0
    while True:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10,
                                   WaitTimeSeconds=wait_seconds)
        messages = resp.get("Messages", [])
        if not messages:
            idle += 1
            if max_idle_polls is not None and idle >= max_idle_polls:
                return
            continue
        idle = 0
        for m in messages:  # já vêm em ordem dentro de cada grupo
            try:
                processor.process(json.loads(m["Body"]))
            except Exception as exc:  # não apaga: a mensagem volta após o visibility timeout
                log(f"erro ao processar {m.get('MessageId')}: {exc!r}")
                break  # preserva a ordem: não segue para as mensagens seguintes do lote
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])


def lambda_handler(event, context=None):
    """Handler para o event source mapping SQS -> Lambda (com ReportBatchItemFailures).

    Ao falhar uma mensagem, ela e todas as seguintes do lote são devolvidas, para manter a
    ordem; depois de `maxReceiveCount` tentativas a mensagem vai para a DLQ.
    """
    processor = _processor()
    falhas = []
    registros = event.get("Records", [])
    for i, record in enumerate(registros):
        try:
            processor.process(json.loads(record["body"]))
        except Exception as exc:
            print(f"erro ao processar {record.get('messageId')}: {exc!r}", file=sys.stderr)
            falhas = [{"itemIdentifier": r["messageId"]} for r in registros[i:]]
            break
    return {"batchItemFailures": falhas}


_cached: Optional[BidProcessor] = None


def _processor() -> BidProcessor:
    global _cached
    if _cached is None:
        from . import aws
        from .repo_dynamodb import DynamoRepository
        _cached = BidProcessor(DynamoRepository(aws.client("dynamodb"), aws.TABLE_NAME))
    return _cached
