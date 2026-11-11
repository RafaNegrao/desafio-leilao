"""Envio para a fila SQS FIFO."""
from __future__ import annotations

import json


class SqsQueue:
    def __init__(self, client, queue_url: str):
        self.client = client
        self.queue_url = queue_url

    def send(self, group_id: str, dedup_id: str, body: dict) -> bool:
        self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(body, separators=(",", ":")),
            MessageGroupId=group_id,              # ordem por leilão
            MessageDeduplicationId=dedup_id)      # chave de idempotência
        return True
