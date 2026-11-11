"""Ingestão: valida, carimba o horário do servidor e envia para a fila ordenada.

Em produção roda atrás do API Gateway (Lambda). O relógio do cliente é ignorado:
só vale `received_at_ms`, carimbado aqui.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from .domain import NOT_HIGHER
from .ports import PriceCache, Queue

ENQUEUED = "ENQUEUED"
REJECTED_FAST = "REJECTED_FAST"
INVALID = "INVALID"
MAX_AMOUNT_CENTS = 10**12


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class SubmitResult:
    status: str
    op_id: str
    reason: str = ""


class Ingestor:
    def __init__(self, queue: Queue, price_cache: Optional[PriceCache] = None,
                 clock_ms: Callable[[], int] = now_ms):
        self.queue = queue
        self.price_cache = price_cache
        self.clock_ms = clock_ms

    def submit_bid(self, auction_id: str, user_id: str, amount_cents: int,
                   idempotency_key: str) -> SubmitResult:
        if (not auction_id or not user_id or not idempotency_key
                or not isinstance(amount_cents, int) or isinstance(amount_cents, bool)
                or not 0 < amount_cents <= MAX_AMOUNT_CENTS):
            return SubmitResult(INVALID, idempotency_key or "", "INVALID_BID")

        # Filtro barato: a maioria dos lances em um pico já está abaixo do preço atual.
        if self.price_cache is not None:
            cached = self.price_cache.get(auction_id)
            if cached is not None and amount_cents <= cached:
                return SubmitResult(REJECTED_FAST, idempotency_key, NOT_HIGHER)

        body = {"type": "BID", "bid_id": idempotency_key, "auction_id": auction_id,
                "user_id": user_id, "amount_cents": amount_cents,
                "received_at_ms": self.clock_ms()}
        # MessageGroupId = leilão (ordem por leilão); MessageDeduplicationId = chave de idempotência
        self.queue.send(auction_id, idempotency_key, body)
        return SubmitResult(ENQUEUED, idempotency_key)

    def submit_close(self, auction_id: str, event_id: Optional[str] = None) -> SubmitResult:
        event_id = event_id or f"close-{uuid.uuid4()}"
        body = {"type": "CLOSE", "event_id": event_id, "auction_id": auction_id,
                "received_at_ms": self.clock_ms()}
        self.queue.send(auction_id, event_id, body)  # mesma fila e mesmo grupo dos lances
        return SubmitResult(ENQUEUED, event_id)
