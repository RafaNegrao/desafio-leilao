"""Cache do preço atual, usado SÓ para rejeitar cedo lances que já nasceram perdidos.

Nunca é fonte de verdade: a decisão final é sempre a escrita condicional no banco.
Em produção seria o ElastiCache (Redis); aqui, uma versão em memória.
"""
from __future__ import annotations

import threading
from typing import Dict, Optional


class InMemoryPriceCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._prices: Dict[str, int] = {}

    def get(self, auction_id: str) -> Optional[int]:
        with self._lock:
            return self._prices.get(auction_id)

    def set(self, auction_id: str, amount_cents: int) -> None:
        with self._lock:
            if amount_cents > self._prices.get(auction_id, 0):  # só sobe
                self._prices[auction_id] = amount_cents
