"""Repositório em memória com a mesma semântica atômica do DynamoDB (para testes)."""
from __future__ import annotations

import threading
from typing import Dict, List, Optional, Tuple

from .domain import ACCEPTED, AuctionState, Result


class InMemoryRepository:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: Dict[str, AuctionState] = {}
        self._results: Dict[Tuple[str, str], Result] = {}
        self._accepted: Dict[str, List[Result]] = {}

    def create_auction(self, state: AuctionState) -> None:
        with self._lock:
            if state.auction_id in self._states:
                raise ValueError("leilão já existe")
            self._states[state.auction_id] = state

    def get_state(self, auction_id: str) -> Optional[AuctionState]:
        with self._lock:
            return self._states.get(auction_id)

    def get_result(self, auction_id: str, op_id: str) -> Optional[Result]:
        with self._lock:
            return self._results.get((auction_id, op_id))

    def commit(self, auction_id: str, expected_version: int,
               new_state: Optional[AuctionState], result: Result) -> bool:
        with self._lock:  # equivale a uma transação condicional
            current = self._states.get(auction_id)
            if current is None or current.version != expected_version:
                return False
            if (auction_id, result.op_id) in self._results:
                return False
            if new_state is not None:
                assert new_state.version == expected_version + 1
                self._states[auction_id] = new_state
            self._results[(auction_id, result.op_id)] = result
            if result.outcome == ACCEPTED:
                self._accepted.setdefault(auction_id, []).append(result)
            return True

    def accepted_bids(self, auction_id: str) -> List[Result]:
        with self._lock:
            return list(self._accepted.get(auction_id, []))
