"""Interfaces (portas) usadas pelo núcleo. Implementações: memória (testes) e AWS."""
from __future__ import annotations

from typing import List, Optional, Protocol

from .domain import AuctionState, Result


class Repository(Protocol):
    def create_auction(self, state: AuctionState) -> None: ...

    def get_state(self, auction_id: str) -> Optional[AuctionState]: ...

    def get_result(self, auction_id: str, op_id: str) -> Optional[Result]: ...

    def commit(self, auction_id: str, expected_version: int,
               new_state: Optional[AuctionState], result: Result) -> bool:
        """Grava de forma atômica: (estado novo, se houver) + resultado/idempotência.

        Só grava se a versão do estado ainda for `expected_version` e a operação
        ainda não tiver sido registrada. Retorna False em caso de conflito.
        """
        ...

    def accepted_bids(self, auction_id: str) -> List[Result]: ...


class Queue(Protocol):
    def send(self, group_id: str, dedup_id: str, body: dict) -> bool: ...


class PriceCache(Protocol):
    def get(self, auction_id: str) -> Optional[int]: ...

    def set(self, auction_id: str, amount_cents: int) -> None: ...
