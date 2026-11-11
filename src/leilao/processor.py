"""Processador de lances: o "escritor único por leilão" da arquitetura.

Garantias:
- Idempotência: a mesma operação (op_id) nunca é contada duas vezes.
- Corretude sob concorrência: a gravação é condicional à versão do estado lida.
  Mesmo que dois processadores atuem no mesmo leilão (ex.: mensagem reentregue
  após timeout de visibilidade), só um commit vence; o outro relê e decide de novo.
- Mensagem malformada não trava o grupo: vira resultado INVALID_BID (sem persistir).
"""
from __future__ import annotations

from dataclasses import replace
from typing import Callable, Optional, Union

from . import domain
from .domain import Bid, CloseEvent, Result
from .ports import Repository


class InvalidMessage(ValueError):
    pass


class ConflictError(RuntimeError):
    pass


def parse_message(msg: dict) -> Union[Bid, CloseEvent]:
    kind = msg.get("type") if isinstance(msg, dict) else None
    try:
        if kind == "BID":
            return Bid(op_id=str(msg["bid_id"]), auction_id=str(msg["auction_id"]),
                       user_id=str(msg["user_id"]), amount_cents=int(msg["amount_cents"]),
                       received_at_ms=int(msg["received_at_ms"]))
        if kind == "CLOSE":
            return CloseEvent(op_id=str(msg["event_id"]), auction_id=str(msg["auction_id"]),
                              received_at_ms=int(msg["received_at_ms"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidMessage(f"mensagem inválida: {exc!r}") from exc
    raise InvalidMessage(f"tipo desconhecido: {kind!r}")


class BidProcessor:
    def __init__(self, repo: Repository, max_attempts: int = 5,
                 on_result: Optional[Callable[[Result], None]] = None):
        self.repo = repo
        self.max_attempts = max_attempts
        self.on_result = on_result  # ex.: atualizar o cache de preço / publicar evento

    def process(self, message: dict) -> Result:
        try:
            op = parse_message(message)
        except InvalidMessage:
            info = message if isinstance(message, dict) else {}
            return Result(str(info.get("bid_id", "?")), str(info.get("auction_id", "?")),
                          "BID", domain.REJECTED, domain.INVALID_BID)

        for _ in range(self.max_attempts):
            existing = self.repo.get_result(op.auction_id, op.op_id)
            if existing is not None:
                return replace(existing, duplicate=True)

            state = self.repo.get_state(op.auction_id)
            if state is None:
                return Result(op.op_id, op.auction_id,
                              "BID" if isinstance(op, Bid) else "CLOSE",
                              domain.REJECTED, domain.AUCTION_NOT_FOUND)

            if isinstance(op, Bid):
                new_state, result = domain.decide_bid(state, op)
            else:
                new_state, result = domain.decide_close(state, op)

            # a versão lida protege também as rejeições: uma decisão baseada em
            # estado velho (ex.: prazo estendido por outro lance) não é gravada
            if self.repo.commit(op.auction_id, state.version, new_state, result):
                if self.on_result is not None:
                    self.on_result(result)
                return result
            # conflito: outro escritor mudou o estado ou gravou esta operação; relê e decide de novo

        raise ConflictError(f"conflitos repetidos ao processar {op.op_id} em {op.auction_id}")
