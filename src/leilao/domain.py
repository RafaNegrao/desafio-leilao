"""Regras de negócio puras do leilão.

Sem I/O e sem relógio: o resultado depende só do estado atual e da mensagem
(incluindo o horário carimbado pela ingestão). Por isso, reprocessar o mesmo
log de mensagens na mesma ordem sempre produz o mesmo resultado (ordem determinística).

Valores monetários em centavos (int) e horários em milissegundos epoch (int),
para evitar problemas de ponto flutuante.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from typing import Optional, Tuple

# Status do leilão
OPEN = "OPEN"
CLOSED = "CLOSED"

# Desfechos de uma operação
ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"
CLOSED_OK = "CLOSED"
IGNORED = "IGNORED"

# Motivos
OK = "OK"
NOT_HIGHER = "NOT_HIGHER"
BELOW_MIN_INCREMENT = "BELOW_MIN_INCREMENT"
BELOW_START_PRICE = "BELOW_START_PRICE"
ALREADY_LEADING = "ALREADY_LEADING"
AUCTION_ENDED = "AUCTION_ENDED"  # passou do horário, aguardando o evento de fechamento
AUCTION_CLOSED = "AUCTION_CLOSED"
AUCTION_NOT_FOUND = "AUCTION_NOT_FOUND"
INVALID_BID = "INVALID_BID"
ALREADY_CLOSED = "ALREADY_CLOSED"
TOO_EARLY = "TOO_EARLY"  # fechamento chegou antes do fim (prazo foi estendido)


@dataclass(frozen=True)
class AuctionState:
    auction_id: str
    status: str
    start_price_cents: int
    min_increment_cents: int
    ends_at_ms: int
    snipe_window_ms: int = 0       # janela final em que um lance estende o prazo
    snipe_extension_ms: int = 0    # quanto o prazo é estendido (anti-sniping)
    current_amount_cents: int = 0
    current_bidder: Optional[str] = None
    winner: Optional[str] = None
    version: int = 0               # cresce a cada mudança de estado (controle otimista)


@dataclass(frozen=True)
class Bid:
    op_id: str                     # chave de idempotência do lance
    auction_id: str
    user_id: str
    amount_cents: int
    received_at_ms: int            # horário do servidor (carimbado na ingestão)


@dataclass(frozen=True)
class CloseEvent:
    op_id: str
    auction_id: str
    received_at_ms: int


@dataclass(frozen=True)
class Result:
    op_id: str
    auction_id: str
    kind: str                      # "BID" | "CLOSE"
    outcome: str                   # ACCEPTED | REJECTED | CLOSED | IGNORED
    reason: str
    amount_cents: int = 0
    user_id: Optional[str] = None
    ends_at_ms: int = 0
    version: int = 0
    duplicate: bool = False        # True quando é a repetição de uma operação já processada

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @staticmethod
    def from_json(raw: str) -> "Result":
        return Result(**json.loads(raw))


def new_auction(auction_id: str, start_price_cents: int, min_increment_cents: int,
                ends_at_ms: int, snipe_window_ms: int = 0, snipe_extension_ms: int = 0) -> AuctionState:
    return AuctionState(auction_id, OPEN, start_price_cents, min_increment_cents,
                        ends_at_ms, snipe_window_ms, snipe_extension_ms)


def decide_bid(state: AuctionState, bid: Bid) -> Tuple[Optional[AuctionState], Result]:
    """Decide um lance. Retorna (novo_estado | None se nada mudou, resultado)."""

    def reject(reason: str):
        return None, Result(bid.op_id, bid.auction_id, "BID", REJECTED, reason,
                            bid.amount_cents, bid.user_id, state.ends_at_ms, state.version)

    if bid.amount_cents <= 0:
        return reject(INVALID_BID)
    if state.status == CLOSED:
        return reject(AUCTION_CLOSED)
    if bid.received_at_ms >= state.ends_at_ms:
        return reject(AUCTION_ENDED)

    if state.current_bidder is None:
        if bid.amount_cents < state.start_price_cents:
            return reject(BELOW_START_PRICE)
    else:
        if bid.amount_cents <= state.current_amount_cents:
            return reject(NOT_HIGHER)  # inclui empate: quem chegou antes na fila vence
        if bid.amount_cents < state.current_amount_cents + state.min_increment_cents:
            return reject(BELOW_MIN_INCREMENT)
        if bid.user_id == state.current_bidder:
            return reject(ALREADY_LEADING)

    ends_at = state.ends_at_ms
    if state.snipe_window_ms > 0 and ends_at - bid.received_at_ms <= state.snipe_window_ms:
        ends_at = max(ends_at, bid.received_at_ms + state.snipe_extension_ms)

    new_state = replace(state, current_amount_cents=bid.amount_cents,
                        current_bidder=bid.user_id, ends_at_ms=ends_at,
                        version=state.version + 1)
    return new_state, Result(bid.op_id, bid.auction_id, "BID", ACCEPTED, OK,
                             bid.amount_cents, bid.user_id, ends_at, new_state.version)


def decide_close(state: AuctionState, event: CloseEvent) -> Tuple[Optional[AuctionState], Result]:
    """Decide o fechamento. O evento passa pela mesma fila/grupo dos lances."""
    if state.status == CLOSED:
        return None, Result(event.op_id, event.auction_id, "CLOSE", IGNORED, ALREADY_CLOSED,
                            state.current_amount_cents, state.winner, state.ends_at_ms, state.version)
    if event.received_at_ms < state.ends_at_ms:
        # o prazo foi estendido por anti-sniping: o agendador deve reagendar para ends_at_ms
        return None, Result(event.op_id, event.auction_id, "CLOSE", IGNORED, TOO_EARLY,
                            state.current_amount_cents, state.current_bidder,
                            state.ends_at_ms, state.version)
    new_state = replace(state, status=CLOSED, winner=state.current_bidder,
                        version=state.version + 1)
    return new_state, Result(event.op_id, event.auction_id, "CLOSE", CLOSED_OK, OK,
                             new_state.current_amount_cents, new_state.winner,
                             new_state.ends_at_ms, new_state.version)
