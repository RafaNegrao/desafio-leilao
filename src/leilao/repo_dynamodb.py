"""Repositório no DynamoDB (tabela única).

Itens (chave de partição `pk` = AUCTION#<id>):
  sk = STATE           estado do leilão (preço atual, líder, prazo, status, version)
  sk = BID#<version>   lance aceito, na ordem em que foi aceito (trilha de auditoria)
  sk = OP#<op_id>      resultado de qualquer operação (aceita ou rejeitada): é a idempotência

O commit é UMA transação: condição na versão do STATE (controle otimista) + registro
do OP#<id> só se ainda não existir. Aceitos também gravam o BID#.
"""
from __future__ import annotations

from typing import List, Optional

from botocore.exceptions import ClientError

from .domain import ACCEPTED, AuctionState, Result


def _s(value: Optional[str]) -> dict:
    return {"NULL": True} if value is None else {"S": value}


def _n(value: int) -> dict:
    return {"N": str(int(value))}


def _opt(item: dict, key: str) -> Optional[str]:
    attr = item.get(key, {})
    return attr.get("S")


def _state_item(s: AuctionState) -> dict:
    return {
        "pk": _s(f"AUCTION#{s.auction_id}"), "sk": _s("STATE"),
        "auction_id": _s(s.auction_id), "status": _s(s.status),
        "start_price_cents": _n(s.start_price_cents), "min_increment_cents": _n(s.min_increment_cents),
        "ends_at_ms": _n(s.ends_at_ms), "snipe_window_ms": _n(s.snipe_window_ms),
        "snipe_extension_ms": _n(s.snipe_extension_ms), "current_amount_cents": _n(s.current_amount_cents),
        "current_bidder": _s(s.current_bidder), "winner": _s(s.winner), "version": _n(s.version),
    }


def _to_state(item: dict) -> AuctionState:
    return AuctionState(
        auction_id=item["auction_id"]["S"], status=item["status"]["S"],
        start_price_cents=int(item["start_price_cents"]["N"]),
        min_increment_cents=int(item["min_increment_cents"]["N"]),
        ends_at_ms=int(item["ends_at_ms"]["N"]), snipe_window_ms=int(item["snipe_window_ms"]["N"]),
        snipe_extension_ms=int(item["snipe_extension_ms"]["N"]),
        current_amount_cents=int(item["current_amount_cents"]["N"]),
        current_bidder=_opt(item, "current_bidder"), winner=_opt(item, "winner"),
        version=int(item["version"]["N"]),
    )


class DynamoRepository:
    def __init__(self, client, table: str):
        self.client = client
        self.table = table

    @staticmethod
    def _pk(auction_id: str) -> dict:
        return _s(f"AUCTION#{auction_id}")

    def create_auction(self, state: AuctionState) -> None:
        self.client.put_item(TableName=self.table, Item=_state_item(state),
                             ConditionExpression="attribute_not_exists(pk)")

    def get_state(self, auction_id: str) -> Optional[AuctionState]:
        resp = self.client.get_item(TableName=self.table, ConsistentRead=True,
                                    Key={"pk": self._pk(auction_id), "sk": _s("STATE")})
        item = resp.get("Item")
        return _to_state(item) if item else None

    def get_result(self, auction_id: str, op_id: str) -> Optional[Result]:
        resp = self.client.get_item(TableName=self.table, ConsistentRead=True,
                                    Key={"pk": self._pk(auction_id), "sk": _s(f"OP#{op_id}")})
        item = resp.get("Item")
        return Result.from_json(item["result"]["S"]) if item else None

    def commit(self, auction_id: str, expected_version: int,
               new_state: Optional[AuctionState], result: Result) -> bool:
        pk = self._pk(auction_id)
        state_key = {"pk": pk, "sk": _s("STATE")}
        items: List[dict] = []

        if new_state is not None:
            items.append({"Update": {
                "TableName": self.table, "Key": state_key,
                "UpdateExpression": ("SET #status = :status, current_amount_cents = :amount, "
                                     "current_bidder = :bidder, winner = :winner, "
                                     "ends_at_ms = :ends, version = :new_version"),
                "ConditionExpression": "version = :expected",
                "ExpressionAttributeNames": {"#status": "status"},
                "ExpressionAttributeValues": {
                    ":status": _s(new_state.status), ":amount": _n(new_state.current_amount_cents),
                    ":bidder": _s(new_state.current_bidder), ":winner": _s(new_state.winner),
                    ":ends": _n(new_state.ends_at_ms), ":new_version": _n(new_state.version),
                    ":expected": _n(expected_version)},
            }})
            if result.outcome == ACCEPTED:
                items.append({"Put": {
                    "TableName": self.table,
                    "Item": {"pk": pk, "sk": _s(f"BID#{new_state.version:012d}"),
                             "op_id": _s(result.op_id), "user_id": _s(result.user_id),
                             "amount_cents": _n(result.amount_cents), "version": _n(new_state.version),
                             "result": _s(result.to_json())},
                    "ConditionExpression": "attribute_not_exists(pk)"}})
        else:
            # nada muda no estado, mas a decisão só vale se o estado lido ainda é o atual
            items.append({"ConditionCheck": {
                "TableName": self.table, "Key": state_key,
                "ConditionExpression": "version = :expected",
                "ExpressionAttributeValues": {":expected": _n(expected_version)}}})

        items.append({"Put": {
            "TableName": self.table,
            "Item": {"pk": pk, "sk": _s(f"OP#{result.op_id}"), "result": _s(result.to_json())},
            "ConditionExpression": "attribute_not_exists(pk)"}})

        try:
            self.client.transact_write_items(TransactItems=items)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "TransactionCanceledException":
                return False  # condição falhou (versão mudou / operação já registrada) ou conflito de transação
            raise

    def accepted_bids(self, auction_id: str) -> List[Result]:
        results: List[Result] = []
        kwargs = dict(TableName=self.table, ConsistentRead=True,
                      KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
                      ExpressionAttributeValues={":pk": self._pk(auction_id), ":prefix": _s("BID#")})
        while True:
            resp = self.client.query(**kwargs)
            results.extend(Result.from_json(i["result"]["S"]) for i in resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                return results
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
