"""Integração com emuladores (DynamoDB Local + ElasticMQ ou Floci/LocalStack).

Só roda com RUN_INTEGRATION=1 e emuladores no ar (`docker compose up -d dynamodb sqs`).
"""
import json
import os
import unittest
import uuid

RUN = os.getenv("RUN_INTEGRATION") == "1"


@unittest.skipUnless(RUN, "defina RUN_INTEGRATION=1 e suba os emuladores")
class AwsLocalIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from leilao import aws
        from leilao.repo_dynamodb import DynamoRepository

        cls.ddb, cls.sqs = aws.client("dynamodb"), aws.client("sqs")
        cls.table = f"t-{uuid.uuid4().hex[:8]}"
        cls.ddb.create_table(
            TableName=cls.table, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}])
        cls.queue_name = f"q-{uuid.uuid4().hex[:8]}.fifo"
        cls.url = cls.sqs.create_queue(QueueName=cls.queue_name, Attributes={"FifoQueue": "true"})["QueueUrl"]
        cls.repo = DynamoRepository(cls.ddb, cls.table)

    @classmethod
    def tearDownClass(cls):
        cls.ddb.delete_table(TableName=cls.table)
        cls.sqs.delete_queue(QueueUrl=cls.url)

    def test_commit_condicional_e_idempotencia_no_dynamodb(self):
        from leilao import domain
        from leilao.domain import Bid, new_auction

        self.repo.create_auction(new_auction("x1", 100, 10, ends_at_ms=10**12))
        state = self.repo.get_state("x1")
        new, res = domain.decide_bid(state, Bid("b1", "x1", "ana", 200, 1))
        self.assertTrue(self.repo.commit("x1", state.version, new, res))
        # mesma versão de novo: conflito
        new2, res2 = domain.decide_bid(state, Bid("b2", "x1", "bia", 300, 2))
        self.assertFalse(self.repo.commit("x1", state.version, new2, res2))
        # mesma operação (op_id) de novo, com a versão correta: também recusada
        atual = self.repo.get_state("x1")
        n3, r3 = domain.decide_bid(atual, Bid("b1", "x1", "ana", 999, 3))
        self.assertFalse(self.repo.commit("x1", atual.version, n3, r3))
        self.assertEqual(self.repo.get_result("x1", "b1").amount_cents, 200)
        self.assertEqual(len(self.repo.accepted_bids("x1")), 1)

    def test_fluxo_completo_fila_fifo_processador(self):
        from leilao.domain import new_auction
        from leilao.ingest import Ingestor
        from leilao.processor import BidProcessor
        from leilao.queue_sqs import SqsQueue
        from leilao.worker import run_worker

        self.repo.create_auction(new_auction("x2", 100, 10, ends_at_ms=10**15))
        ing = Ingestor(SqsQueue(self.sqs, self.url))
        for i, valor in enumerate((100, 150, 120, 200, 200)):
            ing.submit_bid("x2", f"u{i}", valor, f"k{i}")
        ing.submit_bid("x2", "u0", 100, "k0")  # retry: deduplicado pela fila
        run_worker(self.sqs, self.url, BidProcessor(self.repo), wait_seconds=1, max_idle_polls=2,
                   log=lambda m: None)
        valores = [r.amount_cents for r in self.repo.accepted_bids("x2")]
        self.assertEqual(valores, [100, 150, 200])  # 120 e o segundo 200 são rejeitados, na ordem de envio


if __name__ == "__main__":
    unittest.main()
