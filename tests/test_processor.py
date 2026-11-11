import unittest

from leilao import domain
from leilao.domain import new_auction
from leilao.ingest import ENQUEUED, INVALID, REJECTED_FAST, Ingestor
from leilao.cache import InMemoryPriceCache
from leilao.processor import BidProcessor
from leilao.queue_memory import InMemoryFifoQueue
from leilao.repo_memory import InMemoryRepository


def msg(op, user, amount, at=1000, auction="a1"):
    return {"type": "BID", "bid_id": op, "auction_id": auction, "user_id": user,
            "amount_cents": amount, "received_at_ms": at}


class ProcessorTest(unittest.TestCase):
    def setUp(self):
        self.repo = InMemoryRepository()
        self.repo.create_auction(new_auction("a1", 100, 10, ends_at_ms=10_000))
        self.proc = BidProcessor(self.repo)

    def test_lance_aceito_atualiza_estado(self):
        res = self.proc.process(msg("b1", "ana", 200))
        self.assertEqual(res.outcome, domain.ACCEPTED)
        state = self.repo.get_state("a1")
        self.assertEqual((state.current_amount_cents, state.current_bidder, state.version), (200, "ana", 1))

    def test_idempotencia_mesma_operacao_nao_conta_duas_vezes(self):
        first = self.proc.process(msg("b1", "ana", 200))
        again = self.proc.process(msg("b1", "ana", 200))
        self.assertFalse(first.duplicate)
        self.assertTrue(again.duplicate)
        self.assertEqual(again.outcome, first.outcome)
        self.assertEqual(self.repo.get_state("a1").version, 1)
        self.assertEqual(len(self.repo.accepted_bids("a1")), 1)

    def test_rejeicao_tambem_e_idempotente(self):
        self.proc.process(msg("b1", "ana", 200))
        r1 = self.proc.process(msg("b2", "bia", 150))
        r2 = self.proc.process(msg("b2", "bia", 150))
        self.assertEqual(r1.reason, domain.NOT_HIGHER)
        self.assertTrue(r2.duplicate)

    def test_leilao_inexistente(self):
        res = self.proc.process(msg("b1", "ana", 200, auction="nao-existe"))
        self.assertEqual(res.reason, domain.AUCTION_NOT_FOUND)

    def test_mensagem_malformada_nao_levanta_excecao(self):
        for bad in ({}, {"type": "BID"}, {"type": "X"}, {"type": "BID", "bid_id": "b", "auction_id": "a1",
                                                        "user_id": "u", "amount_cents": "abc", "received_at_ms": 1}):
            res = self.proc.process(bad)
            self.assertEqual(res.reason, domain.INVALID_BID)

    def test_fechamento_pela_mesma_via_dos_lances(self):
        self.proc.process(msg("b1", "ana", 200))
        res = self.proc.process({"type": "CLOSE", "event_id": "c1", "auction_id": "a1", "received_at_ms": 10_000})
        self.assertEqual(res.outcome, domain.CLOSED_OK)
        late = self.proc.process(msg("b2", "bia", 900, at=10_001))
        self.assertEqual(late.reason, domain.AUCTION_CLOSED)
        self.assertEqual(self.repo.get_state("a1").winner, "ana")

    def test_conflito_de_versao_releitura_e_nova_decisao(self):
        """Outro escritor grava um lance maior entre a leitura e o commit: o nosso é reavaliado."""
        repo = self.repo
        outro = BidProcessor(repo)
        original_commit = repo.commit
        injetado = {"feito": False}

        def commit_com_corrida(auction_id, expected_version, new_state, result):
            if not injetado["feito"]:
                injetado["feito"] = True
                outro.process(msg("b-outro", "bia", 300))  # corrida: muda a versão antes do nosso commit
            return original_commit(auction_id, expected_version, new_state, result)

        repo.commit = commit_com_corrida
        res = self.proc.process(msg("b1", "ana", 200))
        self.assertEqual(res.outcome, domain.REJECTED)
        self.assertEqual(res.reason, domain.NOT_HIGHER)
        self.assertEqual(repo.get_state("a1").current_amount_cents, 300)

    def test_callback_so_roda_para_operacoes_novas(self):
        vistos = []
        proc = BidProcessor(self.repo, on_result=vistos.append)
        proc.process(msg("b1", "ana", 200))
        proc.process(msg("b1", "ana", 200))
        self.assertEqual(len(vistos), 1)


class IngestTest(unittest.TestCase):
    def test_carimba_horario_do_servidor_e_usa_grupo_e_dedup(self):
        q = InMemoryFifoQueue()
        ing = Ingestor(q, clock_ms=lambda: 777)
        r = ing.submit_bid("a1", "ana", 500, "k1")
        self.assertEqual(r.status, ENQUEUED)
        grupo, body = q.log[0]
        self.assertEqual(grupo, "a1")
        self.assertEqual(body["received_at_ms"], 777)
        ing.submit_bid("a1", "ana", 500, "k1")  # retry do cliente: deduplicado pela fila
        self.assertEqual(len(q.log), 1)

    def test_validacao_e_rejeicao_rapida_por_cache(self):
        q = InMemoryFifoQueue()
        cache = InMemoryPriceCache()
        ing = Ingestor(q, price_cache=cache)
        self.assertEqual(ing.submit_bid("a1", "ana", -5, "k1").status, INVALID)
        self.assertEqual(ing.submit_bid("a1", "ana", 10.5, "k2").status, INVALID)
        self.assertEqual(ing.submit_bid("a1", "", 100, "k3").status, INVALID)
        cache.set("a1", 1000)
        r = ing.submit_bid("a1", "ana", 900, "k4")
        self.assertEqual(r.status, REJECTED_FAST)
        self.assertEqual(len(q.log), 0)
        self.assertEqual(ing.submit_bid("a1", "ana", 1100, "k5").status, ENQUEUED)


if __name__ == "__main__":
    unittest.main()
