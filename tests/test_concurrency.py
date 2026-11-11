"""Teste de concorrência: milhares de lances simultâneos, com retries e duplicatas."""
import itertools
import random
import threading
import time
import unittest
from collections import defaultdict

from leilao import domain
from leilao.domain import new_auction
from leilao.ingest import Ingestor
from leilao.processor import BidProcessor
from leilao.queue_memory import InMemoryFifoQueue
from leilao.repo_memory import InMemoryRepository

LEILOES = ["a1", "a2", "a3"]
START, INC = 100, 10
FAR_FUTURE = 10**12


def build_repo():
    repo = InMemoryRepository()
    for a in LEILOES:
        repo.create_auction(new_auction(a, START, INC, ends_at_ms=FAR_FUTURE))
    return repo


def assert_invariantes(test, repo, auction_id):
    """Lances aceitos, na ordem das versões, formam uma cadeia válida de preços crescentes."""
    aceitos = repo.accepted_bids(auction_id)
    versoes = [r.version for r in aceitos]
    test.assertEqual(versoes, list(range(1, len(aceitos) + 1)), "versões devem ser contíguas")
    anterior = None
    for r in aceitos:
        if anterior is None:
            test.assertGreaterEqual(r.amount_cents, START)
        else:
            test.assertGreaterEqual(r.amount_cents, anterior.amount_cents + INC)
            test.assertNotEqual(r.user_id, anterior.user_id)
        anterior = r
    test.assertEqual(len({r.op_id for r in aceitos}), len(aceitos), "nenhum lance aceito duas vezes")
    state = repo.get_state(auction_id)
    if aceitos:
        test.assertEqual(state.current_amount_cents, aceitos[-1].amount_cents)
        test.assertEqual(state.current_bidder, aceitos[-1].user_id)
        test.assertEqual(state.version, len(aceitos))


class FilaOrdenadaComEscritorUnicoTest(unittest.TestCase):
    """Cenário da arquitetura: fila FIFO por leilão + um consumidor por grupo."""

    PRODUTORES, LANCES_POR_PRODUTOR = 16, 400

    def test_milhares_de_lances_simultaneos(self):
        repo = build_repo()
        queue = InMemoryFifoQueue()
        relogio = itertools.count(1)  # horário do servidor simulado (monotônico)
        ingestor = Ingestor(queue, clock_ms=lambda: next(relogio))
        enviados = defaultdict(set)
        trava = threading.Lock()

        def produtor(n):
            rng = random.Random(1000 + n)
            for i in range(self.LANCES_POR_PRODUTOR):
                leilao = rng.choice(LEILOES)
                op_id = f"p{n}-{i}"
                ingestor.submit_bid(leilao, f"u{rng.randrange(50)}", rng.randrange(100, 20_000, 5), op_id)
                with trava:
                    enviados[leilao].add(op_id)
                sorteio = rng.random()
                if sorteio < 0.15:    # retry do cliente com a mesma chave: a fila deduplica
                    ingestor.submit_bid(leilao, "retry", 99_999, op_id)
                elif sorteio < 0.30:  # duplicata que escapa da janela de dedup da fila: o processador deduplica
                    corpo = dict(queue.log[-1][1]) if queue.log else None
                    if corpo and corpo["bid_id"] == op_id:
                        queue.send(leilao, f"{op_id}-reentrega", corpo)

        processador = BidProcessor(repo)
        resultados = defaultdict(list)
        produtores_terminaram = threading.Event()

        def consumidor(leilao):
            while True:
                corpo = queue.pop(leilao)
                if corpo is None:
                    if produtores_terminaram.is_set() and queue.size() == 0:
                        return
                    time.sleep(0.0005)
                    continue
                resultados[leilao].append(processador.process(corpo))

        consumidores = [threading.Thread(target=consumidor, args=(a,)) for a in LEILOES]
        produtores = [threading.Thread(target=produtor, args=(n,)) for n in range(self.PRODUTORES)]
        for t in consumidores + produtores:
            t.start()
        for t in produtores:
            t.join()
        produtores_terminaram.set()
        for t in consumidores:
            t.join()

        total_aceitos = 0
        for a in LEILOES:
            assert_invariantes(self, repo, a)
            total_aceitos += len(repo.accepted_bids(a))
            novos = {r.op_id for r in resultados[a] if not r.duplicate}
            self.assertEqual(novos, enviados[a], "toda operação distinta foi processada exatamente uma vez")
        self.assertGreater(total_aceitos, 0)
        self.assertTrue(any(r.duplicate for rs in resultados.values() for r in rs),
                        "o teste deve exercitar duplicatas chegando ao processador")

        # Determinismo: reprocessar o log de chegada, na mesma ordem, reproduz o mesmo resultado
        repo2 = build_repo()
        proc2 = BidProcessor(repo2)
        for grupo, corpo in queue.log:
            proc2.process(corpo)
        for a in LEILOES:
            self.assertEqual(repo2.get_state(a), repo.get_state(a))
            self.assertEqual(repo2.accepted_bids(a), repo.accepted_bids(a))


class VariosEscritoresNoMesmoLeilaoTest(unittest.TestCase):
    """Segunda barreira: sem a fila, a escrita condicional por versão ainda impede lances inválidos."""

    def test_escritores_concorrentes_respeitam_as_invariantes(self):
        repo = build_repo()
        proc = BidProcessor(repo, max_attempts=10_000)  # sob contenção alta, relê quantas vezes precisar
        relogio = itertools.count(1)
        trava = threading.Lock()

        def escritor(n):
            rng = random.Random(n)
            for i in range(300):
                with trava:
                    agora = next(relogio)
                proc.process({"type": "BID", "bid_id": f"w{n}-{i}", "auction_id": "a1",
                              "user_id": f"u{rng.randrange(40)}", "amount_cents": rng.randrange(100, 20_000, 5),
                              "received_at_ms": agora})

        threads = [threading.Thread(target=escritor, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert_invariantes(self, repo, "a1")
        self.assertGreater(len(repo.accepted_bids("a1")), 0)

    def test_mesma_operacao_em_varias_threads_conta_uma_vez(self):
        repo = build_repo()
        proc = BidProcessor(repo, max_attempts=1000)
        resultados = []
        barreira = threading.Barrier(8)

        def tenta():
            barreira.wait()
            resultados.append(proc.process({"type": "BID", "bid_id": "mesmo", "auction_id": "a1",
                                            "user_id": "ana", "amount_cents": 500, "received_at_ms": 1}))

        threads = [threading.Thread(target=tenta) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(repo.accepted_bids("a1")), 1)
        self.assertEqual(sum(1 for r in resultados if not r.duplicate), 1)
        self.assertEqual(repo.get_state("a1").version, 1)


if __name__ == "__main__":
    unittest.main()
