import unittest

from leilao import domain
from leilao.domain import Bid, CloseEvent, new_auction


def bid(op, user, amount, at=1000, auction="a1"):
    return Bid(op, auction, user, amount, at)


class DomainRulesTest(unittest.TestCase):
    def setUp(self):
        self.state = new_auction("a1", start_price_cents=100, min_increment_cents=10, ends_at_ms=10_000)

    def test_primeiro_lance_precisa_atingir_preco_inicial(self):
        new, res = domain.decide_bid(self.state, bid("b1", "ana", 99))
        self.assertIsNone(new)
        self.assertEqual(res.reason, domain.BELOW_START_PRICE)
        new, res = domain.decide_bid(self.state, bid("b2", "ana", 100))
        self.assertEqual(res.outcome, domain.ACCEPTED)
        self.assertEqual(new.version, 1)

    def test_lance_igual_ou_menor_e_rejeitado(self):
        s, _ = domain.decide_bid(self.state, bid("b1", "ana", 200))
        for amount in (200, 150):
            new, res = domain.decide_bid(s, bid(f"x{amount}", "bia", amount))
            self.assertIsNone(new)
            self.assertEqual(res.reason, domain.NOT_HIGHER)

    def test_incremento_minimo(self):
        s, _ = domain.decide_bid(self.state, bid("b1", "ana", 200))
        _, res = domain.decide_bid(s, bid("b2", "bia", 205))
        self.assertEqual(res.reason, domain.BELOW_MIN_INCREMENT)
        new, res = domain.decide_bid(s, bid("b3", "bia", 210))
        self.assertEqual(res.outcome, domain.ACCEPTED)
        self.assertEqual(new.current_bidder, "bia")

    def test_usuario_nao_cobre_o_proprio_lance(self):
        s, _ = domain.decide_bid(self.state, bid("b1", "ana", 200))
        new, res = domain.decide_bid(s, bid("b2", "ana", 300))
        self.assertIsNone(new)
        self.assertEqual(res.reason, domain.ALREADY_LEADING)

    def test_lance_apos_o_horario_e_rejeitado(self):
        new, res = domain.decide_bid(self.state, bid("b1", "ana", 500, at=10_000))
        self.assertIsNone(new)
        self.assertEqual(res.reason, domain.AUCTION_ENDED)

    def test_valor_invalido(self):
        _, res = domain.decide_bid(self.state, bid("b1", "ana", 0))
        self.assertEqual(res.reason, domain.INVALID_BID)

    def test_anti_sniping_estende_o_prazo(self):
        s = new_auction("a1", 100, 10, ends_at_ms=10_000, snipe_window_ms=5_000, snipe_extension_ms=8_000)
        # lance fora da janela final: prazo não muda
        n1, _ = domain.decide_bid(s, bid("b1", "ana", 100, at=1_000))
        self.assertEqual(n1.ends_at_ms, 10_000)
        # lance dentro da janela final: prazo vai para received_at + extensão
        n2, res = domain.decide_bid(n1, bid("b2", "bia", 200, at=9_000))
        self.assertEqual(n2.ends_at_ms, 17_000)
        self.assertEqual(res.ends_at_ms, 17_000)

    def test_fechamento(self):
        s, _ = domain.decide_bid(self.state, bid("b1", "ana", 200))
        early, res = domain.decide_close(s, CloseEvent("c1", "a1", 9_999))
        self.assertIsNone(early)
        self.assertEqual(res.reason, domain.TOO_EARLY)
        closed, res = domain.decide_close(s, CloseEvent("c2", "a1", 10_000))
        self.assertEqual(closed.status, domain.CLOSED)
        self.assertEqual(closed.winner, "ana")
        again, res = domain.decide_close(closed, CloseEvent("c3", "a1", 20_000))
        self.assertIsNone(again)
        self.assertEqual(res.reason, domain.ALREADY_CLOSED)

    def test_lance_em_leilao_fechado(self):
        s, _ = domain.decide_bid(self.state, bid("b1", "ana", 200))
        closed, _ = domain.decide_close(s, CloseEvent("c1", "a1", 10_000))
        _, res = domain.decide_bid(closed, bid("b2", "bia", 900, at=10_001))
        self.assertEqual(res.reason, domain.AUCTION_CLOSED)

    def test_resultado_serializa_ida_e_volta(self):
        _, res = domain.decide_bid(self.state, bid("b1", "ana", 200))
        self.assertEqual(domain.Result.from_json(res.to_json()), res)


if __name__ == "__main__":
    unittest.main()
