"""Gera carga de lances concorrentes e confere as garantias ao final.

Uso: python scripts/simulate.py --bids 5000 --producers 16
(o worker precisa estar rodando: `make worker` ou `docker compose up`)
"""
import argparse
import random
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from leilao import aws  # noqa: E402
from leilao.ingest import ENQUEUED, REJECTED_FAST, Ingestor  # noqa: E402
from leilao.queue_sqs import SqsQueue  # noqa: E402
from leilao.repo_dynamodb import DynamoRepository  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--auction", default="leilao-1")
    p.add_argument("--bids", type=int, default=2000)
    p.add_argument("--producers", type=int, default=16)
    p.add_argument("--users", type=int, default=200)
    p.add_argument("--timeout", type=int, default=120)
    args = p.parse_args()

    sqs = aws.client("sqs")
    url = aws.queue_url(sqs)
    repo = DynamoRepository(aws.client("dynamodb"), aws.TABLE_NAME)
    if repo.get_state(args.auction) is None:
        sys.exit(f"leilão {args.auction} não existe; rode scripts/setup_local.py")

    contagem = {ENQUEUED: 0, REJECTED_FAST: 0, "outros": 0}
    trava = threading.Lock()
    por_produtor = args.bids // args.producers

    def produtor(n):
        rng = random.Random(n)
        ing = Ingestor(SqsQueue(aws.client("sqs"), url))  # cliente boto3 próprio por thread
        for _ in range(por_produtor):
            chave = str(uuid.uuid4())
            status = ing.submit_bid(args.auction, f"user-{rng.randrange(args.users)}",
                                    rng.randrange(10_000, 500_000, 50), chave).status
            if rng.random() < 0.2:
                ing.submit_bid(args.auction, "retry", 999_999_999, chave)  # retry com a mesma chave
            with trava:
                contagem[status if status in contagem else "outros"] += 1

    inicio = time.time()
    threads = [threading.Thread(target=produtor, args=(n,)) for n in range(args.producers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"enviados em {time.time() - inicio:.1f}s: {contagem}")

    limite = time.time() + args.timeout
    while time.time() < limite:
        a = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        if int(a["ApproximateNumberOfMessages"]) + int(a["ApproximateNumberOfMessagesNotVisible"]) == 0:
            break
        time.sleep(1)
    else:
        print("aviso: a fila não esvaziou no tempo limite (o worker está rodando?)")

    estado = repo.get_state(args.auction)
    aceitos = repo.accepted_bids(args.auction)
    valores = [r.amount_cents for r in aceitos]
    crescente = all(b - a >= estado.min_increment_cents for a, b in zip(valores, valores[1:]))
    contiguas = [r.version for r in aceitos] == list(range(1, len(aceitos) + 1))
    print(f"lances aceitos: {len(aceitos)} | preço atual: {estado.current_amount_cents} "
          f"| líder: {estado.current_bidder} | versão: {estado.version}")
    print(f"preços estritamente crescentes (com incremento mínimo): {crescente}")
    print(f"versões contíguas: {contiguas}")
    print(f"preço atual == último lance aceito: {not aceitos or estado.current_amount_cents == valores[-1]}")


if __name__ == "__main__":
    main()
