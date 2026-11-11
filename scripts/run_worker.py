"""Consumidor local da fila FIFO (equivalente ao Lambda processador)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from leilao import aws  # noqa: E402
from leilao.processor import BidProcessor  # noqa: E402
from leilao.repo_dynamodb import DynamoRepository  # noqa: E402
from leilao.worker import run_worker  # noqa: E402

if __name__ == "__main__":
    sqs = aws.client("sqs")
    processor = BidProcessor(DynamoRepository(aws.client("dynamodb"), aws.TABLE_NAME))
    print("worker iniciado; Ctrl+C para parar")
    run_worker(sqs, aws.queue_url(sqs), processor)
