"""Fila FIFO em memória que imita o SQS FIFO: ordem por grupo e deduplicação."""
from __future__ import annotations

import threading
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Set, Tuple


class InMemoryFifoQueue:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seen: Set[str] = set()
        self._groups: Dict[str, Deque[dict]] = defaultdict(deque)
        self.log: List[Tuple[str, dict]] = []  # ordem de chegada de tudo que foi aceito pela fila

    def send(self, group_id: str, dedup_id: str, body: dict) -> bool:
        with self._lock:
            if dedup_id in self._seen:
                return False  # deduplicada (como na janela de deduplicação do SQS FIFO)
            self._seen.add(dedup_id)
            self._groups[group_id].append(body)
            self.log.append((group_id, body))
            return True

    def pop(self, group_id: str) -> Optional[dict]:
        with self._lock:
            dq = self._groups.get(group_id)
            return dq.popleft() if dq else None

    def size(self) -> int:
        with self._lock:
            return sum(len(d) for d in self._groups.values())
