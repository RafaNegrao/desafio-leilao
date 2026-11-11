# Leilão online de alta concorrência (desafio técnico)

Núcleo de um sistema de leilão que recebe lances simultâneos nos últimos segundos,
mantém **ordem determinística** de chegada e **não aceita lances inválidos ou duplicados**.
Arquitetura-alvo na AWS: `docs/arquitetura.drawio`. Decisões e cenários: `docs/decisoes.md`.

## Ideia central

1. **Ingestão** (API Gateway + Lambda): valida, carimba o horário **do servidor** e envia o lance
   para uma fila **SQS FIFO** com `MessageGroupId = auctionId` e
   `MessageDeduplicationId = chave de idempotência`.
2. **Ordem por leilão:** o SQS FIFO entrega as mensagens de um mesmo grupo em ordem e para um consumidor
   por vez. Leilões diferentes processam em paralelo.
3. **Processador** (Lambda): lê o estado, aplica as regras e grava no **DynamoDB** com uma
   **transação condicional** (versão do estado + registro de idempotência). É a segunda barreira
   contra race condition: mesmo com reentrega ou dois processadores, só um commit vence.
4. **Fechamento:** o evento "fechar leilão" entra na **mesma fila e no mesmo grupo** dos lances,
   então não há corrida entre o último lance e o encerramento.
5. **Cache** (Redis): serve só para rejeitar cedo lances que já nasceram perdidos. Nunca decide nada.

## Como rodar

Requisitos: Python 3.12+ e Docker.

```bash
# 1) Testes unitários e de concorrência (não precisam de Docker nem de boto3)
make test

# 2) Ambiente AWS local completo
pip install -r requirements.txt
make up         # DynamoDB Local + ElasticMQ (API SQS), sem conta e sem token
make setup      # tabela, fila FIFO + DLQ e leilão de exemplo (leilao-1)
make worker     # em um terminal: consumidor da fila
make simulate   # em outro: 5.000 lances concorrentes (com retries) + conferência final

# 3) Testes de integração contra os emuladores
make integration
```

Tudo em containers (sem instalar Python): `docker compose up --build` (sobe emuladores, setup e worker)
e, em outro terminal, `docker compose run --rm simulate`.

> **Emulador local:** desde março de 2026 a imagem `localstack/localstack` exige conta e token.
> Por isso o compose usa DynamoDB Local + ElasticMQ. Para usar LocalStack (com token) ou Floci,
> defina `AWS_ENDPOINT_URL=http://localhost:4566` e ignore os serviços `dynamodb` e `sqs`.
> Confirme que a versão da imagem do ElasticMQ suporta filas FIFO; se não, use um dos dois acima.

## O que os testes garantem (`tests/`)

- `test_domain.py`: regras puras (preço inicial, incremento mínimo, empate, lance no próprio lance,
  prazo, anti-sniping, fechamento).
- `test_processor.py`: idempotência, mensagem malformada, leilão inexistente, conflito de versão com
  releitura e nova decisão, ingestão (horário do servidor, validação, rejeição rápida por cache).
- `test_concurrency.py`:
  - 16 produtores enviam 6.400 lances simultâneos para 3 leilões, com retries e duplicatas que escapam
    da deduplicação da fila. Verifica: preços estritamente crescentes com incremento mínimo, versões
    contíguas, nenhum lance contado duas vezes, toda operação processada uma única vez.
  - **Determinismo:** reprocessar o log de chegada na mesma ordem reproduz exatamente o mesmo estado.
  - 8 escritores concorrentes no **mesmo** leilão, sem fila: a escrita condicional por versão mantém
    as invariantes. A mesma operação disparada por 8 threads ao mesmo tempo conta uma única vez.

## Estrutura

```
src/leilao/
  domain.py          regras puras e determinísticas (sem I/O, sem relógio)
  processor.py       escritor único: idempotência + commit condicional com releitura
  ingest.py          validação, horário do servidor, envio à fila, rejeição rápida via cache
  repo_dynamodb.py   tabela única: STATE / BID#<versão> / OP#<op_id>, transação condicional
  queue_sqs.py       envio ao SQS FIFO (grupo = leilão, dedup = chave de idempotência)
  worker.py          loop local de consumo e handler Lambda (batchItemFailures, preserva ordem)
  repo_memory.py, queue_memory.py, cache.py   versões em memória usadas nos testes
scripts/             setup_local, run_worker, simulate
docs/                arquitetura.drawio, decisoes.md
```

## Limites desta versão

- Os testes unitários e de concorrência usam repositório e fila em memória com a mesma semântica
  (transação condicional, FIFO por grupo, deduplicação). Os adaptadores AWS (`repo_dynamodb.py`,
  `queue_sqs.py`, `worker.py`) são exercitados por `make integration` e `make simulate`.
- Fora do escopo: IaC (CDK/Terraform), Redis real (o cache é interface + versão em memória),
  autenticação, rate limit por usuário (WAF), saldo/limite do usuário, notificação WebSocket e
  reagendamento do fechamento (o resultado `TOO_EARLY` traz o novo prazo para isso).
- Limites de throughput do SQS FIFO por fila e por grupo devem ser confirmados na documentação da AWS
  e dimensionados para o pico esperado (hot key: um leilão = um grupo).
