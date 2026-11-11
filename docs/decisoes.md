# Decisões técnicas (resumo)

Versão completa, com requisitos e cenários, no Claude Doc "Desafio técnico: leilão online de alta concorrência".

## Premissas de carga (a validar)
200 mil usuários conectados; picos de dezenas de milhares de lances por segundo no mesmo item.

## ADR-1: SQS FIFO como log ordenado
- **Decisão:** SQS FIFO, `MessageGroupId = auctionId`.
- **Alternativas:** Kinesis Data Streams (ordem por shard, replay, escala por shard) e MSK/Kafka.
- **Motivo:** familiaridade e experiência prévia com a SQS; deduplicação e ordem por grupo nativas.
- **Consequências:** dedup só numa janela curta (cerca de 5 min), então a idempotência também é
  registrada no banco; mensagem que falha bloqueia seu grupo até ir para a DLQ (`maxReceiveCount` + alarme).

## ADR-2: Duas barreiras contra race condition
Serialização por leilão (fila) **e** escrita condicional por versão no DynamoDB. A fila dá a ordem;
a condição garante corretude mesmo com reentrega ou escritores concorrentes.

## ADR-3: Idempotência
Chave por lance (`bid_id`) vira `MessageDeduplicationId` e o item `OP#<id>` gravado na mesma transação.
Vale também para rejeições (retry devolve o mesmo resultado).

## ADR-4: Tempo
Só o horário do servidor, carimbado na ingestão, entra nas regras. O processador não lê relógio:
reprocessar o log gera o mesmo resultado (determinismo). Pequena defasagem de relógio entre
instâncias de ingestão é um risco aceito e deve ser mitigado com sincronização de tempo.

## ADR-5: Fechamento e anti-sniping
O evento de fechamento entra no mesmo grupo dos lances. Lance aceito na janela final estende o prazo;
um fechamento que chega cedo devolve `TOO_EARLY` com o novo prazo, para reagendamento.

## ADR-6: Cache só para rejeição antecipada
Reduz a carga sobre a fila no hot key, mas nunca é fonte de verdade.

## ADR-7: Tabela única no DynamoDB
`STATE`, `BID#<versão>` (trilha de auditoria ordenada) e `OP#<id>` (idempotência) na mesma partição,
o que permite uma transação única por operação.

## ADR-8: Emulador local sem conta
DynamoDB Local + ElasticMQ, porque a imagem do LocalStack passou a exigir token em março de 2026.
Endpoints configuráveis por variável de ambiente.

## ADR-9: Cold start e escolha de compute (proposta, a validar com teste de carga)
- **Contexto:** o tráfego sobe de quase zero para picos em segundos, e cada instância nova de Lambda
  passa por cold start ao mesmo tempo. A Lambda também tem um ritmo máximo de criação de ambientes.
- **Onde pesa:** Lambda de ingestão (alto: está no caminho do usuário) e rota `$connect` do WebSocket
  (médio/alto). Processador, notificador e fechador pesam pouco: são assíncronos, e a fila FIFO absorve
  o atraso sem afetar a ordem nem a corretude.
- **Proposta:** ingestão em Lambda com **Provisioned Concurrency agendada** antes de leilões previstos,
  inicialização enxuta (clientes criados fora do handler, imports sob demanda, pacote pequeno) e
  Python. Processador em Lambda, sem concorrência provisionada.
- **Alternativas:** ECS/Fargate atrás de um ALB (sem cold start por requisição, mas escala mais devagar
  e exige pré-escalar para o pico); API Gateway integrado direto à SQS, sem Lambda (sem cold start,
  mas perde validação rica e a consulta ao Redis); SnapStart (confirmar runtimes e custo).
- **Critério de revisão:** medir `Init Duration` e o p99 da ingestão em teste de carga. Se o cold
  start aparecer no p99 mesmo com concorrência provisionada, migrar a ingestão para ECS.
- **Status:** não medido nem implementado. Existe a classe `Ingestor` (`src/leilao/ingest.py`), mas
  não o handler Lambda. O `scripts/run_worker.py` já roda como consumidor de longa duração, o que
  permitiria levar o processador para ECS sem mudar o código.

## Cenários cobertos nos testes
Lance igual ou menor, empate, incremento mínimo, lance no próprio lance, lance após o horário, leilão
fechado/inexistente, retry e reentrega, escritores concorrentes, mensagem malformada, anti-sniping,
fechamento antecipado.

## Riscos conhecidos
Hot key (um grupo por leilão limita o throughput desse leilão), defasagem de relógio entre
instâncias de ingestão, mensagem que bloqueia o grupo até a DLQ, cold start da ingestão no pico.
