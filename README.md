# Pulso de Mercado

Lab prático em Go e Kafka para estudar streaming de eventos, particionamento, consumer groups, consumer lag, retry, DLQ, backpressure, hot partitions e controle de processamento por chave.

O projeto simula um fluxo de eventos de mercado, como cotações de ativos, com o objetivo de aprofundar conceitos de sistemas distribuídos e arquitetura orientada a eventos.

## Objetivo

Este projeto foi criado como um laboratório técnico incremental para estudar Kafka na prática.

A ideia não é construir um produto final bonito, mas sim um ambiente de experimentação para entender decisões arquiteturais relevantes em sistemas críticos, como:

* publicação e consumo de eventos;
* tópicos, partições e offsets;
* ordenação por chave;
* paralelismo com consumer groups;
* consumer lag;
* hot partitions;
* retry e DLQ;
* backpressure;
* rate limiting por entidade/chave;
* trade-offs entre ordenação, escala e resiliência.

## Contexto do domínio

O domínio escolhido é mercado financeiro, simulando eventos de cotação de ativos.

Exemplo de evento:

```json
{
  "symbol": "PETR4",
  "price": 34.82,
  "volume": 1200,
  "timestamp": "2026-07-04T22:08:24.066472316Z"
}
```

## Stack

* Go
* Kafka local via Docker Compose
* segmentio/kafka-go
* JSON como formato inicial de serialização

## Estrutura atual

```txt
pulso-mercado/
├── docker-compose.yml
├── go.mod
├── go.sum
├── .gitignore
├── cmd/
│   ├── producer/
│   │   └── main.go
│   └── consumer/
│       └── main.go
└── internal/
    ├── kafka/
    │   └── config.go
    └── market/
        └── event.go
```

## Componentes

### Kafka

O Kafka roda localmente via Docker Compose.

O broker fica disponível em:

```txt
localhost:9092
```

Container:

```txt
pulso-kafka
```

### Tópico inicial

Tópico usado na primeira fase:

```txt
market.quotes
```

Configuração inicial:

```txt
PartitionCount: 1
ReplicationFactor: 1
Partition: 0
```

Nesta fase inicial, o tópico possui apenas uma partição para simplificar o entendimento de producer, consumer, offset e serialização.

### Evento de domínio

Arquivo:

```txt
internal/market/event.go
```

Representa o contrato inicial do evento de cotação:

```go
type QuoteEvent struct {
    Symbol    string    `json:"symbol"`
    Price     float64   `json:"price"`
    Volume    int64     `json:"volume"`
    Timestamp time.Time `json:"timestamp"`
}
```

Também existe uma função de criação:

```go
func NewQuoteEvent(symbol string, price float64, volume int64) QuoteEvent
```

### Configuração Kafka

Arquivo:

```txt
internal/kafka/config.go
```

Centraliza configurações básicas:

```go
const (
    BrokerAddress     = "localhost:9092"
    TopicMarketQuotes = "market.quotes"
)
```

Essa separação evita espalhar nomes de tópicos e endereço do broker diretamente no código.

### Producer

Arquivo:

```txt
cmd/producer/main.go
```

Responsável por:

1. criar um evento `QuoteEvent`;
2. serializar o evento para JSON;
3. publicar a mensagem no tópico `market.quotes`.

Fluxo:

```txt
QuoteEvent
   ↓
json.Marshal
   ↓
[]byte
   ↓
Kafka topic market.quotes
```

Nesta fase, o producer ainda não envia `Key`. O particionamento por chave será estudado em uma etapa posterior.

### Consumer

Arquivo:

```txt
cmd/consumer/main.go
```

Responsável por:

1. conectar no Kafka;
2. ler da `partition 0`;
3. iniciar leitura no primeiro offset;
4. consumir uma mensagem;
5. desserializar o JSON para `QuoteEvent`;
6. imprimir os dados do evento.

Nesta fase, o consumer ainda não usa consumer group. Ele funciona como um leitor direto da partição para facilitar o entendimento do log interno do Kafka.

## Como executar

### 1. Subir Kafka

```bash
docker compose up -d
```

### 2. Criar tópico

```bash
docker exec -it pulso-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic market.quotes \
  --partitions 1 \
  --replication-factor 1
```

### 3. Validar tópico

```bash
docker exec -it pulso-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic market.quotes
```

### 4. Publicar evento

```bash
go run ./cmd/producer
```

### 5. Consumir evento

```bash
go run ./cmd/consumer
```

## Conceitos já explorados

* Kafka broker
* Topic
* Partition
* Offset
* Producer
* Consumer
* Serialização JSON
* Desserialização JSON
* Kafka como log append-only
* Diferença entre Kafka e filas tradicionais como SQS/RabbitMQ

## Observação importante

Kafka não remove a mensagem do tópico quando ela é consumida.

A mensagem permanece no log até atingir a política de retenção configurada. O que avança é a posição de leitura do consumidor ou do consumer group, controlada por offsets.

## Roadmap do lab

### Fase 1 — Kafka básico

Status: em andamento.

Objetivo:

* subir Kafka local;
* criar tópico;
* publicar evento;
* consumir evento;
* entender mensagem, offset e serialização.

Estado atual:

* Kafka local rodando;
* tópico `market.quotes` criado;
* producer Go publicando evento;
* consumer Go lendo e desserializando evento.

### Fase 2 — Partições e chave

Objetivo:

* criar tópico com múltiplas partições;
* publicar eventos usando `symbol` como key;
* observar distribuição entre partições;
* entender ordenação por chave.

### Fase 3 — Alta volumetria

Objetivo:

* gerar carga;
* medir eventos publicados/consumidos;
* observar throughput, latência e gargalos.

### Fase 4 — Consumer groups

Objetivo:

* rodar múltiplos consumers;
* entender rebalance;
* observar relação entre número de consumers e partições.

### Fase 5 — Hot partition

Objetivo:

* simular concentração de eventos em uma única chave;
* observar skew de carga;
* discutir trade-offs entre ordenação e distribuição.

### Fase 6 — Processamento e agregação

Objetivo:

* gerar candles simples de 1 minuto;
* calcular abertura, fechamento, mínimo, máximo e volume.

### Fase 7 — Retry e DLQ

Objetivo:

* tratar mensagens inválidas;
* separar erro transitório de erro definitivo;
* implementar retry topic e DLQ.

### Fase 8 — Backpressure

Objetivo:

* simular consumer lento;
* observar crescimento de lag;
* discutir alternativas de escala e controle.

### Fase 9 — Rate limit por chave

Objetivo:

* limitar processamento por entidade, como `symbol`, cliente ou tenant;
* estudar token bucket;
* avaliar uso de Redis;
* discutir trade-offs em fluxo assíncrono.

## Evolução do projeto

### Checkpoint 1 — Baseline Kafka

Implementado:

* Docker Compose com Kafka local;
* tópico `market.quotes`;
* modelo `QuoteEvent`;
* producer básico;
* consumer básico;
* serialização e desserialização JSON.

Aprendizado principal:

```txt
Kafka não funciona como fila clássica.
Kafka mantém mensagens no log.
Consumers avançam por offset.
Consumer groups controlam posição de leitura.
```

## Próximo passo

Publicar múltiplos eventos com símbolos diferentes e observar offsets sequenciais dentro da partição.

Exemplo esperado:

```txt
offset 0 -> PETR4
offset 1 -> VALE3
offset 2 -> ITUB4
offset 3 -> BBAS3
```

Esse passo prepara a entrada para estudar partições, partition key e ordenação por ativo.
