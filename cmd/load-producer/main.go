package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	appkafka "pulso-mercado/internal/kafka"
	"pulso-mercado/internal/market"
)

var allSymbols = []string{
	"PETR4", "VALE3", "ITUB4", "BBAS3", "BBDC4",
	"ABEV3", "WEGE3", "MGLU3", "RENT3", "PRIO3",
	"B3SA3", "ELET3", "SUZB3", "LREN3", "RADL3",
	"VIVT3", "EQTL3", "RAIL3", "GGBR4", "CSNA3",
	"HAPV3", "EMBR3", "KLBN4", "BBSE3", "CMIG4",
}

func main() {
	count := flag.Int("count", 1000, "quantidade de eventos a publicar")
	symbolsCount := flag.Int("symbols", 25, "quantidade de ativos distintos a usar")
	batchSize := flag.Int("batch-size", 100, "quantidade de mensagens por lote")

	flag.Parse()

	if *count <= 0 {
		log.Fatal("count precisa ser maior que zero")
	}

	if *symbolsCount <= 0 {
		log.Fatal("symbols precisa ser maior que zero")
	}

	if *symbolsCount > len(allSymbols) {
		*symbolsCount = len(allSymbols)
	}

	if *batchSize <= 0 {
		log.Fatal("batch-size precisa ser maior que zero")
	}

	symbols := allSymbols[:*symbolsCount]

	ctx := context.Background()

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(appkafka.BrokerAddress),
		Topic:    appkafka.TopicMarketQuotesPartitioned,
		Balancer: &kafkago.Hash{},
	}

	defer writer.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	start := time.Now()

	fmt.Printf("publicando %d eventos no tópico %s\n", *count, appkafka.TopicMarketQuotesPartitioned)
	fmt.Printf("ativos distintos: %d\n", len(symbols))
	fmt.Printf("batch-size: %d\n", *batchSize)

	published := 0

	for published < *count {
		remaining := *count - published
		currentBatchSize := *batchSize

		if remaining < currentBatchSize {
			currentBatchSize = remaining
		}

		messages := make([]kafkago.Message, 0, currentBatchSize)

		for i := 0; i < currentBatchSize; i++ {
			event := newRandomQuoteEvent(r, symbols)

			payload, err := json.Marshal(event)
			if err != nil {
				log.Fatalf("erro ao serializar evento: %v", err)
			}

			messages = append(messages, kafkago.Message{
				Key:   []byte(event.Symbol),
				Value: payload,
			})
		}

		if err := writer.WriteMessages(ctx, messages...); err != nil {
			log.Fatalf("erro ao publicar lote no Kafka: %v", err)
		}

		published += currentBatchSize

		fmt.Printf("publicadas=%d/%d\n", published, *count)
	}

	fmt.Printf("finalizado em %s\n", time.Since(start).Round(time.Millisecond))
}

func newRandomQuoteEvent(r *rand.Rand, symbols []string) market.QuoteEvent {
	symbol := symbols[r.Intn(len(symbols))]
	price := 10 + r.Float64()*90
	volume := int64(100 + r.Intn(10000))

	return market.NewQuoteEvent(symbol, price, volume)
}
