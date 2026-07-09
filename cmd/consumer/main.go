package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	kafkago "github.com/segmentio/kafka-go"

	appkafka "pulso-mercado/internal/kafka"
	"pulso-mercado/internal/market"
)

const (
	partitionToRead = 1
	maxMessages     = 1
)

func main() {
	ctx := context.Background()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{appkafka.BrokerAddress},
		Topic:     appkafka.TopicMarketQuotesPartitioned,
		Partition: partitionToRead,
		MinBytes:  1,
		MaxBytes:  10e6,
	})

	defer reader.Close()

	reader.SetOffset(kafkago.LastOffset)

	fmt.Printf("lendo tópico %s / partition %d\n", appkafka.TopicMarketQuotesPartitioned, partitionToRead)
	fmt.Println("offset | partition | key   | symbol | price | volume")
	fmt.Println("-----------------------------------------------------")

	for i := 0; i < maxMessages; i++ {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("erro ao ler mensagem do Kafka: %v", err)
		}

		var event market.QuoteEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Fatalf("erro ao desserializar evento: %v", err)
		}

		fmt.Printf(
			"%6d | %9d | %-5s | %-6s | %.2f | %d\n",
			message.Offset,
			message.Partition,
			string(message.Key),
			event.Symbol,
			event.Price,
			event.Volume,
		)
	}
}
