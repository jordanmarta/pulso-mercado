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

const maxMessages = 5

func main() {
	ctx := context.Background()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{appkafka.BrokerAddress},
		Topic:     appkafka.TopicMarketQuotes,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})

	defer reader.Close()

	reader.SetOffset(kafkago.FirstOffset)

	fmt.Println("mensagens consumidas:")
	fmt.Println("offset | partition | symbol | price | volume")
	fmt.Println("--------------------------------------------")

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
			"%6d | %9d | %-6s | %.2f | %d\n",
			message.Offset,
			message.Partition,
			event.Symbol,
			event.Price,
			event.Volume,
		)
	}
}
