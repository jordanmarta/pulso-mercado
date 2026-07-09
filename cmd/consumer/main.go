package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	kafkago "github.com/segmentio/kafka-go"

	appkafka "pulso-mercado/internal/kafka"
	"pulso-mercado/internal/market"
)

const (
	consumerGroupID = "pulso-quotes-group-v1"
	maxMessages     = 8
)

func main() {
	ctx := context.Background()

	instanceName := "consumer-1"
	if len(os.Args) > 1 {
		instanceName = os.Args[1]
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{appkafka.BrokerAddress},
		Topic:       appkafka.TopicMarketQuotesPartitioned,
		GroupID:     consumerGroupID,
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	defer reader.Close()

	fmt.Printf("instance: %s\n", instanceName)
	fmt.Printf("consumer group: %s\n", consumerGroupID)
	fmt.Printf("topic: %s\n", appkafka.TopicMarketQuotesPartitioned)
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
