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

	message, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatalf("erro ao ler mensagem do Kafka: %v", err)
	}

	var event market.QuoteEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		log.Fatalf("erro ao desserializar evento: %v", err)
	}

	fmt.Println("mensagem consumida")
	fmt.Printf("topic: %s\n", message.Topic)
	fmt.Printf("partition: %d\n", message.Partition)
	fmt.Printf("offset: %d\n", message.Offset)

	fmt.Println("evento:")
	fmt.Printf("symbol: %s\n", event.Symbol)
	fmt.Printf("price: %.2f\n", event.Price)
	fmt.Printf("volume: %d\n", event.Volume)
	fmt.Printf("timestamp: %s\n", event.Timestamp)
}
