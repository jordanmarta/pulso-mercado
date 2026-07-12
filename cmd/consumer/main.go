package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	appkafka "pulso-mercado/internal/kafka"
	"pulso-mercado/internal/market"
)

const (
	consumerGroupID = "pulso-quotes-group-v1"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

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
	fmt.Println("modo: fetch -> process -> commit")
	fmt.Println("aguardando mensagens...")
	fmt.Println("time     | instance   | stage     | offset | partition | key   | symbol")
	fmt.Println("-----------------------------------------------------------------------")

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nconsumer finalizado")
				return
			}

			log.Fatalf("erro ao buscar mensagem do Kafka: %v", err)
		}

		var event market.QuoteEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Fatalf("erro ao desserializar evento: %v", err)
		}

		printStage(instanceName, "fetched", message, event)

		if err := processQuoteEvent(ctx, event); err != nil {
			log.Printf("erro ao processar evento symbol=%s offset=%d partition=%d: %v",
				event.Symbol,
				message.Offset,
				message.Partition,
				err,
			)

			continue
		}

		printStage(instanceName, "processed", message, event)

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Fatalf("erro ao commitar offset: %v", err)
		}

		printStage(instanceName, "committed", message, event)
	}
}

func processQuoteEvent(ctx context.Context, event market.QuoteEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
		return nil
	}
}

func printStage(instanceName string, stage string, message kafkago.Message, event market.QuoteEvent) {
	fmt.Printf(
		"%s | %-10s | %-9s | %6d | %9d | %-5s | %-6s\n",
		time.Now().Format("15:04:05"),
		instanceName,
		stage,
		message.Offset,
		message.Partition,
		string(message.Key),
		event.Symbol,
	)
}
