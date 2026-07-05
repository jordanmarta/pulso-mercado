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

	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(appkafka.BrokerAddress),
		Topic: appkafka.TopicMarketQuotes,
	}

	defer writer.Close()

	events := []market.QuoteEvent{
		market.NewQuoteEvent("PETR4", 34.82, 1200),
		market.NewQuoteEvent("VALE3", 58.40, 900),
		market.NewQuoteEvent("ITUB4", 33.10, 1500),
		market.NewQuoteEvent("BBAS3", 24.70, 800),
	}

	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			log.Fatalf("erro ao serializar evento: %v", err)
		}

		err = writer.WriteMessages(ctx, kafkago.Message{
			Value: payload,
		})

		if err != nil {
			log.Fatalf("erro ao publicar mensagem no Kafka: %v", err)
		}

		fmt.Printf("evento publicado no tópico %s: %s\n", appkafka.TopicMarketQuotes, string(payload))
	}
}
