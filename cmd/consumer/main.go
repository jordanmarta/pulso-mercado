package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	appkafka "pulso-mercado/internal/kafka"
	"pulso-mercado/internal/market"
)

const (
	consumerGroupID     = "pulso-quotes-group-v1"
	processedQuotesFile = "data/processed-quotes.log"
)

func main() {
	instanceName := flag.String("instance", "consumer-1", "nome da instância do consumer")
	failSymbol := flag.String("fail-symbol", "", "symbol que deve falhar durante o processamento")
	crashAfterRecordSymbol := flag.String("crash-after-record-symbol", "", "symbol que deve simular crash depois do registro e antes do commit")

	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{appkafka.BrokerAddress},
		Topic:       appkafka.TopicMarketQuotesPartitioned,
		GroupID:     consumerGroupID,
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	defer reader.Close()

	fmt.Printf("instance: %s\n", *instanceName)
	fmt.Printf("consumer group: %s\n", consumerGroupID)
	fmt.Printf("topic: %s\n", appkafka.TopicMarketQuotesPartitioned)
	fmt.Println("modo: fetch -> idempotency check -> process -> record -> commit")
	fmt.Printf("fail-symbol: %q\n", *failSymbol)
	fmt.Printf("crash-after-record-symbol: %q\n", *crashAfterRecordSymbol)
	fmt.Println("aguardando mensagens...")
	fmt.Println("time     | instance   | stage            | offset | partition | key   | symbol")
	fmt.Println("-------------------------------------------------------------------------------")

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

		printStage(*instanceName, "fetched", message, event)

		alreadyRecorded, err := wasQuoteAlreadyRecorded(message)
		if err != nil {
			log.Fatalf("erro ao verificar idempotência: %v", err)
		}

		if alreadyRecorded {
			printStage(*instanceName, "already-recorded", message, event)

			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Fatalf("erro ao commitar offset: %v", err)
			}

			printStage(*instanceName, "committed", message, event)
			continue
		}

		if err := processQuoteEvent(ctx, event, *failSymbol); err != nil {
			log.Printf(
				"falha durante processamento: symbol=%s offset=%d partition=%d erro=%v",
				event.Symbol,
				message.Offset,
				message.Partition,
				err,
			)

			log.Println("encerrando sem commitar esta mensagem")
			return
		}

		printStage(*instanceName, "processed", message, event)

		if err := recordProcessedQuote(message, event); err != nil {
			log.Printf(
				"falha ao registrar cotação processada: symbol=%s offset=%d partition=%d erro=%v",
				event.Symbol,
				message.Offset,
				message.Partition,
				err,
			)

			log.Println("encerrando sem commitar esta mensagem")
			return
		}

		printStage(*instanceName, "recorded", message, event)

		if *crashAfterRecordSymbol != "" && event.Symbol == *crashAfterRecordSymbol {
			log.Printf(
				"simulando crash depois do registro e antes do commit: symbol=%s offset=%d partition=%d",
				event.Symbol,
				message.Offset,
				message.Partition,
			)

			os.Exit(1)
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Fatalf("erro ao commitar offset: %v", err)
		}

		printStage(*instanceName, "committed", message, event)
	}
}

func processQuoteEvent(ctx context.Context, event market.QuoteEvent, failSymbol string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}

	if failSymbol != "" && event.Symbol == failSymbol {
		return errors.New("falha simulada durante processamento")
	}

	return nil
}

func recordProcessedQuote(message kafkago.Message, event market.QuoteEvent) error {
	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(processedQuotesFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	defer file.Close()

	line := fmt.Sprintf(
		"event_id=%s topic=%s partition=%d offset=%d key=%s symbol=%s price=%.2f volume=%d at=%s\n",
		quoteProcessingID(message),
		message.Topic,
		message.Partition,
		message.Offset,
		string(message.Key),
		event.Symbol,
		event.Price,
		event.Volume,
		time.Now().Format(time.RFC3339),
	)

	_, err = file.WriteString(line)
	return err
}

func wasQuoteAlreadyRecorded(message kafkago.Message) (bool, error) {
	file, err := os.Open(processedQuotesFile)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	defer file.Close()

	expectedPrefix := "event_id=" + quoteProcessingID(message)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, expectedPrefix) {
			return true, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return false, err
	}

	return false, nil
}

func quoteProcessingID(message kafkago.Message) string {
	return fmt.Sprintf("%s:%d:%d", message.Topic, message.Partition, message.Offset)
}

func printStage(instanceName string, stage string, message kafkago.Message, event market.QuoteEvent) {
	fmt.Printf(
		"%s | %-10s | %-16s | %6d | %9d | %-5s | %-6s\n",
		time.Now().Format("15:04:05"),
		instanceName,
		stage,
		message.Offset,
		message.Partition,
		string(message.Key),
		event.Symbol,
	)
}
