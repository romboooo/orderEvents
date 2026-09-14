package main

import (
	"context"
	"encoding/json"
	"log"
	"orderEvents/internal/event"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)

	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}

	defer client.Close()

	if err := client.Ping(ctx); err != nil {
		log.Printf("failed to connect to kafka: %v", err)
		return
	}
	log.Println("connected to kafka")

	orderEvent := event.OrderCreated{
		OrderID:   "order-2",
		UserID:    "user-42",
		Amount:    250000,
		CreatedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(orderEvent)

	if err != nil {
		log.Printf("failed to marshal order event: %v", err)
		return
	}

	record := &kgo.Record{
		Topic: "orders.created",
		Key:   []byte(orderEvent.OrderID),
		Value: payload,
	}

	log.Printf("record created: topic=%s key=%s value=%s",
		record.Topic,
		record.Key,
		record.Value,
	)

	results := client.ProduceSync(ctx, record)

	if err := results.FirstErr(); err != nil {
		log.Printf("failed to produce oerder event: %v", err)
		return
	}

	log.Printf("event produced: topic=%s Partition=%d Offset=%d",
		record.Topic,
		record.Partition,
		record.Offset,
	)
}
