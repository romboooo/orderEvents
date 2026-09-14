package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"orderEvents/internal/event"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func processRecord(record *kgo.Record) error {
	var orderEvent event.OrderCreated

	if err := json.Unmarshal(record.Value, &orderEvent); err != nil {
		return fmt.Errorf("unmarshal order event: %w", err)
	}

	log.Printf(
		"order received: id=%s user=%s amount=%d created_at=%s partition=%d offset=%d",
		orderEvent.OrderID,
		orderEvent.UserID,
		orderEvent.Amount,
		orderEvent.CreatedAt,
		record.Partition,
		record.Offset,
	)

	return nil
}

func main() {

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop()
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics("orders.created"),
		kgo.ConsumerGroup("order-notifications"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		log.Fatalf("failed to create kafka consumer: %v", err)
	}
	defer client.Close()

	pingCtx, cancelPing := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	pingErr := client.Ping(pingCtx)

	cancelPing()
	if pingErr != nil {
		log.Printf("consumer couldnt connect to kafka: %v", pingErr)
		return
	}
	log.Println("consumer has been connected to kafka")

	for {
		fetches := client.PollRecords(ctx, 1)

		if ctx.Err() != nil {
			break
		}

		for _, fetchErr := range fetches.Errors() {
			log.Printf(
				"failed to fetch: topic=%s partition=%d: %v",
				fetchErr.Topic,
				fetchErr.Partition,
				fetchErr.Err,
			)
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		record := records[0]

		if err := processRecord(record); err != nil {
			log.Printf(
				"failed to process record: partition=%d offset=%d: %v",
				record.Partition,
				record.Offset,
				err,
			)
			break
		}

		commitCtx, cancelCommit := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		err := client.CommitRecords(commitCtx, record)
		cancelCommit()

		if err != nil {
			log.Printf(
				"failed to commit record: partition=%d offset=%d: %v",
				record.Partition,
				record.Offset,
				err,
			)
			break
		}

		log.Printf(
			"record committed: partition=%d offset=%d",
			record.Partition,
			record.Offset,
		)
	}

	log.Println("consumer stopped!")
}
