package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
		log.Printf("consumer couldnt connect to kafka: %v", err)
		return
	}
	log.Println("consumer has been connected to kafka")

	for {
		fetches := client.PollFetches(ctx)

		if ctx.Err() != nil {
			break
		}

		for _, fetchErr := range fetches.Errors() {
			log.Printf(
				"failed to fetch: topic=%s partition=%d: %v ",
				fetchErr.Topic,
				fetchErr.Partition,
				fetchErr.Err,
			)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			log.Printf(
				"received record: topic=%s partition=%d offset=%d key=%s value=%s",
				record.Topic,
				record.Partition,
				record.Offset,
				record.Key,
				record.Value,
			)
		})
	}

	log.Println("consumer stopped!")
}
