package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "example-topic"
	partition := 0

	// Set up a Kafka writer to produce messages
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
	})

	// Produce a message
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello Kafka"),
		},
	)
	if err != nil {
		log.Fatal("Failed to write message:", err)
	}
	fmt.Println("Produced a message")

	// Set up a Kafka reader to consume messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		log.Fatal("Failed to read message:", err)
	}
	fmt.Printf("Consumed message: %s\n", string(msg.Value))

	writer.Close()
	reader.Close()
}
