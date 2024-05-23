package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "Test"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		//Dialer:   dialer,
		Balancer: &kafka.LeastBytes{},
	})
	ctx := context.TODO()

	fmt.Println("HEllo")
	for i := 0; i < 100; i++ {
		message := fmt.Sprintf("Message %d", i)
		err := writer.WriteMessages(ctx,
			kafka.Message{Value: []byte(message), Topic: topic},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}
	if err := writer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

}
