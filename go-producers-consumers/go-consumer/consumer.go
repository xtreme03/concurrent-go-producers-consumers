package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "Test"
	partition := 0
	conn, _ := connect(topic, partition)

	//writeMessages(conn, []string{"msg 1", "msg 22", "msg 333"})
	//readMessages(conn, 10, 10e3)
	readWithReader(topic, "test-consumer-group")

	if err := conn.Close(); err != nil {
		fmt.Println("failed to close connection:", err)
	}
} //end main

// Connect to the specified topic and partition in the server
func connect(topic string, partition int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		"localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader")
	}
	return conn, err
} //end connect

func readWithReader(topic string, groupID string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  groupID,
		Topic:    topic,
		MaxBytes: 100, //per message
		// more options are available
	})

	//Create a deadline
	fmt.Println("COnsumer is up and running")
	//readDeadline, _ := context.WithDeadline(context.Background(),
	//	time.Now().Add(5*time.Second))
	ctx := context.Background()
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		time.Sleep(5 * time.Millisecond)
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	if err := r.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}
}
