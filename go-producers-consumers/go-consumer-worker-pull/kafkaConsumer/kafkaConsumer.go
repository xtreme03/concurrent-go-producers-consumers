package kafkaconsumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

func ReadWithReader(topic string, groupID string, messageCHannel chan<- kafka.Message, wg *sync.WaitGroup) {
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

		//check the partition no and put it to the respective channel
		messageCHannel <- msg
		wg.Add(1)
		//buffChannels[msg.Partition] <- string(msg.Value)

		//fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
		//	msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	if err := r.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}
}

func Connect(topic string, partition int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		"localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader")
	}
	return conn, err
} //end connect
