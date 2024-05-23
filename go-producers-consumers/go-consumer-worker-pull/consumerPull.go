package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	buffChannels := CreateWorkerPulls()

	/*i := 0
	for {
		buffChannels[0] <- string(fmt.Sprintf("hello in 1st channel message no: %d", i+1))
		buffChannels[1] <- string(fmt.Sprintf("hello in 2nd channel message no: %d", i+1))
		time.Sleep(5 * time.Second)

	}*/
	kafkaReaderWaitGroup := sync.WaitGroup{}
	topic := "Test"
	partition := 0
	conn, _ := connect(topic, partition)

	//writeMessages(conn, []string{"msg 1", "msg 22", "msg 333"})
	//readMessages(conn, 10, 10e3)
	readWithReader(topic, "test-consumer-group", buffChannels, kafkaReaderWaitGroup)
	/*
		consumerThreads := 3
		for j := 0; j < consumerThreads; j++ {
			kafkaReaderWaitGroup.Add(1)
			go readWithReader(topic, "test-consumer-group", buffChannels, kafkaReaderWaitGroup)
		}*/

	if err := conn.Close(); err != nil {
		fmt.Println("failed to close connection:", err)
	}

}
func GetMessageFromChannel(channel chan string, stopChan chan bool, wg *sync.WaitGroup, channelNo int, threadno int) {
	fmt.Printf("Initiated Consumer ThreadNo:%d for channel Bts %d \n", threadno, channelNo)
	defer wg.Done()
	for {
		select {
		case msg := <-channel:
			//to imitate processing time
			time.Sleep(5 * time.Millisecond)
			fmt.Println(msg)

		case stp := <-stopChan:
			fmt.Printf("got stop signal %v\n", stp)
			if len(channel) != 0 {
				fmt.Println("Data in channel", <-channel)
			}
			return
		}
	}
}

func CreateWorkerPulls() []chan string {
	var maxChannels, maxChannelSize, maxThreadsPerChan int
	consumeMessageWaitGroup := sync.WaitGroup{}
	var btsBuffchannels []chan string
	maxChannels = 2
	maxChannelSize = 10
	//create channels
	for i := 0; i < maxChannels; i++ {
		btsBuffchannels = append(btsBuffchannels, make(chan string, maxChannelSize))
	}

	var stopperc = make(chan bool)
	//create the object

	//create threads per channel

	maxThreadsPerChan = 5
	for i := 0; i < maxThreadsPerChan; i++ {
		consumeMessageWaitGroup.Add(maxThreadsPerChan)
		for j := 0; j < maxChannels; j++ {
			go GetMessageFromChannel(btsBuffchannels[j], stopperc, &consumeMessageWaitGroup, j, i)
		}
	}
	//consumeMessageWaitGroup.Wait()
	return btsBuffchannels
}

// Connect to the specified topic and partition in the server
func connect(topic string, partition int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp",
		"localhost:9092", topic, partition)
	if err != nil {
		fmt.Println("failed to dial leader")
	}
	return conn, err
} //end connect

func readWithReader(topic string, groupID string, buffChannels []chan string, wg sync.WaitGroup) {
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

		buffChannels[msg.Partition] <- string(msg.Value)

		//fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n",
		//	msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	if err := r.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}
}
