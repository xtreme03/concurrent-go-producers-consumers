package main

/*
import (
	"fmt"
	"sync"

	kafkaconsumer "go-consumer-worker-pull/kafkaConsumer"

	"github.com/segmentio/kafka-go"
)

func main() {
	buffChannels := CreateWorkerPulls()
	var kafkaChannels chan kafka.Message

	kafkaReaderWaitGroup := sync.WaitGroup{}
	topic := "Test"
	partition := 0

	conn, _ := kafkaconsumer.Connect(topic, partition)
	defer conn.Close()
	//writeMessages(conn, []string{"msg 1", "msg 22", "msg 333"})
	//readMessages(conn, 10, 10e3)

	consumerThreads := 3

	for j := 0; j < consumerThreads; j++ {

		go kafkaconsumer.ReadWithReader(topic, "test-consumer-group", kafkaChannels, &kafkaReaderWaitGroup)
	}
	fmt.Println("In main thread")
	for {
		val := <-kafkaChannels
		buffChannels[val.Partition] <- string(val.Value)
		kafkaReaderWaitGroup.Done()
		break

	}

}
func GetMessageFromChannel(channel chan string, stopChan chan bool, wg *sync.WaitGroup, channelNo int, threadno int) {
	fmt.Printf("Initiated Consumer ThreadNo:%d for channel no %d \n", threadno, channelNo)
	defer wg.Done()
	for {
		select {
		case msg := <-channel:
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
*/
