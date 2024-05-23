package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	buffChannels := CreateWorkerPulls()
	i := 0
	for {
		buffChannels[0] <- string(fmt.Sprintf("hello in 1st channel message no: %d", i+1))
		buffChannels[1] <- string(fmt.Sprintf("hello in 2nd channel message no: %d", i+1))
		time.Sleep(5 * time.Second)

	}

}
func GetMessageFromChannel(channel chan string, stopChan chan bool, wg *sync.WaitGroup, channelNo int, threadno int) {
	fmt.Printf("Initiated Consumer ThreadNo:%d for channel Bts %d \n", threadno, channelNo)
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
	return btsBuffchannels
}
