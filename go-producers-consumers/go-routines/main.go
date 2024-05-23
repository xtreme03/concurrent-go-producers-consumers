package main

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"
)

func main() {
	//set the max procs
	runtime.GOMAXPROCS(runtime.NumCPU())
	links := []string{
		"https://www.google.com",
		"https://www.youtube.com",
		"https://www.facebook.com",
		"https://www.amazon.com",
		"https://www.netflix.com",
		"https://www.wikipedia.org",
		"https://www.twitter.com",
		"https://www.instagram.com",
		"https://www.linkedin.com",
		"https://www.reddit.com",
		"https://in.bookmyshow.com/",
	}
	t1 := time.Now()
	wg := sync.WaitGroup{}
	for _, link := range links {
		//go checklink(link)
		wg.Add(1)
		go checklinkWaitGroups(link, &wg)
		//checklink(link)
	}
	wg.Wait()
	t2 := time.Now()
	diff := t2.Sub(t1)

	fmt.Println("Time Taken :", diff.Seconds())
}
func checklink(link string) {
	_, err := http.Get(link)
	time.Sleep(10 * time.Millisecond)
	if err != nil {
		fmt.Println(link, "site is down")
		return
	}
	fmt.Println(link, "up")
}

func checklinkWaitGroups(link string, wg *sync.WaitGroup) {
	_, err := http.Get(link)
	time.Sleep(10 * time.Millisecond)
	if err != nil {
		fmt.Println(link, "site is down")
		return
	}
	fmt.Println(link, "up")
	wg.Done()
}
