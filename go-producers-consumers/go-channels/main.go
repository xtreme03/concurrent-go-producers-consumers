package main

import (
	"fmt"
	"net/http"
)

func main() {
	links := []string{
		"http://google.com",
		"http://facebook.com",
		"http://stackoverflow.com"}
	c := make(chan string)
	for _, link := range links {
		go checklink(link, c)
	}
	for i := 0; i < len(links); i++ {
		fmt.Println(<-c)
	}
}
func checklink(link string, c chan string) {
	_, err := http.Get(link)
	if err != nil {
		fmt.Println(link, "site is down")
		c <- "Might be down"
		return
	}
	fmt.Println(link, "up")
	c <- "Link is up"
}
