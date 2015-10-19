package main

import (
	"flag"
	"log"
	"sync/atomic"
	"time"

	"github.com/kadirahq/kadiyadb-client"
)

var (
	addr = flag.String("addr", "localhost:8000", "Host and port of the server <host>:<port>")
	conc = flag.Int64("conc", 1000, "Number of concurrent operations")
	size = flag.Int64("size", 100, "Number of requests per batch")

	count int64
)

func main() {
	flag.Parse()

	c, err := client.New(*addr)
	if err != nil {
		panic(err)
	}

	var i int64
	for i = 0; i < *conc; i++ {
		go func() {
			reqs := []*client.ReqFetch{}

			var i int64
			for i = 0; i < *size; i++ {
				reqs = append(reqs, &client.ReqFetch{
					Database: "kadiyadb",
					From:     0,
					To:       uint64(60000000000 * 30),
					Fields:   []string{"foo", "bar"},
				})
			}

			for {
				// TODO randomize all track requests
				if _, err := c.Fetch(reqs); err != nil {
					panic(err)
				}
				atomic.AddInt64(&count, 1)
			}
		}()
	}

	for {
		time.Sleep(time.Second)
		log.Println(*size * count)
		atomic.StoreInt64(&count, 0)
	}
}
