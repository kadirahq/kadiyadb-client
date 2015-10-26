package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/kadirahq/kadiyadb-client"
	"github.com/kadirahq/kadiyadb-protocol"
)

var (
	addr = flag.String("addr", "localhost:8000", "server address <host>:<port>")
	conn = flag.Uint64("conn", 10, "number of concurrent connections")
	conc = flag.Uint64("conc", 1000, "number of concurrent operations")
	flds = []string{"a", "b", "c"}

	counter uint64
)

func main() {
	flag.Parse()

	for i := uint64(0); i < *conn; i++ {
		start(*conc / *conn)
	}

	for {
		time.Sleep(time.Second)
		fmt.Println(counter)
		atomic.StoreUint64(&counter, 0)
	}
}

func start(n uint64) {
	c, err := client.Dial(*addr)
	if err != nil {
		panic(err)
	}

	for i := uint64(0); i < n; i++ {
		go process(c)
	}
}

func process(c *client.Conn) {
	ch := make(chan bool)
	req := &protocol.ReqFetch{
		Database: "test",
		Fields:   make([]string, len(flds)),
	}

	for {
		req.To = uint64(time.Now().UnixNano())
		req.From = req.To - uint64(time.Minute)

		for i := range req.Fields {
			req.Fields[i] = flds[i] + strconv.Itoa(rand.Intn(100))
		}

		c.Fetch(req, func(res *protocol.ResFetch, err error) {
			ch <- true
		})

		<-ch
		atomic.AddUint64(&counter, 1)
	}
}
