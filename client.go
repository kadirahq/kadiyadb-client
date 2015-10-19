package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/kadirahq/kadiyadb-transport"
)

// message types
const (
	MsgTypeTrack = iota + 1
	MsgTypeFetch
)

// Client is a kadiyadb Client
type Client struct {
	conn     *transport.Conn
	tran     *transport.Transport
	inflight map[uint64]chan [][]byte
	mtx      *sync.RWMutex
	nextID   uint64
}

// New creates a new kadiyadb Client
func New(addr string) (c *Client, err error) {
	conn, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}

	c = &Client{
		conn:     conn,
		tran:     transport.New(conn),
		inflight: make(map[uint64]chan [][]byte, 1),
		mtx:      &sync.RWMutex{},
	}

	go c.read()

	return c, nil
}

func (c *Client) read() {
	for {
		// `msgType` is dropped. Its not important for the client
		data, id, _, err := c.tran.ReceiveBatch()
		if err != nil {
			fmt.Println(err)
			break
		}

		c.mtx.RLock()
		ch, ok := c.inflight[id]
		c.mtx.RUnlock()

		if !ok {
			fmt.Println("Unknown response id")
			continue
		}

		ch <- data
		c.mtx.Lock()
		delete(c.inflight, id)
		c.mtx.Unlock()
	}
}

func (c *Client) call(b [][]byte, msgType uint8) ([][]byte, error) {
	ch := make(chan [][]byte, 1)
	id := atomic.AddUint64(&c.nextID, 1)

	c.mtx.Lock()
	c.inflight[id] = ch
	c.mtx.Unlock()

	if err := c.tran.SendBatch(b, id, msgType); err != nil {
		// Error during a `SendBatch` call makes the connection unusable
		// Data sent following such an error may not be parsable
		c.conn.Close()
		return nil, err
	}

	return <-ch, nil
}

// Track tracks kadiyadb points
func (c *Client) Track(reqs []*ReqTrack) (ress []*ResTrack, err error) {
	reqData := make([][]byte, len(reqs))
	for i, req := range reqs {
		if reqData[i], err = req.Marshal(); err != nil {
			return nil, err
		}
	}

	resData, err := c.call(reqData, MsgTypeTrack)
	if err != nil {
		return nil, err
	}

	ress = make([]*ResTrack, len(reqs))
	for i := range ress {
		res := &ResTrack{}
		ress[i] = res

		if err := res.Unmarshal(resData[i]); err != nil {
			return nil, err
		}
	}

	return ress, nil
}

// Fetch fetches kadiyadb point data
func (c *Client) Fetch(reqs []*ReqFetch) (ress []*ResFetch, err error) {
	reqData := make([][]byte, len(reqs))
	for i, req := range reqs {
		if reqData[i], err = req.Marshal(); err != nil {
			return nil, err
		}
	}

	resData, err := c.call(reqData, MsgTypeTrack)
	if err != nil {
		return nil, err
	}

	ress = make([]*ResFetch, len(reqs))
	for i := range ress {
		res := &ResFetch{}
		ress[i] = res

		if err := res.Unmarshal(resData[i]); err != nil {
			return nil, err
		}
	}

	return ress, nil
}
