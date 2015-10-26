package client

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kadirahq/kadiyadb-protocol"
	"github.com/kadirahq/kadiyadb-transport"
)

// message types
const (
	MsgTypeTrack = iota + 1
	MsgTypeFetch
)

const (
	// FlushInterval ...
	FlushInterval = 100 * time.Millisecond
)

// Conn ...
type Conn struct {
	conn       *transport.Conn
	cbTrackMap map[uint32]CbTrack
	cbTrackMtx *sync.Mutex
	cbFetchMap map[uint32]CbFetch
	cbFetchMtx *sync.Mutex
	nextID     uint32
}

// CbTrack ...
type CbTrack func(res *protocol.ResTrack, err error)

// CbFetch ...
type CbFetch func(res *protocol.ResFetch, err error)

// Dial ...
func Dial(addr string) (c *Conn, err error) {
	conn, err := transport.Dial(addr)
	if err != nil {
		return nil, err
	}

	c = &Conn{
		conn:       conn,
		cbTrackMap: make(map[uint32]CbTrack),
		cbTrackMtx: &sync.Mutex{},
		cbFetchMap: make(map[uint32]CbFetch),
		cbFetchMtx: &sync.Mutex{},
	}

	go c.recv()
	go c.send()

	return c, nil
}

// Track ...
func (c *Conn) Track(req *protocol.ReqTrack, cb CbTrack) {
	id := atomic.AddUint32(&c.nextID, 1)
	c.cbTrackMtx.Lock()
	c.cbTrackMap[id] = cb
	c.cbTrackMtx.Unlock()

	err := c.conn.Send(&protocol.Request{
		Id:  id,
		Req: &protocol.Request_Track{Track: req},
	})

	if err != nil {
		c.conn.Close()
		cb(nil, err)
	}
}

// Fetch ...
func (c *Conn) Fetch(req *protocol.ReqFetch, cb CbFetch) {
	id := atomic.AddUint32(&c.nextID, 1)
	c.cbFetchMtx.Lock()
	c.cbFetchMap[id] = cb
	c.cbFetchMtx.Unlock()

	err := c.conn.Send(&protocol.Request{
		Id:  id,
		Req: &protocol.Request_Fetch{Fetch: req},
	})

	if err != nil {
		c.conn.Close()
		cb(nil, err)
	}
}

func (c *Conn) send() {
	for {
		time.Sleep(FlushInterval)
		if err := c.conn.Flush(); err != nil {
			c.conn.Close()
		}
	}
}

func (c *Conn) recv() {
	res := &protocol.Response{}
	for {
		if err := c.conn.Recv(res); err != nil && err != io.EOF {
			break
		}

		switch t := res.Res.(type) {
		case *protocol.Response_Track:
			c.cbTrackMtx.Lock()
			cb, ok := c.cbTrackMap[res.Id]
			if ok {
				delete(c.cbTrackMap, res.Id)
				c.cbTrackMtx.Unlock()
				cb(t.Track, nil)
			} else {
				c.cbTrackMtx.Unlock()
			}
		case *protocol.Response_Fetch:
			c.cbFetchMtx.Lock()
			cb, ok := c.cbFetchMap[res.Id]
			if ok {
				delete(c.cbFetchMap, res.Id)
				c.cbFetchMtx.Unlock()
				cb(t.Fetch, nil)
			} else {
				c.cbFetchMtx.Unlock()
			}
		}
	}
}
