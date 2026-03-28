package netx

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
)

func NewClient(c *Conn, h Handler) *Client {
	return &Client{
		Conn:   c,
		h:      h,
		respCh: make(map[uint32]chan *Request),
	}
}

type Client struct {
	*Conn
	h      Handler
	msgId  uint32
	respLk sync.RWMutex
	respCh map[uint32]chan *Request
}

func (c *Client) Serve() error {
	defer c.Close()
	head := make([]byte, 5)
	readLock := new(sync.Mutex)
	for {
		readLock.Lock()
		_, err := io.ReadFull(c.Conn, head)
		if err != nil {
			return err
		}
		id := binary.LittleEndian.Uint32(head[1:])
		switch head[0] {
		case typeRequest:
			go c.h.Handle(&Request{
				id: id,
				l:  readLock,
				c:  c.Conn,
			}, &Response{
				id: id,
				c:  c.Conn,
			})
		case typeResponse:
			c.respLk.RLock()
			ch, ok := c.respCh[id]
			if ok {
				ch <- &Request{
					id: id,
					l:  readLock,
					c:  c.Conn,
				}
			}
			c.respLk.RUnlock()
		}
	}
}

func (c *Client) Request(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
	ch := make(chan *Request, 1)
	id := atomic.AddUint32(&c.msgId, 1)
	c.respLk.Lock()
	c.respCh[id] = ch
	c.respLk.Unlock()
	defer func() {
		c.respLk.Lock()
		close(ch)
		delete(c.respCh, id)
		c.respLk.Unlock()
	}()
	_, err := c.WriteFrom(&message{
		typ: typeRequest,
		id:  id,
		r:   r,
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}
