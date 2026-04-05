package netx

import (
	"context"
	"io"
	"sync/atomic"
)

func NewClient(c *Conn, h Handler) *Client {
	return &Client{
		conn:     c,
		h:        h,
		response: newPipeManager(),
	}
}

type Client struct {
	conn      *Conn
	h         Handler
	idCounter uint32
	response  *pipeManager
}

func (c *Client) Serve() error {
	defer c.conn.Close()
	request := newPipeManager()
	for {
		data, err := ReadPacket(c.conn)
		if err != nil {
			return err
		}
		stm, err := newStream(data)
		if err != nil {
			return err
		}
		if stm.flag&flagRequest != 0 {
			if stm.seq == 0 {
				p := newPipe()
				p.closeFunc = func() {
					request.Del(stm.id)
				}
				p.seq++
				request.Set(stm.id, p)
				go c.h.Handle(&Request{id: stm.id, ReadCloser: p}, &responseWriter{w: c.conn, id: stm.id})
				if p.Write(stm.data); stm.flag&flagEOF != 0 {
					p.PipeWriter.Close()
				}
				continue
			}
			p, ok := request.Get(stm.id)
			if ok {
				if p.seq != stm.seq {
					p.PipeWriter.CloseWithError(ErrStreamSeqInvalid)
					request.Del(stm.id)
					continue
				}
				if stm.flag&flagERR != 0 {
					p.PipeWriter.CloseWithError(ErrStreamReadReader)
					request.Del(stm.id)
					continue
				}
				if len(stm.data) > 0 {
					p.Write(stm.data)
				}
				if stm.flag&flagEOF != 0 {
					p.PipeWriter.Close()
					request.Del(stm.id)
				}
				p.seq++
			}
			continue
		}
		if stm.flag&flagResponse != 0 {
			p, ok := c.response.Get(stm.id)
			if ok {
				if p.seq != stm.seq {
					p.PipeWriter.CloseWithError(ErrStreamSeqInvalid)
					c.response.Del(stm.id)
					continue
				}
				if stm.flag&flagERR != 0 {
					p.PipeWriter.CloseWithError(ErrStreamReadReader)
					c.response.Del(stm.id)
					continue
				}
				if len(stm.data) > 0 {
					p.PipeWriter.Write(stm.data)
				}
				if stm.flag&flagEOF != 0 {
					c.response.Del(stm.id)
					p.PipeWriter.Close()
				}
				p.seq++
				continue
			}
		}
	}
}

func (c *Client) Write(data []byte) (int, error) {
	return c.conn.Write((&stream{
		flag: flagRequest | flagEOF,
		id:   atomic.AddUint32(&c.idCounter, 1),
		data: data,
	}).Encode())
}

func (c *Client) Request(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
	p := newPipe()
	id := atomic.AddUint32(&c.idCounter, 1)
	p.closeFunc = func() {
		c.response.Del(id)
	}
	c.response.Set(id, p)
	buf := make([]byte, 4096)
	packet := &stream{
		flag: flagRequest,
		id:   id,
	}
	for {
		n, err := r.Read(buf)
		packet.data = buf[:n]
		if err != nil {
			if err != io.EOF {
				packet.flag |= flagERR
				c.conn.Write(packet.Encode())
				return nil, err
			}
			packet.flag |= flagEOF
			if _, err = c.conn.Write(packet.Encode()); err != nil {
				return nil, err
			}
			break
		}
		if _, err = c.conn.Write(packet.Encode()); err != nil {
			return nil, err
		}
		packet.seq++
	}
	return p, nil
}

func (c *Client) Stop() error {
	return c.conn.Close()
}
