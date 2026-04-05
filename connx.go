package netx

import (
	"context"
	"encoding/json"
	"io"
	"sync/atomic"
)

type ConnX struct {
	idCounter *uint32
	response  *requestResponseManager
	conn      *Conn
	h         Handler
}

func (c *ConnX) Serve() error {
	defer c.conn.Close()
	request := newRequestResponseManager()
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
				req := newRequestResponse()
				req.seq++
				if stm.flag&flagEOF != 0 {
					req.writeEOF(stm.data)
				} else {
					req.closeFunc = func() {
						request.Del(stm.id)
					}
					request.Set(stm.id, req)
					if len(stm.data) > 0 {
						req.write(stm.data)
					}
				}
				go c.h.Handle(req, &responseWriter{w: c.conn, id: stm.id})
				continue
			}
			p, ok := request.Get(stm.id)
			if ok {
				if p.seq != stm.seq {
					p.closeWithError(ErrStreamSeqInvalid)
					continue
				}
				if stm.flag&flagERR != 0 {
					p.closeWithError(ErrStreamReadReader)
					continue
				}
				p.seq++
				if len(stm.data) > 0 {
					p.write(stm.data)
				}
				if stm.flag&flagEOF != 0 {
					p.closeWithError(io.EOF)
				}
			}
			continue
		}
		if stm.flag&flagResponse != 0 {
			p, ok := c.response.Get(stm.id)
			if ok {
				if p.seq != stm.seq {
					p.closeWithError(ErrStreamSeqInvalid)
					continue
				}
				if stm.flag&flagERR != 0 {
					p.closeWithError(ErrStreamReadReader)
					continue
				}
				p.seq++
				if len(stm.data) > 0 {
					p.write(stm.data)
				}
				if stm.flag&flagEOF != 0 {
					p.closeWithError(io.EOF)
				}
				continue
			}
		}
	}
}

func (c *ConnX) Request(ctx context.Context, data []byte) (resp []byte, err error) {
	curCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer cancel()
		var response Response
		if response, err = c.RequestOutStream(curCtx, data); err != nil {
			return
		}
		resp, err = response.(*requestResponse).Scanner().ScanAll()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-curCtx.Done():
		return
	}
}

func (c *ConnX) RequestInStream(ctx context.Context, r io.Reader, readBufSizeOpt ...int) (resp []byte, err error) {
	curCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer cancel()
		var response Response
		if response, err = c.RequestStream(curCtx, r, readBufSizeOpt...); err != nil {
			return
		}
		resp, err = response.(*requestResponse).Scanner().ScanAll()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-curCtx.Done():
		return
	}
}

func (c *ConnX) RequestOutStream(ctx context.Context, data []byte) (Response, error) {
	p := newRequestResponse()
	id := atomic.AddUint32(c.idCounter, 1)
	p.closeFunc = func() {
		c.response.Del(id)
	}
	p.id = id
	c.response.Set(id, p)
	_, err := c.conn.Write((&stream{
		flag: flagRequest | flagEOF,
		id:   id,
		data: data,
	}).Encode())
	return p, err
}

func (c *ConnX) RequestStream(ctx context.Context, r io.Reader, readBufSizeOpt ...int) (Response, error) {
	readBufSize := DefaultBufferSize
	if len(readBufSizeOpt) > 0 && readBufSizeOpt[0] > 0 {
		readBufSize = readBufSizeOpt[0]
	}
	p := newRequestResponse()
	id := atomic.AddUint32(c.idCounter, 1)
	p.closeFunc = func() {
		c.response.Del(id)
	}
	c.response.Set(id, p)
	buf := make([]byte, readBufSize)
	packet := &stream{
		flag: flagRequest,
		id:   id,
	}
	curCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var err error
	go func() {
		defer cancel()
		var n int
		for {
			n, err = r.Read(buf)
			packet.data = buf[:n]
			if err != nil {
				if err != io.EOF {
					packet.flag |= flagERR
					c.conn.Write(packet.Encode())
					return
				}
				packet.flag |= flagEOF
				_, err = c.conn.Write(packet.Encode())
				return
			}
			if _, err = c.conn.Write(packet.Encode()); err != nil {
				return
			}
			packet.seq++
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-curCtx.Done():
		return p, err
	}
}

func (c *ConnX) RequestJSON(ctx context.Context, a, dst any) error {
	req, err := json.Marshal(a)
	if err != nil {
		return err
	}
	resp, err := c.Request(ctx, req)
	if err != nil {
		return err
	}
	return json.Unmarshal(resp, dst)
}

func (c *ConnX) RequestAny(ctx context.Context, a Encoder, dst Decoder) error {
	req, err := a.Encode()
	if err != nil {
		return err
	}
	resp, err := c.Request(ctx, req)
	if err != nil {
		return err
	}
	return dst.Decode(resp)
}

func (c *ConnX) Close() error {
	return c.conn.Close()
}
