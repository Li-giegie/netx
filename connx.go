package netx

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
)

const (
	flagRequest uint8 = 1 << iota
	flagResponse
	flagEOF
	flagERR
)

func newChunk(data []byte) (*chunk, error) {
	p := new(chunk)
	err := p.Decode(data)
	return p, err
}

type chunk struct {
	flag uint8
	id   uint32
	seq  uint32
	data []byte
}

func (p *chunk) Encode() []byte {
	data := make([]byte, 9+len(p.data))
	data[0] = p.flag
	binary.LittleEndian.PutUint32(data[1:5], p.id)
	binary.LittleEndian.PutUint32(data[5:9], p.seq)
	copy(data[9:], p.data)
	return data
}

func (p *chunk) Decode(data []byte) error {
	if len(data) < 9 {
		return fmt.Errorf("packet len err")
	}
	p.flag = data[0]
	p.id = binary.LittleEndian.Uint32(data[1:5])
	p.seq = binary.LittleEndian.Uint32(data[5:9])
	p.data = data[9:]
	return nil
}

type ConnX struct {
	idCounter *uint32
	response  *streamManager
	conn      *Conn
	h         Handler
}

func (c *ConnX) Serve() error {
	defer c.conn.Close()
	request := newStreamManager()
	for {
		data, err := ReadPacket(c.conn)
		if err != nil {
			return err
		}
		chunk, err := newChunk(data)
		if err != nil {
			return err
		}
		if chunk.flag&flagRequest != 0 {
			if chunk.seq == 0 {
				req := newStream(chunk.id)
				req.seq++
				if chunk.flag&flagEOF != 0 {
					req.writeEOF(chunk.data)
				} else {
					req.closeFunc = func() {
						request.Del(chunk.id)
					}
					request.Set(chunk.id, req)
					if len(chunk.data) > 0 {
						req.write(chunk.data)
					}
				}
				go c.h.Handle(req, &requestResponseWriter{flag: flagResponse, id: chunk.id, conn: c})
				continue
			}
			p, ok := request.Get(chunk.id)
			if ok {
				if p.seq != chunk.seq {
					p.closeWithError(ErrStreamSeqInvalid)
					continue
				}
				if chunk.flag&flagERR != 0 {
					p.closeWithError(ErrStreamReadReader)
					continue
				}
				p.seq++
				if len(chunk.data) > 0 {
					p.write(chunk.data)
				}
				if chunk.flag&flagEOF != 0 {
					p.closeWithError(io.EOF)
				}
			}
			continue
		}
		if chunk.flag&flagResponse != 0 {
			p, ok := c.response.Get(chunk.id)
			if ok {
				if p.seq != chunk.seq {
					p.closeWithError(ErrStreamSeqInvalid)
					continue
				}
				if chunk.flag&flagERR != 0 {
					p.closeWithError(ErrStreamReadReader)
					continue
				}
				p.seq++
				if len(chunk.data) > 0 {
					p.write(chunk.data)
				}
				if chunk.flag&flagEOF != 0 {
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
		resp, err = response.(*stream).Scanner().ScanAll()
		response.Close()
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
		resp, err = response.(*stream).Scanner().ScanAll()
		response.Close()
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-curCtx.Done():
		return
	}
}

func (c *ConnX) RequestOutStream(ctx context.Context, data []byte) (Response, error) {
	writer, resp := c.RequestWriter()
	_, err := writer.WriteClose(data)
	if err != nil {
		resp.Close()
		return nil, err
	}
	return resp, nil
}

func (c *ConnX) RequestStream(ctx context.Context, r io.Reader, readBufSizeOpt ...int) (Response, error) {
	reqWriter, resp := c.RequestWriter()
	curCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var err error
	go func() {
		defer cancel()
		readBufSize := DefaultBufferSize
		if len(readBufSizeOpt) > 0 && readBufSizeOpt[0] > 0 {
			readBufSize = readBufSizeOpt[0]
		}
		buf := make([]byte, readBufSize)
		_, err = io.CopyBuffer(reqWriter, r, buf)
		reqWriter.CloseWithError(err)
	}()
	select {
	case <-ctx.Done():
		resp.Close()
		return nil, ctx.Err()
	case <-curCtx.Done():
		if err != nil {
			resp.Close()
			return nil, err
		}
		return resp, nil
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

func (c *ConnX) writeChunk(ch *chunk) (int, error) {
	n, err := c.conn.Write(ch.Encode())
	if err != nil {
		if n -= 9; n < 0 {
			n = 0
		}
		return n, err
	}
	return n - 9, nil
}

type RequestWriter interface {
	iRequestResponseWriter
}

func (c *ConnX) RequestWriter() (RequestWriter, Response) {
	rw := &requestResponseWriter{
		flag: flagRequest,
		id:   atomic.AddUint32(c.idCounter, 1),
		conn: c,
	}
	stream := newStream(rw.id)
	stream.closeFunc = func() {
		c.response.Del(rw.id)
	}
	c.response.Set(rw.id, stream)
	return rw, stream
}

func (c *ConnX) Close() error {
	return c.conn.Close()
}
