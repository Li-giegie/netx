package netx

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

type Handler interface {
	Handle(r *Request, w *Response)
}

type Request struct {
	id       uint32
	isEOF    bool
	isClosed bool
	l        *sync.Mutex
	c        *Conn
}

func (r *Request) Id() int {
	return int(r.id)
}

var ErrClosed = errors.New("closed")

func (r *Request) Close() (err error) {
	if !r.isClosed {
		r.isClosed = true
		if !r.isEOF {
			if _, err = io.Copy(io.Discard, r); errors.Is(err, PacketEOF) {
				err = nil
			}
		}
		return nil
	}
	return ErrClosed
}

func (r *Request) Read(b []byte) (n int, err error) {
	if r.isClosed {
		return 0, ErrClosed
	}
	if r.isEOF {
		return 0, io.EOF
	}
	if n, err = r.c.Read(b); err != nil {
		r.l.Unlock()
		if errors.Is(err, PacketEOF) {
			r.isEOF = true
			return n, io.EOF
		}
		if err == io.EOF {
			err = ErrStreamError
		}
		return n, err
	}
	return n, err
}

type Response struct {
	id uint32
	c  *Conn
}

func (r *Response) Response(rd io.Reader) (int, error) {
	return r.c.WriteFrom(&message{
		typ: typeResponse,
		id:  r.id,
		r:   rd,
	})
}

const (
	typeRequest = iota
	typeResponse
)

type message struct {
	typ        uint8
	isReadHead bool
	id         uint32
	r          io.Reader
}

func (m *message) Read(b []byte) (n int, err error) {
	if len(b) < 5 {
		return 0, io.ErrShortBuffer
	}
	if !m.isReadHead {
		m.isReadHead = true
		b[0] = m.typ
		binary.LittleEndian.PutUint32(b[1:], m.id)
		n, err = m.r.Read(b[5:])
		n += 5
		return
	}
	n, err = m.r.Read(b)
	return
}
