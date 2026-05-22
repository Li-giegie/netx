package netx

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultBufferSize = 1430
	chunkSize         = 5
)

const (
	flagRequest uint8 = 1 << iota
	flagResponse
	flagOpen
	flagClose
)

func newChunk(data []byte) (*chunk, error) {
	if len(data) < chunkSize {
		return nil, ErrChunkInvalid
	}
	var ch chunk
	ch.flag = data[0]
	ch.id = binary.LittleEndian.Uint32(data[1:chunkSize])
	ch.data = data[chunkSize:]
	return &ch, nil
}

type chunk struct {
	flag uint8
	id   uint32
	data []byte
}

var ErrChunkInvalid = errors.New("invalid chunk")

type Config struct {
	ReadBufferSize  int
	WriteBufferSize int
}

func Dial(network, addr string, opt ...Config) (*Conn, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	c := Config{
		ReadBufferSize:  DefaultBufferSize,
		WriteBufferSize: DefaultBufferSize,
	}
	if n := len(opt); n > 0 {
		if n != 1 {
			panic("need 1 option")
		}
		if opt[0].ReadBufferSize != 0 && opt[0].WriteBufferSize != 0 {
			c = opt[0]
		}
	}
	return NewConn(conn, &c), nil
}

func NewConn(conn net.Conn, cfg *Config) *Conn {
	c := &Conn{
		conn:        conn,
		idCounter:   new(uint32),
		respSession: newSessionManager(),
		reqSessions: newSessionManager(),
		rd:          NewReader(bufio.NewReaderSize(conn, cfg.ReadBufferSize)),
		wr:          NewWriter(conn, NewPool(cfg.WriteBufferSize)),
	}
	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	return c
}

type Conn struct {
	ctx         context.Context
	cancel      context.CancelCauseFunc
	idCounter   *uint32
	respSession *sessionManager
	reqSessions *sessionManager
	conn        net.Conn
	rd          *Reader
	wr          *Writer
	// 并发安全的状态存储器
	storage
}

func (c *Conn) Serve(h Handler) error {
	defer func() {
		c.conn.Close()
		c.storage.Clear()
	}()
	go func() {
		for {
			data, err := ReadPacket(c.rd)
			if err != nil {
				c.cancel(err)
				return
			}
			chunk, err := newChunk(data)
			if err != nil {
				c.cancel(err)
				return
			}
			if chunk.flag&flagRequest != 0 {
				if chunk.flag&flagOpen != 0 {
					session := newSession(newSessionState(flagResponse, chunk.id, c))
					c.reqSessions.Set(chunk.id, session)
					go h.Handle(session.SessionReader, session.SessionWriter)
					continue
				}
				session, ok := c.reqSessions.Get(chunk.id)
				if ok {
					if len(chunk.data) > 0 {
						session.SessionReader.setRecv(chunk.data)
					}
					if chunk.flag&flagClose != 0 {
						session.SessionReader.closeWithError(io.EOF)
					}
				}
				continue
			}
			if chunk.flag&flagResponse != 0 {
				session, ok := c.respSession.Get(chunk.id)
				if ok {
					if len(chunk.data) > 0 {
						session.SessionReader.setRecv(chunk.data)
					}
					if chunk.flag&flagClose != 0 {
						session.SessionReader.closeWithError(io.EOF)
					}
					continue
				}
			}
		}
	}()
	<-c.ctx.Done()
	err := context.Cause(c.ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c *Conn) writeChunk(ch *chunk) (int, error) {
	buf := c.wr.bufPool.Get()
	*buf = (*buf)[:0]
	*buf = append(*buf, ch.flag, byte(ch.id), byte(ch.id>>8), byte(ch.id>>16), byte(ch.id>>24))
	*buf = append(*buf, ch.data...)
	n, err := c.wr.Write(*buf)
	c.wr.bufPool.Put(buf)
	if err != nil {
		if n -= chunkSize; n < 0 {
			n = 0
		}
		return n, err
	}
	return n - chunkSize, nil
}

func (c *Conn) Stop() {
	c.cancel(nil)
}

// Session 创建一个会话
func (c *Conn) Session() (*Session, error) {
	id := atomic.AddUint32(c.idCounter, 1)
	_, err := c.writeChunk(&chunk{
		flag: flagRequest | flagOpen,
		id:   id,
	})
	if err != nil {
		return nil, err
	}
	session := newSession(newSessionState(flagRequest, id, c))
	c.respSession.Set(id, session)
	return session, nil
}

func (c *Conn) Request(ctx context.Context, data []byte) (resp []byte, err error) {
	curCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer cancel()
		var session *Session
		if session, err = c.Session(); err != nil {
			return
		}
		defer func() {
			session.SessionReader.Close()
			session.SessionWriter.Close()
		}()
		if _, err = session.WriteClose(data); err != nil {
			return
		}
		for {
			var b []byte
			if b, err = session.ReadChunk(); err != nil {
				if err == io.EOF {
					err = nil
				}
				return
			}
			resp = append(resp, b...)
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-curCtx.Done():
		return
	}
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *Conn) NetConn() net.Conn {
	return c.conn
}

type storage struct {
	m map[any]any
	l sync.RWMutex
}

func (s *storage) Set(k, v any) {
	s.l.Lock()
	if s.m == nil {
		s.m = make(map[any]any)
	}
	s.m[k] = v
	s.l.Unlock()
}

func (s *storage) Get(k any) (any, bool) {
	s.l.RLock()
	v, ok := s.m[k]
	s.l.RUnlock()
	return v, ok
}

func (s *storage) Del(k any) {
	s.l.Lock()
	delete(s.m, k)
	s.l.Unlock()
}

func (s *storage) Range(fn func(k, v any) bool) {
	s.l.RLock()
	for k, v := range s.m {
		if !fn(k, v) {
			break
		}
	}
	s.l.RUnlock()
}

func (s *storage) Clear() {
	s.l.Lock()
	s.m = nil
	s.l.Unlock()
}
