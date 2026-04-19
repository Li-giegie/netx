package netx

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
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

func NewConn(conn net.Conn, c *Config) *Conn {
	return &Conn{
		idCounter:   new(uint32),
		respSession: newSessionManager(),
		reqSessions: newSessionManager(),
		conn:        conn,
		rd:          NewReader(bufio.NewReaderSize(conn, c.ReadBufferSize)),
		wr:          NewWriter(conn, NewPool(c.WriteBufferSize)),
	}
}

type Conn struct {
	ctx         context.Context
	cancel      context.CancelFunc
	idCounter   *uint32
	respSession *sessionManager
	reqSessions *sessionManager
	conn        net.Conn
	rd          *Reader
	wr          *Writer
}

func (c *Conn) Serve(h Handler) error {
	c.ctx, c.cancel = context.WithCancel(context.TODO())
	ctx, cancel := context.WithCancelCause(context.TODO())
	defer func() {
		c.cancel()
		cancel(nil)
		c.conn.Close()
	}()
	go func() {
		for {
			data, err := ReadPacket(c.rd)
			if err != nil {
				cancel(err)
				return
			}
			chunk, err := newChunk(data)
			if err != nil {
				cancel(err)
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
						session.SessionReader.Close()
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
						session.SessionReader.Close()
					}
					continue
				}
			}
		}
	}()
	select {
	case <-c.ctx.Done():
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
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
	c.cancel()
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

func newConnManager() *connManager {
	return &connManager{
		m: make(map[string]*Conn),
	}
}

type connManager struct {
	m map[string]*Conn
	l sync.RWMutex
}

func (c *connManager) Set(k string, v *Conn) {
	c.l.Lock()
	c.m[k] = v
	c.l.Unlock()
}

func (c *connManager) Get(k string) (*Conn, bool) {
	c.l.RLock()
	v, ok := c.m[k]
	c.l.RUnlock()
	return v, ok
}

func (c *connManager) Del(k string) {
	c.l.Lock()
	delete(c.m, k)
	c.l.Unlock()
}

func (c *connManager) RangeConn(fn func(conn *Conn) bool) {
	c.l.RLock()
	for _, conn := range c.m {
		if fn(conn) {
			break
		}
	}
	c.l.RUnlock()
}

func (c *connManager) Clear() {
	c.l.Lock()
	c.m = make(map[string]*Conn)
	c.l.Unlock()
}
