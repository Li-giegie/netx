package netx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrClosed           = errors.New("closed")
	ErrAlreadyResponded = errors.New("already responded")
	ErrStreamSeqInvalid = errors.New("stream seq invalid")
	ErrStreamReadReader = errors.New("read stream reader error")
)

func NewConn(conn net.Conn, writeBufSize, readBufSize int) *Conn {
	var r io.Reader = conn
	if readBufSize > 0 {
		r = bufio.NewReaderSize(r, readBufSize)
	}
	return &Conn{
		conn: conn,
		rd:   NewReader(r),
		wr:   NewWriter(conn, NewPool(writeBufSize)),
	}
}

type Conn struct {
	conn net.Conn
	rd   *Reader
	wr   *Writer
}

func (c *Conn) Close() error {
	return c.conn.Close()
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

const (
	DefaultWriteBufferSize = 1024 * 32

	pipeStateOpen = iota
	pipeStateClose
)

// PipeWriter
// 是一个减少系统Write调用提高性能的管道
// Write 方法会在适合的时机发出数据
type PipeWriter struct {
	state uint32
	err   error
	l     sync.RWMutex
	ch    chan []byte
}

func (p *PipeWriter) Write(b []byte) error {
	p.l.RLock()
	defer p.l.RUnlock()
	if p.state == pipeStateOpen {
		p.ch <- b
	}
	return p.err
}

func (p *PipeWriter) Close() error {
	p.l.Lock()
	defer p.l.Unlock()
	if p.state == pipeStateOpen {
		close(p.ch)
		p.state = pipeStateClose
		p.err = io.ErrClosedPipe
		return nil
	}
	return p.err
}

func (c *Conn) PipeWriter(ch chan []byte, bufferSize int) *PipeWriter {
	pw := &PipeWriter{ch: ch}
	go func() {
		buf := make([]byte, 0, bufferSize)
		for b := range pw.ch {
			if pw.err != nil {
				continue
			}
			if len(b)+len(buf) >= cap(buf) {
				if _, pw.err = c.Write(buf); pw.err != nil {
					pw.state = pipeStateClose
					continue
				}
				buf = buf[:0]
				if len(b) > cap(buf) {
					if _, pw.err = c.Write(b); pw.err != nil {
						pw.state = pipeStateClose
						return
					}
					continue
				}
			}
			buf = append(buf, b...)
			if len(pw.ch) == 0 {
				if _, pw.err = c.Write(b); pw.err != nil {
					pw.state = pipeStateClose
					continue
				}
			}
		}
	}()
	return pw
}

func (c *Conn) Write(b []byte) (n int, err error) {
	return c.wr.Write(b)
}

func (c *Conn) Read(b []byte) (n int, err error) {
	return c.rd.Read(b)
}

func (c *Conn) GetConn() net.Conn {
	return c.conn
}

type Dialer struct {
	net.Dialer
	WriteBufSize int
	ReadBufSize  int
}

func (d *Dialer) Dial(network, address string) (*Conn, error) {
	return d.DialContext(context.TODO(), network, address)
}

func (d *Dialer) DialContext(ctx context.Context, network, address string) (*Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewConn(conn, d.WriteBufSize, d.ReadBufSize), nil
}

type Listener struct {
	net.Listener
	WriteBufSize int
	ReadBufSize  int
}

func (l *Listener) Accept() (*Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewConn(conn, l.WriteBufSize, l.ReadBufSize), nil
}

type ListenConfig struct {
	net.ListenConfig
	WriteBufSize int
	ReadBufSize  int
}

func (l *ListenConfig) Listen(ctx context.Context, network string, address string) (*Listener, error) {
	listener, err := l.ListenConfig.Listen(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &Listener{
		Listener:     listener,
		WriteBufSize: l.WriteBufSize,
		ReadBufSize:  l.ReadBufSize,
	}, nil
}

type Option func(any)

func WithWriteBufSize(size int) Option {
	return func(a any) {
		switch v := a.(type) {
		case *ListenConfig:
			v.WriteBufSize = size
		case *Dialer:
			v.WriteBufSize = size
		default:
			panic("invalid type")
		}
	}
}

func WithReadBufSize(size int) Option {
	return func(a any) {
		switch v := a.(type) {
		case *ListenConfig:
			v.ReadBufSize = size
		case *Dialer:
			v.ReadBufSize = size
		default:
			panic("invalid type")
		}
	}
}

func Listen(network, address string, opts ...Option) (*Listener, error) {
	var lc ListenConfig
	lc.WriteBufSize = 1024
	lc.ReadBufSize = 1024
	for _, opt := range opts {
		opt(&lc)
	}
	return lc.Listen(context.TODO(), network, address)
}

func Dial(network, address string, opts ...Option) (*Conn, error) {
	var d Dialer
	d.WriteBufSize = 1024
	d.ReadBufSize = 1024
	for _, opt := range opts {
		opt(&d)
	}
	return d.Dial(network, address)
}
