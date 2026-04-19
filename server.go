package netx

import (
	"context"
	"net"
	"sync"
)

func Listen(network, addr string, opt ...Config) (*Server, error) {
	l, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	return NewServer(l, opt...), nil
}

func NewServer(l net.Listener, opt ...Config) *Server {
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
	return &Server{
		respSession: newSessionManager(),
		conns:       newConnManager(),
		conf:        c,
		listener:    l,
	}
}

type Server struct {
	ctx         context.Context
	cancel      context.CancelFunc
	idCounter   uint32
	respSession *sessionManager
	conns       *connManager
	conf        Config
	listener    net.Listener
	once        sync.Once
}

func (s *Server) Serve(h Handler) error {
	s.once = sync.Once{}
	s.ctx, s.cancel = context.WithCancel(context.TODO())
	ctx, cancel := context.WithCancelCause(s.ctx)
	defer func() {
		s.listener.Close()
		s.cancel()
		cancel(nil)
	}()
	go func() {
		var err error
		defer func() {
			cancel(err)
		}()
		for {
			var conn net.Conn
			conn, err = s.listener.Accept()
			if err != nil {
				return
			}
			go func() {
				conn := NewConn(conn, &s.conf)
				s.conns.Set(conn.conn.RemoteAddr().String(), conn)
				defer func() {
					conn.Stop()
					s.conns.Del(conn.conn.RemoteAddr().String())
				}()
				conn.Serve(h)
			}()
		}
	}()
	select {
	case <-s.ctx.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) RangeConn(fn func(conn *Conn) bool) {
	s.conns.RangeConn(fn)
	return
}

func (s *Server) Stop() {
	s.once.Do(func() {
		s.cancel()
		s.conns.RangeConn(func(conn *Conn) bool {
			conn.Stop()
			return true
		})
		s.conns.Clear()
	})
}
