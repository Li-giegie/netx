package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
)

var ErrNilHandlerFunc = errors.New("tcpserver: HandlerFunc is nil, set it before Serve")

type Server struct {
	Addr        string
	HandlerFunc func(ctx context.Context, conn net.Conn)
	status      uint32
	closeCh     chan struct{}
}

func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	if s.HandlerFunc == nil {
		return ErrNilHandlerFunc
	}

	curCtx, cancel := context.WithCancel(context.Background())
	defer func() {
		l.Close()
		cancel()
		atomic.StoreUint32(&s.status, uint32(0))
	}()

	s.closeCh = make(chan struct{})
	atomic.StoreUint32(&s.status, 1)

	doneCh := make(chan struct{})
	doneErr := error(nil)
	go func() {
		defer close(doneCh)

		for {
			conn, err := l.Accept()
			if err != nil {
				doneErr = err
				return
			}
			go s.HandlerFunc(curCtx, conn)
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return nil
	case <-doneCh:
		return doneErr
	}
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(context.Background(), l)
}

func (s *Server) ListenAndServeTLS(c *tls.Config) error {
	l, err := tls.Listen("tcp", s.Addr, c)
	if err != nil {
		return err
	}
	return s.Serve(context.Background(), l)
}

func (s *Server) Close() error {
	if atomic.CompareAndSwapUint32(&s.status, 1, 0) {
		close(s.closeCh)
	}
	return nil
}
