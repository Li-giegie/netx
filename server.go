package netx

import (
	"context"
	"errors"
	"net"
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
		conf:     c,
		listener: l,
	}
}

type Server struct {
	ctx         context.Context
	cancel      context.CancelCauseFunc
	idCounter   uint32
	respSession *sessionManager
	conf        Config
	listener    net.Listener
	// 并发安全的状态存储器
	storage
	// AcceptFunc 自定义接受连接回调函数，当 AcceptFunc != nil 时优先调用 AcceptFunc 接受连接
	// 返回值 error = ErrSkipConn 时跳过该连接，服务端会忽略该连接，并执行下一次 Accept
	// 返回值 error != nil && !errors.Is(err, ErrSkipConn) 时终止侦听，返回该错误
	AcceptFunc func(context.Context, net.Listener) (net.Conn, error)
	// OnConnect 新建连接第一个回调函数，异步执行
	OnConnect func(context.Context, *Conn)
	// OnStop 连接断开时触发的回调 conn 连接 err 断开原因
	OnStop func(context.Context, *Conn, error)
}

var ErrSkipConn = errors.New("skip conn")

func (s *Server) Serve(h Handler) error {
	s.respSession = newSessionManager()
	s.ctx, s.cancel = context.WithCancelCause(context.Background())
	defer func() {
		s.listener.Close()
		s.storage.Clear()
	}()
	go func() {
		for {
			var conn net.Conn
			var err error
			if s.AcceptFunc != nil {
				conn, err = s.AcceptFunc(s.ctx, s.listener)
			} else {
				conn, err = s.listener.Accept()
			}
			if err != nil {
				if errors.Is(err, ErrSkipConn) {
					continue
				}
				s.cancel(err)
				return
			}
			go func() {
				conn := NewConn(conn, &s.conf)
				if s.OnConnect != nil {
					go s.OnConnect(s.ctx, conn)
				}
				srvErr := conn.Serve(h)
				if s.OnStop != nil {
					s.OnStop(s.ctx, conn, srvErr)
				}
			}()
		}
	}()
	<-s.ctx.Done()
	err := context.Cause(s.ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *Server) Stop() {
	s.cancel(nil)
}
