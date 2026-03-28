package netx

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
)

func NewServer(l *Listener, h Handler) *Server {
	return &Server{
		h:      h,
		l:      l,
		respCh: make(map[uint32]chan *Request),
		conns:  make(map[string]*Conn),
	}
}

type Server struct {
	h      Handler
	msgId  uint32
	respLk sync.RWMutex
	respCh map[uint32]chan *Request
	connLk sync.RWMutex
	conns  map[string]*Conn
	l      *Listener
}

func (s *Server) Serve() error {
	defer s.l.Close()
	for {
		conn, cErr := s.l.Accept()
		if cErr != nil {
			return cErr
		}
		go func() {
			s.connLk.Lock()
			s.conns[conn.RemoteAddr().String()] = conn
			s.connLk.Unlock()

			defer func() {
				s.connLk.Lock()
				delete(s.conns, conn.RemoteAddr().String())
				s.connLk.Unlock()
				conn.Close()
			}()

			head := make([]byte, 5)
			readLock := new(sync.Mutex)
			for {
				readLock.Lock()
				_, err := io.ReadFull(conn, head)
				if err != nil {
					return
				}
				id := binary.LittleEndian.Uint32(head[1:])
				switch head[0] {
				case typeRequest:
					go s.h.Handle(&Request{
						id: id,
						l:  readLock,
						c:  conn,
					}, &Response{
						id: id,
						c:  conn,
					})
				case typeResponse:
					s.respLk.RLock()
					ch, ok := s.respCh[id]
					if ok {
						ch <- &Request{
							id: id,
							l:  readLock,
							c:  conn,
						}
					}
					s.respLk.RUnlock()
				}
			}
		}()
	}
}

func (s *Server) Request(ctx context.Context, conn *Conn, r io.Reader) (io.ReadCloser, error) {
	ch := make(chan *Request, 1)
	id := atomic.AddUint32(&s.msgId, 1)
	s.respLk.Lock()
	s.respCh[id] = ch
	s.respLk.Unlock()
	defer func() {
		s.respLk.Lock()
		close(ch)
		delete(s.respCh, id)
		s.respLk.Unlock()
	}()
	_, err := conn.WriteFrom(&message{
		typ: typeRequest,
		id:  id,
		r:   r,
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

func (s *Server) RangeConn(fn func(addr string, conn *Conn) bool) {
	s.connLk.RLock()
	defer s.connLk.RUnlock()
	for addr, conn := range s.conns {
		if !fn(addr, conn) {
			break
		}
	}
}

func (s *Server) Stop() {
	s.l.Close()
	s.connLk.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.connLk.Unlock()
}
