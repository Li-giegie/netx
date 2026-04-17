package netx

func NewServer(l *Listener, h Handler) *Server {
	return &Server{
		h:        h,
		l:        l,
		response: newStreamManager(),
		conns:    newConnManager(),
	}
}

type Server struct {
	h         Handler
	idCounter uint32
	response  *streamManager
	conns     *connManager
	l         *Listener
}

func (s *Server) Serve() error {
	defer s.l.Close()
	for {
		conn, cErr := s.l.Accept()
		if cErr != nil {
			return cErr
		}
		go func() {
			conn := &ConnX{
				idCounter: &s.idCounter,
				response:  newStreamManager(),
				conn:      conn,
				h:         s.h,
			}
			s.conns.Set(conn.conn.RemoteAddr().String(), conn)
			defer func() {
				conn.Close()
				s.conns.Del(conn.conn.RemoteAddr().String())
			}()
			conn.Serve()
		}()
	}
}

func (s *Server) RangeConn(fn func(conn *ConnX) bool) {
	s.conns.RangeConn(fn)
	return
}

func (s *Server) Stop() {
	s.l.Close()
	s.conns.RangeConn(func(conn *ConnX) bool {
		conn.Close()
		return true
	})
	s.conns.Clear()
}
