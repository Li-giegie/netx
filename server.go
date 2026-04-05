package netx

func NewServer(l *Listener, h Handler) *Server {
	return &Server{
		h:        h,
		l:        l,
		response: newPipeManager(),
		conns:    newConnManager(),
	}
}

type Server struct {
	h        Handler
	msgId    uint32
	response *pipeManager
	conns    *connManager
	l        *Listener
}

func (s *Server) Serve() error {
	defer s.l.Close()
	for {
		conn, cErr := s.l.Accept()
		if cErr != nil {
			return cErr
		}
		go func() {
			s.conns.Set(conn.RemoteAddr().String(), conn)
			request := newPipeManager()
			defer func() {
				conn.Close()
				s.conns.Del(conn.RemoteAddr().String())
			}()
			for {
				data, err := ReadPacket(conn)
				if err != nil {
					return
				}
				stm, err := newStream(data)
				if err != nil {
					return
				}
				if stm.flag&flagRequest != 0 {
					if stm.seq == 0 {
						p := newPipe()
						p.closeFunc = func() {
							request.Del(stm.id)
						}
						p.seq++
						request.Set(stm.id, p)
						go s.h.Handle(&Request{id: stm.id, ReadCloser: p}, &responseWriter{w: conn, id: stm.id})
						if p.Write(stm.data); stm.flag&flagEOF != 0 {
							p.PipeWriter.Close()
						}
						continue
					}
					p, ok := request.Get(stm.id)
					if ok {
						if p.seq != stm.seq {
							p.PipeWriter.CloseWithError(ErrStreamSeqInvalid)
							request.Del(stm.id)
							continue
						}
						if stm.flag&flagERR != 0 {
							p.PipeWriter.CloseWithError(ErrStreamReadReader)
							request.Del(stm.id)
							continue
						}
						if len(stm.data) > 0 {
							p.Write(stm.data)
						}
						if stm.flag&flagEOF != 0 {
							p.PipeWriter.Close()
							request.Del(stm.id)
						}
						p.seq++
					}
					continue
				}
				if stm.flag&flagResponse != 0 {
					p, ok := s.response.Get(stm.id)
					if ok {
						if p.seq != stm.seq {
							p.PipeWriter.CloseWithError(ErrStreamSeqInvalid)
							s.response.Del(stm.id)
							continue
						}
						if stm.flag&flagERR != 0 {
							p.PipeWriter.CloseWithError(ErrStreamReadReader)
							s.response.Del(stm.id)
							continue
						}
						if len(stm.data) > 0 {
							p.PipeWriter.Write(stm.data)
						}
						if stm.flag&flagEOF != 0 {
							p.PipeWriter.Close()
							s.response.Del(stm.id)
						}
						p.seq++
						continue
					}
				}
			}
		}()
	}
}

func (s *Server) RangeConn(fn func(conn *Conn) bool) {
	s.conns.RangeConn(fn)
	return
}

func (s *Server) Stop() {
	s.conns.RangeConn(func(conn *Conn) bool {
		conn.Close()
		return true
	})
	s.conns.Clear()
}
