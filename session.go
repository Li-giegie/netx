package netx

import (
	"net"
	"sync"
)

func newSession(state *sessionState) *Session {
	return &Session{
		SessionWriter: &SessionWriter{
			sessionState: state,
		},
		SessionReader: &SessionReader{
			sessionState: state,
			recv:         make(chan []byte, 1),
		},
	}
}

// Session 一个逻辑层连接，每个 Session 都拥有一个唯一递增Id，发送和接收字节流通过其提供的 Write、Read方法操作
type Session struct {
	*SessionWriter
	*SessionReader
}

func newSessionState(flag uint8, id uint32, c *Conn) *sessionState {
	return &sessionState{
		flag: flag,
		id:   id,
		c:    c,
	}
}

type sessionState struct {
	flag uint8
	id   uint32
	c    *Conn
}

func (s *sessionState) Id() uint32 {
	return s.id
}

// SessionWriter 会话 Writer 发送字节流
type SessionWriter struct {
	*sessionState
	err  error
	once sync.Once
}

// WriteClose 写入后立即关闭写入会话，相当于 Write + Close
func (s *SessionWriter) WriteClose(b []byte) (n int, err error) {
	if n, err = s.Write(b); err != nil {
		return
	}
	return n, s.Close()
}

// Write 写入一个字节块
func (s *SessionWriter) Write(b []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	var n int
	n, s.err = s.c.writeChunk(&chunk{
		flag: s.flag,
		id:   s.id,
		data: b,
	})
	return n, s.err
}

// Close 关闭写会话，关闭后不能再向该会话写入
func (s *SessionWriter) Close() (err error) {
	if s.err != nil {
		return s.err
	}
	s.once.Do(func() {
		_, err = s.c.writeChunk(&chunk{
			flag: s.flag | flagClose,
			id:   s.id,
		})
		if err != nil {
			return
		}
		s.err = net.ErrClosed
	})
	return
}

// SessionReader 会话 Reader 发送方 SessionWriter 调用 Close 方法后 Read 返回 io.EOF 错误
type SessionReader struct {
	*sessionState
	recv chan []byte
	buf  []byte
	i    int
	err  error
	l    sync.Mutex
}

// Read 读取字节流，发送方 SessionWriter 调用 Close 方法后 Read 返回 io.EOF 错误
func (s *SessionReader) Read(b []byte) (n int, err error) {
	if s.i != len(s.buf) {
		n = copy(b, s.buf[s.i:])
		s.i += n
		return
	}
	rb, ok := <-s.recv
	if !ok {
		return 0, s.err
	}
	n = copy(b, rb)
	s.i = n
	s.buf = rb
	return
}

// ReadChunk 读取一个块，发送方 SessionWriter 调用 Close 方法后 Read 返回 io.EOF 错误
func (s *SessionReader) ReadChunk() ([]byte, error) {
	if s.i != len(s.buf) {
		s.i = len(s.buf)
		return s.buf, nil
	}
	rb, ok := <-s.recv
	if !ok {
		return nil, s.err
	}
	return rb, nil
}

func (s *SessionReader) setRecv(b []byte) {
	s.l.Lock()
	defer s.l.Unlock()
	if s.err != nil {
		return
	}
	s.recv <- b
	return
}

// Close 关闭读会话
func (s *SessionReader) Close() (err error) {
	return s.closeWithError(net.ErrClosed)
}

func (s *SessionReader) closeWithError(err error) error {
	s.l.Lock()
	defer s.l.Unlock()
	if s.err != nil {
		return s.err
	}
	s.err = err
	if s.flag&flagRequest != 0 {
		s.c.respSession.Del(s.id)
	} else {
		s.c.reqSessions.Del(s.id)
	}
	close(s.recv)
	return nil
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		m: make(map[uint32]*Session),
	}
}

type sessionManager struct {
	m map[uint32]*Session
	l sync.RWMutex
}

func (p *sessionManager) Set(k uint32, v *Session) {
	p.l.Lock()
	p.m[k] = v
	p.l.Unlock()
}

func (p *sessionManager) Get(k uint32) (*Session, bool) {
	p.l.RLock()
	v, ok := p.m[k]
	p.l.RUnlock()
	return v, ok
}

func (p *sessionManager) Del(k uint32) {
	p.l.Lock()
	delete(p.m, k)
	p.l.Unlock()
}
