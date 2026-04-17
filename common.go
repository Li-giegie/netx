package netx

import (
	"encoding/json"
	"io"
	"sync"
)

func newConnManager() *connManager {
	return &connManager{
		m: make(map[string]*ConnX),
	}
}

type connManager struct {
	m map[string]*ConnX
	l sync.RWMutex
}

func (c *connManager) Set(k string, v *ConnX) {
	c.l.Lock()
	c.m[k] = v
	c.l.Unlock()
}
func (c *connManager) Get(k string) (*ConnX, bool) {
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

func (c *connManager) RangeConn(fn func(conn *ConnX) bool) {
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
	c.m = make(map[string]*ConnX)
	c.l.Unlock()
}

func newStreamManager() *streamManager {
	return &streamManager{
		m: make(map[uint32]*stream),
	}
}

type streamManager struct {
	m map[uint32]*stream
	l sync.RWMutex
}

func (p *streamManager) Set(k uint32, v *stream) {
	p.l.Lock()
	p.m[k] = v
	p.l.Unlock()
}

func (p *streamManager) Get(k uint32) (*stream, bool) {
	p.l.RLock()
	v, ok := p.m[k]
	p.l.RUnlock()
	return v, ok
}

func (p *streamManager) Del(k uint32) {
	p.l.Lock()
	delete(p.m, k)
	p.l.Unlock()
}

func newStream(id uint32) *stream {
	return &stream{
		id: id,
		r:  make(chan []byte, 1),
	}
}

type Scanner struct {
	r *stream
}

// Scan 扫描请求字节流，并报告是否可以下一次扫描，当next = true、seq = 1、err = io.EOF 时，说明请求字节流已经读完，可以立即结束扫描，如果不结束下一次读取next返回false
func (s *Scanner) Scan() (data []byte, next bool) {
	if len(s.r.data) > 0 {
		data = s.r.data
		s.r.data = nil
		return data, true
	}
	data, next = <-s.r.r
	return
}

// Err 返回发送请求字节流错误，发送完字节流返回io.EOF或其他发送错误，io.EOF并不代表读取完成，仅代表发送结束
func (s *Scanner) Err() error {
	return s.r.err
}

// Seq 返回请求流的序号从1开始计数
func (s *Scanner) Seq() uint32 {
	return s.r.seq
}

// ScanAll 扫描全部字节
func (s *Scanner) ScanAll() (b []byte, err error) {
	for {
		data, next := s.Scan()
		if !next {
			break
		}
		if s.r.seq == 1 && s.r.err == io.EOF {
			return data, nil
		}
		b = append(b, data...)
	}
	if s.r.err != io.EOF {
		err = s.r.err
	}
	return
}

type stream struct {
	id        uint32
	seq       uint32
	closeFunc func()
	i         int
	err       error
	data      []byte
	r         chan []byte
	once      sync.Once
	l         sync.Mutex
}

// Id 返回请求id
func (r *stream) Id() uint32 {
	return r.id
}

// Scanner 返回一个扫描器，后续的请求字节流从扫描器中获取
func (r *stream) Scanner() *Scanner {
	return &Scanner{r: r}
}

// Read 返回请求字节流
func (r *stream) Read(b []byte) (n int, err error) {
	if r.i != len(r.data) {
		n = copy(b, r.data[r.i:])
		r.i += n
		return
	}
	rb, ok := <-r.r
	if !ok {
		return 0, r.err
	}
	n = copy(b, rb)
	r.i = n
	r.data = rb
	return
}

func (r *stream) BindJSON(a any) error {
	data, err := r.Scanner().ScanAll()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, a)
}

func (r *stream) BindAny(a Decoder) error {
	data, err := r.Scanner().ScanAll()
	if err != nil {
		return err
	}
	return a.Decode(data)
}

// Close 关闭请求字节流
func (r *stream) Close() error {
	return r.closeWithError(io.ErrClosedPipe)
}

func (r *stream) write(b []byte) (n int, err error) {
	r.l.Lock()
	defer r.l.Unlock()
	if r.err != nil {
		return 0, r.err
	}
	r.r <- b
	return len(b), nil
}

func (r *stream) writeEOF(b []byte) (int, error) {
	r.l.Lock()
	defer r.l.Unlock()
	if r.err != nil {
		return 0, r.err
	}
	r.once.Do(func() {
		r.err = io.EOF
		r.data = b
		if r.closeFunc != nil {
			r.closeFunc()
		}
		close(r.r)
	})
	return len(b), nil
}

func (r *stream) closeWithError(e error) error {
	r.l.Lock()
	defer r.l.Unlock()
	if r.err != nil {
		return r.err
	}
	r.once.Do(func() {
		r.err = e
		if r.closeFunc != nil {
			r.closeFunc()
		}
		close(r.r)
	})
	return nil
}

func newOrArg[T any](b bool, val T) *orArg[T] {
	return &orArg[T]{
		b:   b,
		val: val,
	}
}

type orArg[T any] struct {
	b   bool
	val T
}

func or[T any](args ...*orArg[T]) T {
	for _, item := range args {
		if item.b {
			return item.val
		}
	}
	panic("must have a value is true")
}

type iRequestResponseWriter interface {
	Id() uint32
	// Write 写入字节流，不能并发调用
	Write([]byte) (int, error)
	// Close 关闭字节流写入，关闭后不可再写入
	Close() error
	// CloseWithError 异常关闭字节流发送，并报告对端本次发送异常
	CloseWithError(e error) error
	// WriteClose 写入后立即关闭，相当于 Write + Close
	WriteClose([]byte) (int, error)
	// WriteString 写入String
	WriteString(s string) (n int, err error)
	// WriteJSON 写入JSON
	WriteJSON(v any) (n int, err error)
	// WriteAny 写入实现了 Encoder 接口的任意类型
	WriteAny(v Encoder) (n int, err error)
}

type requestResponseWriter struct {
	flag uint8
	id   uint32
	seq  uint32
	err  error
	conn *ConnX
}

func (r *requestResponseWriter) Id() uint32 {
	return r.id
}

func (r *requestResponseWriter) Write(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err = r.conn.writeChunk(&chunk{
		flag: r.flag,
		id:   r.id,
		seq:  r.seq,
		data: p,
	})
	r.seq++
	return
}

func (r *requestResponseWriter) Close() (err error) {
	if r.err != nil {
		return r.err
	}
	_, err = r.conn.writeChunk(&chunk{
		flag: r.flag | flagEOF,
		id:   r.id,
		seq:  r.seq,
	})
	r.err = ErrClosed
	return
}

func (r *requestResponseWriter) CloseWithError(e error) error {
	if e == nil {
		return r.Close()
	}
	if r.err != nil {
		return r.err
	}
	_, err := r.conn.writeChunk(&chunk{
		flag: r.flag | flagERR,
		id:   r.id,
		seq:  r.seq,
	})
	if err != nil {
		r.err = err
		return r.err
	}
	r.err = e
	return nil
}

func (r *requestResponseWriter) WriteClose(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err = r.conn.writeChunk(&chunk{
		flag: r.flag | flagEOF,
		id:   r.id,
		seq:  r.seq,
		data: p,
	})
	if err != nil {
		r.err = err
		return
	}
	r.err = ErrClosed
	return
}

func (r *requestResponseWriter) WriteString(s string) (n int, err error) {
	return r.Write([]byte(s))
}

func (r *requestResponseWriter) WriteJSON(v any) (n int, err error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return r.Write(data)
}

func (r *requestResponseWriter) WriteAny(v Encoder) (n int, err error) {
	data, err := v.Encode()
	if err != nil {
		return 0, err
	}
	return r.Write(data)
}
