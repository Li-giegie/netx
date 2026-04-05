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

func newRequestResponseManager() *requestResponseManager {
	return &requestResponseManager{
		m: make(map[uint32]*requestResponse),
	}
}

type requestResponseManager struct {
	m map[uint32]*requestResponse
	l sync.RWMutex
}

func (p *requestResponseManager) Set(k uint32, v *requestResponse) {
	p.l.Lock()
	p.m[k] = v
	p.l.Unlock()
}

func (p *requestResponseManager) Get(k uint32) (*requestResponse, bool) {
	p.l.RLock()
	v, ok := p.m[k]
	p.l.RUnlock()
	return v, ok
}

func (p *requestResponseManager) Del(k uint32) {
	p.l.Lock()
	delete(p.m, k)
	p.l.Unlock()
}

func newRequestResponse() *requestResponse {
	return &requestResponse{
		r: make(chan []byte, 1),
	}
}

type Scanner struct {
	r *requestResponse
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

type requestResponse struct {
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
func (r *requestResponse) Id() uint32 {
	return r.id
}

// Scanner 返回一个扫描器，后续的请求字节流从扫描器中获取
func (r *requestResponse) Scanner() *Scanner {
	return &Scanner{r: r}
}

// Read 返回请求字节流
func (r *requestResponse) Read(b []byte) (n int, err error) {
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

func (r *requestResponse) BindJSON(a any) error {
	data, err := r.Scanner().ScanAll()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, a)
}

func (r *requestResponse) BindAny(a Decoder) error {
	data, err := r.Scanner().ScanAll()
	if err != nil {
		return err
	}
	return a.Decode(data)
}

// Close 关闭请求字节流
func (r *requestResponse) Close() error {
	return r.closeWithError(io.ErrClosedPipe)
}

func (r *requestResponse) write(b []byte) (n int, err error) {
	r.l.Lock()
	defer r.l.Unlock()
	if r.err != nil {
		return 0, r.err
	}
	r.r <- b
	return len(b), nil
}

func (r *requestResponse) writeEOF(b []byte) (int, error) {
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

func (r *requestResponse) closeWithError(e error) error {
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
