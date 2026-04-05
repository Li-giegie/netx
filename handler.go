package netx

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

type Handler interface {
	// Handle 请求处理函数
	Handle(r Request, w ResponseWriter)
}

type Request interface {
	// Id 请求id
	Id() uint32
	// Scanner 返回字节流扫描器
	Scanner() *Scanner
	// ReadCloser Read请求流，或关闭请求流
	io.ReadCloser
	// BindJSON 请求流绑定到JSON对象
	BindJSON(a any) error
	// BindAny 绑定到实现了 Decoder 接口的任意对象
	BindAny(a Decoder) error
}

type Response interface {
	// Id 相应Id
	Id() uint32
	// Scanner 返回字节流扫描器
	Scanner() *Scanner
	// ReadCloser Read响应流，或关闭响应流
	io.ReadCloser
}

type ResponseWriter interface {
	// WriteCloser 响应字节流，Write 写入响应字节流，Close响应结束
	io.WriteCloser
	// Response 响应字节，每个请求只能响应一次，需要响应字节流请使用Write发送Close关闭
	Response(data []byte) (int, error)
	// ResponseString 响应字符串，每个请求只能响应一次，需要响应字节流请使用Write发送Close关闭
	ResponseString(data string) (int, error)
	// ResponseJSON 响应JSON编码的字节流
	ResponseJSON(a any) (int, error)
	// ResponseAny 响应实现了 Encoder 接口的任意类型
	ResponseAny(a Encoder) (int, error)
}

type responseWriter struct {
	w   io.Writer
	id  uint32
	seq uint32
	err error
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.err != nil {
		return 0, rw.err
	}
	p := stream{
		flag: flagResponse,
		id:   rw.id,
		seq:  rw.seq,
		data: b,
	}
	rw.seq++
	return rw.w.Write(p.Encode())
}
func (rw *responseWriter) Close() error {
	if rw.err != nil {
		return rw.err
	}
	p := stream{
		flag: flagResponse | flagEOF,
		id:   rw.id,
		seq:  rw.seq,
	}
	if _, rw.err = rw.w.Write(p.Encode()); rw.err != nil {
		return rw.err
	}
	rw.err = ErrAlreadyResponded
	return nil
}

func (rw *responseWriter) ResponseString(data string) (int, error) {
	return rw.Response([]byte(data))
}

func (rw *responseWriter) Response(data []byte) (int, error) {
	if rw.err != nil {
		return 0, rw.err
	}
	p := stream{
		flag: flagResponse | flagEOF,
		id:   rw.id,
		seq:  rw.seq,
		data: data,
	}
	n := 0
	if n, rw.err = rw.w.Write(p.Encode()); rw.err != nil {
		return n, rw.err
	}
	rw.err = ErrAlreadyResponded
	return n, nil
}

func (rw *responseWriter) ResponseJSON(a any) (int, error) {
	data, err := json.Marshal(a)
	if err != nil {
		return 0, err
	}
	return rw.Response(data)
}

type Encoder interface {
	Encode() ([]byte, error)
}

type Decoder interface {
	Decode([]byte) error
}

func (rw *responseWriter) ResponseAny(a Encoder) (int, error) {
	data, err := a.Encode()
	if err != nil {
		return 0, err
	}
	return rw.Response(data)
}

const (
	flagRequest uint8 = 1 << iota
	flagResponse
	flagEOF
	flagERR
)

func newStream(data []byte) (*stream, error) {
	p := new(stream)
	err := p.Decode(data)
	return p, err
}

type stream struct {
	flag uint8
	id   uint32
	seq  uint32
	data []byte
}

func (p *stream) Encode() []byte {
	data := make([]byte, 9+len(p.data))
	data[0] = p.flag
	binary.LittleEndian.PutUint32(data[1:5], p.id)
	binary.LittleEndian.PutUint32(data[5:9], p.seq)
	copy(data[9:], p.data)
	return data
}

func (p *stream) Decode(data []byte) error {
	if len(data) < 9 {
		return fmt.Errorf("packet len err")
	}
	p.flag = data[0]
	p.id = binary.LittleEndian.Uint32(data[1:5])
	p.seq = binary.LittleEndian.Uint32(data[5:9])
	p.data = data[9:]
	return nil
}
