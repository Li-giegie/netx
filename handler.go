package netx

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type Request struct {
	id uint32
	io.ReadCloser
}

func (r *Request) Id() uint32 {
	return r.id
}

type Handler interface {
	Handle(r *Request, w ResponseWriter)
}

type ResponseWriter interface {
	io.WriteCloser
	Response(data []byte) (int, error)
	ResponseString(data string) (int, error)
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

func newPipe() *pipe {
	r, w := io.Pipe()
	return &pipe{
		PipeReader: r,
		PipeWriter: w,
	}
}

type pipe struct {
	seq uint32
	*io.PipeReader
	*io.PipeWriter
	closeFunc func()
	sync.Once
}

func (p *pipe) Read(b []byte) (int, error) {
	return p.PipeReader.Read(b)
}

func (p *pipe) Close() (err error) {
	p.Do(func() {
		p.closeFunc()
		err = p.PipeReader.Close()
	})
	return
}
