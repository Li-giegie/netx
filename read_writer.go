package netx

import (
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"sync"
)

var (
	InvalidCheckSum = errors.New("read packet err:invalid checksum")
	ErrStreamError  = errors.New("read stream error")
	PacketEOF       = errors.New("packet EOF")
)

func NewWriter(w io.Writer, bufPool Pool) *Writer {
	return &Writer{
		wr:      w,
		bufPool: bufPool,
	}
}

type Writer struct {
	l       sync.RWMutex
	wr      io.Writer
	bufPool Pool
}

const (
	bytePacket = iota
	byteStream
	byteStreamEOF
	byteStreamError
)

func (c *Writer) Write(p []byte) (n int, err error) {
	c.l.RLock()
	n, err = c.write(p, bytePacket)
	c.l.RUnlock()
	return
}

func (c *Writer) write(p []byte, flag uint8) (n int, err error) {
	buffer := c.bufPool.Get()
	*buffer = (*buffer)[:12]
	rnd := rand.Uint32()<<8 | uint32(flag)
	binary.LittleEndian.PutUint32((*buffer)[:4], rnd)
	binary.LittleEndian.PutUint32((*buffer)[4:8], uint32(len(p)))
	binary.LittleEndian.PutUint32((*buffer)[8:12], uint32(len(p))^rnd)
	*buffer = append(*buffer, p...)
	n, err = c.wr.Write(*buffer)
	c.bufPool.Put(buffer)
	return
}

func (c *Writer) WriteFrom(r io.Reader, bufSize int) (n int, err error) {
	c.l.Lock()
	buffer := c.bufPool.Get()
	if len(*buffer) < bufSize {
		*buffer = make([]byte, bufSize-len(*buffer))
	}
	rn := 0
	for {
		rn, err = r.Read(*buffer)
		n += rn
		if err != nil {
			if err != io.EOF {
				c.write((*buffer)[:rn], byteStreamError)
				break
			}
			_, err = c.write((*buffer)[:rn], byteStreamEOF)
			break
		}
		if _, err = c.write((*buffer)[:rn], byteStream); err != nil {
			break
		}
	}
	c.l.Unlock()
	c.bufPool.Put(buffer)
	return
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		rd:   r,
		head: make([]byte, 12),
		size: 0,
	}
}

type Reader struct {
	flag uint8
	size int
	rd   io.Reader
	head []byte
	err  error
}

func (c *Reader) Read(b []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.size == 0 {
		if _, c.err = io.ReadFull(c.rd, c.head); c.err != nil {
			return 0, c.err
		}
		rnd := binary.LittleEndian.Uint32(c.head[:4])
		length := binary.LittleEndian.Uint32(c.head[4:8])
		sum := binary.LittleEndian.Uint32(c.head[8:12])
		if length^rnd != sum {
			return 0, InvalidCheckSum
		}
		c.size = int(length)
		c.flag = uint8(rnd)
	}
	if len(b) > c.size {
		b = b[:c.size]
	}
	var n int
	if c.size > 0 {
		n, c.err = c.rd.Read(b)
	}
	if c.size -= n; c.size == 0 {
		if c.flag == bytePacket || c.flag == byteStreamEOF {
			return n, PacketEOF
		}
		if c.flag == byteStreamError {
			return n, ErrStreamError
		}
	}
	return n, c.err
}

type Pool interface {
	Get() *[]byte
	Put(*[]byte)
}

func NewPool(sizeOpt ...int) *BufferPool {
	size := 512
	if len(sizeOpt) > 0 {
		if sizeOpt[0] < 12 {
			panic("size must be at least 12")
		}
		size = sizeOpt[0]
	}
	return &BufferPool{
		Pool: sync.Pool{
			New: func() interface{} {
				buffer := make([]byte, 12, size)
				return &buffer
			},
		},
	}
}

type BufferPool struct {
	sync.Pool
}

func (p *BufferPool) Get() *[]byte {
	return p.Pool.Get().(*[]byte)
}
func (p *BufferPool) Put(b *[]byte) {
	p.Pool.Put(b)
}

func ReadPacketBuff(r io.Reader, b []byte) ([]byte, error) {
	for {
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err != PacketEOF {
				return b, err
			}
			return b, nil
		}

		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
	}
}

func ReadPacket(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 512)
	for {
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if errors.Is(err, PacketEOF) {
				err = nil
			}
			return b, err
		}

		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
	}
}
