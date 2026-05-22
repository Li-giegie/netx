package netx

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"
)

func TestWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := NewWriter(buf, NewPool(1024))
	_, err := w.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(hex.EncodeToString(buf.Bytes()))
	fmt.Println(buf.String())
}

func TestReader(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := NewWriter(buf, NewPool(1024))
	n := 10
	for i := 0; i < n; i++ {
		_, err := w.Write([]byte("hello" + strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	r := NewReader(buf)
	data := make([]byte, 0, 1024)
	err := error(nil)
	for i := 0; ; i++ {
		data, err = ReadPacketBuff(r, data)
		if err != nil {
			if err == io.EOF && i == n {
				return
			}
			t.Fatal(err)
		}
		fmt.Println(string(data))
		data = data[:0]
	}
}

func BenchmarkWriter(b *testing.B) {
	w := NewWriter(io.Discard, NewPool(1024))
	for i := 0; i < b.N; i++ {
		_, err := w.Write([]byte("hello"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReader(b *testing.B) {
	data, err := hex.DecodeString("0019f893050000000519f89368656c6c6f")
	if err != nil {
		b.Fatal(err)
	}
	dr := &reader{
		b:   data,
		off: 0,
	}
	r := NewReader(dr)
	msg := make([]byte, 0, 1024)
	for i := 0; i < b.N; i++ {
		msg, err = ReadPacketBuff(r, msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type reader struct {
	b   []byte
	off int
}

func (r *reader) Read(p []byte) (int, error) {
	n := copy(p, r.b[r.off%len(r.b):])
	r.off += n
	return n, nil
}

func Test_reader(t *testing.T) {
	r := reader{
		b:   []byte("hello"),
		off: 0,
	}
	var buf = make([]byte, 1024)
	for {
		fmt.Println(r.Read(buf))
		fmt.Println(string(buf))
		time.Sleep(time.Second)
	}
}
