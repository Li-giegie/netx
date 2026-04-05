package netx

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	l, err := Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	s := NewServer(l, handler{})
	if err = s.Serve(); err != nil {
		t.Error(err)
		return
	}
}

func TestClient(t *testing.T) {
	conn, err := Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Fatal(err)
		return
	}
	c := NewClient(conn, handler{})
	go func() {
		t1 := time.Now()
		defer func() {
			log.Println("cost", time.Now().Sub(t1))
			c.Stop()
		}()
		wg := sync.WaitGroup{}
		for i := 0; i < 100000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				resp, err := c.Request(context.TODO(), bytes.NewReader([]byte(strconv.Itoa(i))))
				if err != nil {
					t.Error(err)
					return
				}
				data, err := io.ReadAll(resp)
				if err != nil {
					t.Error(err)
					return
				}
				_ = data
				//fmt.Println(string(data))
			}(i)
		}

		wg.Wait()

	}()
	err = c.Serve()
	if err != nil {
		t.Fatal(err)
		return
	}
}

type handler struct{}

func (s handler) Handle(r *Request, w ResponseWriter) {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		log.Println("read err", err)
		return
	}
	_, err = w.Response([]byte("收到：" + string(data)))
	if err != nil {
		log.Println("write err", err)
		return
	}
}

type file struct {
	*os.File
	isReadName bool
}

func (f *file) Read(p []byte) (n int, err error) {
	if !f.isReadName {
		name := filepath.Base(f.Name())
		if len(p) <= len(name) {
			return 0, io.ErrShortBuffer
		}
		n = copy(p, name)
		p[n] = 0
		f.isReadName = true
		n2, err := f.File.Read(p[n+1:])
		return n + 1 + n2, err
	}
	return f.File.Read(p)
}

type uploadFileHandler struct{}

func (h uploadFileHandler) Handle(r *Request, w ResponseWriter) {
	defer r.Close()
	br := bufio.NewReader(r)
	name, err := br.ReadBytes(0)
	if err != nil {
		log.Println("read err", err)
		w.ResponseString("read err " + err.Error())
		return
	}
	file, err := os.Create(string(name[:len(name)-1]))
	if err != nil {
		log.Println("create err", err)
		w.ResponseString("create file err " + err.Error())
		return
	}
	defer file.Close()
	_, err = io.Copy(file, br)
	if err != nil {
		log.Println("copy err", err)
		w.ResponseString("copy err " + err.Error())
		return
	}
	log.Println("upload file ok")
	fmt.Println(w.ResponseString("upload file ok "))
}
