package netx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
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
		time.Sleep(time.Second)
		wg := sync.WaitGroup{}
		t1 := time.Now()
		for i := 0; i < 100000; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				resp, err := c.Request(context.TODO(), bytes.NewBufferString(fmt.Sprintf("request %d", i)))
				if err != nil {
					t.Error(err)
					return
				}
				data, err := io.ReadAll(resp)
				resp.Close()
				if err != nil {
					t.Error(err)
					return
				}
				_ = data
				//log.Println("receive", string(data))
			}(i)
		}
		wg.Wait()
		fmt.Println(time.Since(t1))
	}()
	err = c.Serve()
	if err != nil {
		t.Fatal(err)
		return
	}
}

type handler struct{}

func (s handler) Handle(r *Request, w *Response) {
	defer func() {
		fmt.Println("close", r.Close())
	}()
	data, err := io.ReadAll(r)
	if err != nil {
		log.Println("read err", err)
		return
	}
	log.Println(string(data))
	_, err = w.Response(bytes.NewBufferString("收到:" + string("data")))
	if err != nil {
		log.Println("write err", err)
		return
	}
}
