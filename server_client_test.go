package netx

import (
	"bytes"
	"context"
	"io"
	"log"
	"strconv"
	"sync"
	"testing"
)

func TestServer(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	l, err := Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	s := NewServer(l, EchoHandler{})
	if err = s.Serve(); err != nil {
		t.Error(err)
		return
	}
}

func TestClient(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Fatal(err)
		return
	}
	c := NewClient(conn, EchoHandler{})
	go func() {
		defer c.Close()
		testFunc := map[string]func(t2 *testing.T){
			"Request": func(t *testing.T) {
				data, err := c.Request(context.TODO(), []byte("Request"))
				if err != nil {
					t.Error(err)
					return
				}
				if string(data) != "Request" {
					t.Error("Err: " + string(data))
					return
				}
			},
			"RequestInStream": func(t *testing.T) {
				data, err := c.RequestInStream(context.TODO(), bytes.NewReader([]byte("RequestInStream")))
				if err != nil {
					t.Error(err)
					return
				}
				if string(data) != "RequestInStream" {
					t.Error("Err: " + string(data))
					return
				}
			},
			"RequestOutStream": func(t *testing.T) {
				resp, err := c.RequestOutStream(context.TODO(), []byte("RequestOutStream"))
				if err != nil {
					t.Error(err)
					return
				}
				defer resp.Close()
				data, err := resp.Scanner().ScanAll()
				if string(data) != "RequestOutStream" {
					t.Error("Err: " + string(data))
					return
				}
			},
			"RequestStream": func(t *testing.T) {
				resp, err := c.RequestStream(context.TODO(), bytes.NewReader([]byte("RequestStream")))
				if err != nil {
					t.Error(err)
					return
				}
				defer resp.Close()
				data, err := resp.Scanner().ScanAll()
				if string(data) != "RequestStream" {
					t.Error("Err: " + string(data))
					return
				}
			},
			"RequestWriter": func(t *testing.T) {
				writer, response := c.RequestWriter()
				if err != nil {
					t.Error(err)
					return
				}
				if _, err = writer.WriteClose([]byte("RequestWriter")); err != nil {
					t.Error(err)
					return
				}
				defer response.Close()
				data, err := response.Scanner().ScanAll()
				if err != nil {
					t.Error(err)
					return
				}
				if string(data) != "RequestWriter" {
					t.Error("Err: " + string(data))
					return
				}
			},
			"RequestJSON": func(t *testing.T) {
				var data string
				err := c.RequestJSON(context.TODO(), "RequestJSON", &data)
				if err != nil {
					t.Error(err)
					return
				}
				if data != "RequestJSON" {
					t.Error("Err: " + string(data))
					return
				}
			},
		}
		for s, f := range testFunc {
			if !t.Run(s, f) {
				t.Error("Err: ", s)
				return
			}
		}
	}()
	err = c.Serve()
	if err != nil {
		t.Fatal(err)
		return
	}
}

func BenchmarkClientRequest(b *testing.B) {
	conn, err := Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		b.Error(err)
		return
	}
	c := NewClient(conn, EchoHandler{})
	go c.Serve()
	defer c.Close()
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := c.Request(context.TODO(), []byte(strconv.Itoa(i)))
			if err != nil {
				b.Error(err)
				return
			}
			_ = resp
		}()
	}
	wg.Wait()
}

type EchoHandler struct{}

func (s EchoHandler) Handle(r Request, w ResponseWriter) {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		log.Printf("read err %s 1 %v %#v\n", data, err == nil, err)
		return
	}
	w.WriteString("1")
	w.WriteString("2")
	w.WriteString("3")
	w.Close()
	//log.Println("receive:", string(data))
	//if _, err = w.WriteClose(data); err != nil {
	//	log.Println("write err", err)
	//	return
	//}
}
