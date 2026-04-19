package netx

import (
	"io"
	"log"
	"testing"
)

func TestServer(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	srv, err := Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	log.Println("server started")
	err = srv.Serve(&Echo{})
	log.Println("start server err:", err)
}

func TestConn(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		t.Fatal(err)
		return
	}
	defer conn.Stop()
	log.Println("conn started")
	go func() {
		defer conn.Stop()
		session, err := conn.Session()
		if err != nil {
			t.Error(err)
			return
		}
		defer session.SessionReader.Close()
		_, err = session.SessionWriter.Write([]byte("hello world"))
		if err != nil {
			t.Error(err)
			return
		}
		if err = session.SessionWriter.Close(); err != nil {
			t.Error(err)
			return
		}
		for {
			data, err := session.SessionReader.ReadChunk()
			if err != nil {
				log.Println("read err", err)
				return
			}
			log.Println(string(data))
		}
	}()
	err = conn.Serve(&Echo{})
	if err != nil {
		t.Fatal(err)
		return
	}
	log.Println("conn closed")
}

func TestServerBench(t *testing.T) {
	srv, err := Listen("tcp", "127.0.0.1:8888", Config{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer srv.Stop()
	err = srv.Serve(bench{})
	log.Println("serve err: -2", err)
}

func BenchmarkConn(b *testing.B) {
	conn, err := Dial("tcp", "127.0.0.1:8888", Config{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
		return
	}
	defer func() {
		conn.Stop()
	}()
	go func() {
		err = conn.Serve(bench{})
		if err != nil {
			b.Error(err)
		}
	}()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session, err := conn.Session()
		if err != nil {
			b.Error("create session err", err)
			return
		}
		_, err = session.WriteClose([]byte("hello world"))
		if err != nil {
			b.Error("write session err", err)
			return
		}
		for {
			if _, err = session.SessionReader.ReadChunk(); err != nil {
				if err != io.EOF {
					b.Error("read session err", err)
					break
				}
				break
			}
		}
		session.SessionReader.Close()
	}
}

func BenchmarkConnS2(b *testing.B) {
	conn, err := Dial("tcp", "127.0.0.1:8888", Config{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
		return
	}
	defer func() {
		conn.Stop()
	}()
	go func() {
		err = conn.Serve(bench{})
		if err != nil {
			b.Error(err)
		}
	}()
	b.ResetTimer()
	session, err := conn.Session()
	if err != nil {
		b.Error("create session err", err)
		return
	}
	defer func() {
		session.SessionWriter.Close()
		session.SessionReader.Close()
	}()

	for i := 0; i < b.N; i++ {
		_, err := session.Write([]byte("hello world"))
		if err != nil {
			b.Error("write session err", err)
			return
		}
		if _, err = session.SessionReader.ReadChunk(); err != nil {
			if err != io.EOF {
				b.Error("read session err", err)
				break
			}
			break
		}
	}
}

func BenchmarkConnS3(b *testing.B) {
	conn, err := Dial("tcp", "127.0.0.1:8888", Config{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
		return
	}
	defer func() {
		conn.Stop()
	}()
	go func() {
		err = conn.Serve(bench{})
		if err != nil {
			b.Error(err)
		}
	}()
	b.ResetTimer()
	session, err := conn.Session()
	if err != nil {
		b.Error("create session err", err)
		return
	}
	defer func() {
		session.SessionWriter.Close()
		session.SessionReader.Close()
	}()
	go io.Copy(io.Discard, session.SessionReader)
	for i := 0; i < b.N; i++ {
		_, err := session.Write([]byte("hello world"))
		if err != nil {
			b.Error("write session err", err)
			return
		}
	}
}

// BenchmarkConn-12           18382             65008 ns/op            1375 B/op         10 allocs/op
// BenchmarkConnS2-12         25860             46237 ns/op             529 B/op          2 allocs/op
// BenchmarkConnS3-12        112328             12936 ns/op             493 B/op          1 allocs/op
type Echo struct{}

func (e Echo) Handle(r *SessionReader, w *SessionWriter) {
	defer w.Close()
	for {
		data, err := r.ReadChunk()
		if err != nil {
			log.Println(err)
			return
		}
		log.Println("data:", string(data))
		w.Write(data)
	}
}

type bench struct{}

func (bench) Handle(r *SessionReader, w *SessionWriter) {
	defer func() {
		w.Close()
		r.Close()
	}()
	for {
		data, err := r.ReadChunk()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			return
		}
		w.Write(data)
	}
}
