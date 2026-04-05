package main

import (
	"context"
	"flag"
	"git.com/Li-giegie/netx"
	"git.com/Li-giegie/netx/example/stream"
	"io"
	"log"
	"os"
	"path/filepath"
)

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

var filename = flag.String("f", "", "file name")

func main() {
	flag.Parse()
	if *filename == "" {
		log.Fatal("-f flag is required")
	}

	f, err := os.Open(*filename)
	if err != nil {
		log.Fatal("open file", *filename, "err", err)
	}
	defer f.Close()

	conn, err := netx.Dial("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalln(err)
	}

	client := netx.NewClient(conn, stream.EchoHandler{})
	defer client.Close()

	go func() {
		defer client.Close()
		resp, err := client.RequestStream(context.TODO(), &file{File: f}, 1024*1024)
		if err != nil {
			log.Println("request error", err)
			return
		}
		data, err := io.ReadAll(resp)
		if err != nil {
			log.Println("read response error", err)
			return
		}
		log.Println(string(data))
	}()

	if err = client.Serve(); err != nil {
		log.Fatalln(err)
	}
}
