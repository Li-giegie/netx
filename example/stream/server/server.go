package main

import (
	"bufio"
	"fmt"
	"git.com/Li-giegie/netx"
	"io"
	"log"
	"os"
)

type uploadFileHandler struct{}

func (h uploadFileHandler) Handle(r netx.Request, w netx.ResponseWriter) {
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

func main() {
	l, err := netx.Listen("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Listening on", l.Addr())
	srv := netx.NewServer(l, &uploadFileHandler{})
	defer srv.Stop()
	err = srv.Serve()
	log.Println("serve", err)
}
