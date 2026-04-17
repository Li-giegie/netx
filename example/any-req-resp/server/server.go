package main

import (
	"github.com/Li-giegie/netx"
	any_req_resp "github.com/Li-giegie/netx/example/any-req-resp"
	"log"
)

type Handler struct{}

func (h Handler) Handle(r netx.Request, w netx.ResponseWriter) {
	defer func() {
		r.Close()
		w.Close()
	}()

	var param any_req_resp.Api
	err := r.BindAny(&param)
	if err != nil {
		log.Println("bind error:", err)
		w.WriteString("bind error " + err.Error())
		return
	}
	log.Printf("param %#v\n", param)
	w.WriteAny(&param)
}

func main() {
	l, err := netx.Listen("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Listening on", l.Addr())
	srv := netx.NewServer(l, &Handler{})
	defer srv.Stop()
	err = srv.Serve()
	log.Println("serve", err)
}
