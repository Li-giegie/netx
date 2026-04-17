package main

import (
	"context"
	"github.com/Li-giegie/netx"
	any_req_resp "github.com/Li-giegie/netx/example/any-req-resp"
	"github.com/Li-giegie/netx/example/stream"
	"log"
)

func main() {
	conn, err := netx.Dial("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalln(err)
	}

	client := netx.NewClient(conn, stream.EchoHandler{})
	defer client.Close()

	go func() {
		defer client.Close()
		var result any_req_resp.Api
		err = client.RequestAny(context.TODO(), &any_req_resp.Api{
			Path:   "/api/test",
			Method: "any",
			Param:  map[string]string{"id": "xxx", "other": "hello"},
		}, &result)
		if err != nil {
			log.Println("request error", err)
			return
		}
		log.Printf("%#v\n", result)
	}()

	if err = client.Serve(); err != nil {
		log.Fatalln(err)
	}
}
