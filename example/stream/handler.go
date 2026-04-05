package stream

import (
	"git.com/Li-giegie/netx"
	"io"
	"log"
)

type EchoHandler struct{}

func (h EchoHandler) Handle(r netx.Request, w netx.ResponseWriter) {
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Request:", string(data))
	w.Response(data)
}
