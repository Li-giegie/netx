package main

import (
	"bufio"
	"context"
	"github.com/Li-giegie/netx"
	"github.com/Li-giegie/netx/example/stream"
	"log"
	"os"
)

func main() {
	l, err := netx.Listen("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Listening on", l.Addr())
	srv := netx.NewServer(l, &stream.EchoHandler{})
	defer srv.Stop()
	go func() {
		defer srv.Stop()
		sc := bufio.NewScanner(os.Stdin)
		print(">> ")
		for sc.Scan() {
			if sc.Text() == "exit" {
				return
			}
			srv.RangeConn(func(conn *netx.ConnX) bool {
				resp, err := conn.Request(context.TODO(), sc.Bytes())
				if err != nil {
					log.Println(err)
					return true
				}
				log.Println(string(resp))
				return true
			})
			print(">> ")
		}
	}()
	err = srv.Serve()
	log.Println("serve", err)
}
