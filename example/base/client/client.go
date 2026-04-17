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
	conn, err := netx.Dial("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalln(err)
	}
	client := netx.NewClient(conn, stream.EchoHandler{})
	defer client.Close()
	go func() {
		defer client.Close()
		sc := bufio.NewScanner(os.Stdin)
		print(">> ")
		for sc.Scan() {
			if sc.Text() == "exit" {
				return
			}
			resp, err := client.Request(context.Background(), sc.Bytes())
			if err != nil {
				log.Println(err)
				return
			}
			log.Println(string(resp))
			print(">> ")
		}
	}()
	if err = client.Serve(); err != nil {
		log.Fatalln(err)
	}
}
