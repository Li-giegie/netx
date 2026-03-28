package netx

import (
	"fmt"
	"log"
	"os"
	"testing"
)

var (
	srvAddr = "127.0.0.1:8000"
)

func TestListen(t *testing.T) {
	l, err := Listen("tcp", srvAddr,
		WithReadBufSize(1024),
		WithWriteBufSize(1024),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			t.Error(err)
			return
		}
		go func() {
			defer conn.Close()
			// 一次读完一个包
			data, err := ReadPacket(conn)
			if err != nil {
				t.Error(err)
				return
			}
			log.Println(string(data))

			// 读取字节流，根据END错误判断是否读完
			file, err := os.Create("./README.md.bak")
			if err != nil {
				t.Error(err)
				return
			}
			defer file.Close()
			buf := make([]byte, 1024*32)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					if err != END {
						t.Error(err)
						return
					}
					log.Println("接收完毕")
					break
				}
				_, err = file.Write(buf[:n])
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
}

func TestDial(t *testing.T) {
	conn, err := Dial("tcp", srvAddr,
		WithReadBufSize(1024),
		WithWriteBufSize(1024),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.Write([]byte("hello world"))
	if err != nil {
		t.Error(err)
		return
	}
	// 发送文件字节流
	file, err := os.Open("./README.md")
	if err != nil {
		t.Error(err)
		return
	}
	defer file.Close()
	n, err := conn.WriteFrom(file)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("发送成功", n)
}
