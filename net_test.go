package netx

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"testing"
	"time"
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
					if errors.Is(err, PacketEOF) {
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
}

func TestS(t *testing.T) {
	l, err := net.Listen("tcp", srvAddr)
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
			file, err := os.Create("./.tmp")
			if err != nil {
				t.Error(err)
				return
			}
			bw := bufio.NewWriterSize(file, 1024*1024)
			defer func() {
				bw.Flush()
				file.Close()
			}()
			_, err = io.Copy(bw, conn)
			if err != nil {
				t.Error(err)
				return
			}
			log.Println("copy done")
		}()
	}

}

func TestC(t *testing.T) {
	for _, n := range []int{1024, 2048, 4096, 8192, 10240} {
		err := upload(n)
		if err != nil {
			t.Error(err)
			return
		}
		time.Sleep(time.Second)
	}
}

func upload(wbs int) error {
	conn, err := net.Dial("tcp", "192.168.225.254:8000")
	if err != nil {
		return err
	}
	defer conn.Close()
	file, err := os.Open(`C:\Users\Lisa\Downloads\goland-2024.3.1.exe`)
	if err != nil {
		return err
	}
	defer file.Close()
	br := bufio.NewReaderSize(file, 1024*1024)
	buf := make([]byte, wbs)
	t1 := time.Now()
	log.Println("开始发送 写缓冲区：", wbs)
	defer func() {
		log.Println("消耗时间", time.Since(t1).String())
	}()
	for {
		n, err := br.Read(buf)
		if err != nil {
			if err != io.EOF {
				return err
			}
			if n > 0 {
				if _, err = conn.Write(buf[:n]); err != nil {
					return err
				}
			}
			break
		}
		if _, err = conn.Write(buf[:n]); err != nil {
			return err
		}
	}
	return nil
}

func TestPipe(t *testing.T) {
	pr, pw := io.Pipe()
	go func() {
		defer func() {
			fmt.Println("pr close", pr.Close())
		}()
		fmt.Println(io.Copy(os.Stdout, pr))
	}()
	for i := 0; i < 3; i++ {
		fmt.Println(pw.Write([]byte("hello world")))
	}
	fmt.Println("pw close", pw.CloseWithError(errors.New("err")))
	time.Sleep(time.Second)
}
