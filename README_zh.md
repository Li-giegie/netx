# netx
[![EN](https://img.shields.io/badge/Language-English-blue)](README.md)
[![中文](https://img.shields.io/badge/Language-中文-red)](README_zh.md)

一个增强原生net.Conn读写能力的库，兼容原生net库API，开箱即用支持请求响应模型的Server、Client封装对象。

Write 方法会把发送的字节封装成一个数据包

Read 方法读取字节，n返回读取的字节数，err返回错误信息，如果err不为nil 为`PacketEOF`错误代表一个包读取完成

Language: [English](README.md) | [中文](README_zh.md)

## 获取
```go
go get git.com/Li-giegie/netx
```
## 特性
- 轻量：同原生API一致，仅增加少量API
- 连接多路复用
- 封装字节：用于解决粘包
- 数据包校验和：不会被非客户端包干扰，如互联网爬虫包
- 支持字节流传输：上传、下载文件
- 支持请求响应模型

## 使用
1. 下面例子演示了使用原生API发送普通字节和字节流两种方式
    ```go
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
    ```

2. 除了上文原始API用法，还可以使用内部封装好的支持请求响应模型的Server、Client
    ```go
    package netx
    
    import (
        "bytes"
        "context"
        "fmt"
        "io"
        "log"
        "sync"
        "testing"
        "time"
    )
    
    func TestServer(t *testing.T) {
        l, err := Listen("tcp", "127.0.0.1:8888")
        if err != nil {
            t.Fatal(err)
        }
        defer l.Close()
        s := NewServer(l, handler{})
        if err = s.Serve(); err != nil {
            t.Error(err)
            return
        }
    }
    
    func TestClient(t *testing.T) {
        conn, err := Dial("tcp", "127.0.0.1:8888")
        if err != nil {
            t.Fatal(err)
            return
        }
        c := NewClient(conn, handler{})
        go func() {
            time.Sleep(time.Second)
            wg := sync.WaitGroup{}
            t1 := time.Now()
            for i := 0; i < 100000; i++ {
                wg.Add(1)
                go func(i int) {
                    defer wg.Done()
                    resp, err := c.Request(context.TODO(), bytes.NewBufferString(fmt.Sprintf("request %d", i)))
                    if err != nil {
                        t.Error(err)
                        return
                    }
                    data, err := io.ReadAll(resp)
                    resp.Close()
                    if err != nil {
                        t.Error(err)
                        return
                    }
                    _ = data
                    //log.Println("receive", string(data))
                }(i)
            }
            wg.Wait()
            fmt.Println(time.Since(t1))
        }()
        err = c.Serve()
        if err != nil {
            t.Fatal(err)
            return
        }
    }
    
    type handler struct{}
    
    func (s handler) Handle(r *Request, w *Response) {
        defer func() {
            fmt.Println("close", r.Close())
        }()
        data, err := io.ReadAll(r)
        if err != nil {
            log.Println("read err", err)
            return
        }
        log.Println(string(data))
        _, err = w.Response(bytes.NewBufferString("收到:" + string("data")))
        if err != nil {
            log.Println("write err", err)
            return
        }
    }
    ```