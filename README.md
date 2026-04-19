# netx
[![EN](https://img.shields.io/badge/Language-English-blue)](README.md)
[![中文](https://img.shields.io/badge/Language-中文-red)](README_zh.md)

在一条连接上构建基于会话（逻辑概念）的通信模式

Language: [English](README.md) | [中文](README_zh.md)

## 获取
```go
go get git.com/Li-giegie/netx
```
## 特性
- 在一条连接上构建基于会话的通信模式
- 连接多路复用
- 封装字节：用于解决粘包
- 数据包校验和：不会被非客户端包干扰，如互联网爬虫包

## 使用
   ```go
   package netx
   
   import (
       "log"
       "testing"
   )
   
   func TestServer(t *testing.T) {
       log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
       srv, err := Listen("tcp", "127.0.0.1:8888")
       if err != nil {
           t.Fatal(err)
       }
       defer srv.Stop()
       log.Println("server started")
       err = srv.Serve(&Echo{})
       log.Println("start server err:", err)
   }
   
   func TestConn(t *testing.T) {
       log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
       conn, err := Dial("tcp", "127.0.0.1:8888")
       if err != nil {
           t.Fatal(err)
           return
       }
       defer conn.Close()
       log.Println("conn started")
       go func() {
           defer conn.Close()
           session, err := conn.Session()
           if err != nil {
               t.Error(err)
               return
           }
           defer session.SessionReader.Close()
           _, err = session.SessionWriter.Write([]byte("hello world"))
           if err != nil {
               t.Error(err)
               return
           }
           if err = session.SessionWriter.Close(); err != nil {
               t.Error(err)
               return
           }
           for {
               data, err := session.SessionReader.ReadChunk()
               if err != nil {
                   log.Println("read err", err)
                   return
               }
               log.Println(string(data))
           }
       }()
       err = conn.Serve(&Echo{})
       if err != nil {
           t.Fatal(err)
           return
       }
       log.Println("conn closed")
   }
   
   type Echo struct {}
   
   func (e Echo) Handle(r *SessionReader, w *SessionWriter) {
       defer w.Close()
       for {
           data, err := r.ReadChunk()
           if err != nil {
               log.Println(err)
               return
           }
           log.Println("data:", string(data))
           w.Write(data)
       }
   } 
   ```