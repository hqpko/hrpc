# hrpc

### example

> 注意：正常情况下不建议使用 `server.Call`，容易产生死锁，仅在确定不会死锁的情况下使用

```go
package main

import (
	"fmt"
	"time"

	"github.com/hqpko/hnet"
	"github.com/hqpko/hrpc"
)

const rpcAddr = "127.0.0.1:9001"

var server *hrpc.Server

func main() {
	go listenConn()
	time.Sleep(time.Second)

	socket, _ := hnet.ConnectSocket(rpcAddr)
	client := hrpc.NewClient(socket).SetCallTimeout(time.Second)

	// client 仅能注册 oneWay 方式回调，避免死锁
	client.SetHandlerOneWay(func(pid int32, args []byte) {
		// one way handler
		fmt.Printf("Server.OneWay\n")
	})
	go client.Run()

	// client.Call 请求并接收返回
	reply, err := client.Call(1, []byte{1})
	fmt.Printf("Client.Call, reply:%v, error:%v\n", reply, err) // [1],nil

	// client.OneWay 仅发送请求
	err = client.OneWay(2, []byte{1})
	fmt.Printf("Client.OneWay, error:%v\n", err) // nil

	// server.OneWay 仅发送请求
	server.OneWay(3, []byte{1})

	time.Sleep(time.Second)
}

func listenConn() {
	hnet.ListenSocket(rpcAddr, func(socket *hnet.Socket) {
		server = hrpc.NewServer(socket)

		// server 端设置应答式回调
		server.SetHandlerCall(func(pid int32, seq uint64, args []byte) {
			_ = server.Reply(seq, args)
		})
		// server 端注册 仅接收无返回(oneWay) 回调
		server.SetHandlerOneWay(func(pid int32, args []byte) {})
		go server.Run()
	})
}

```