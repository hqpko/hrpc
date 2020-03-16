# hrpc

### example

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

	socket, _ := hnet.ConnectSocket("tcp", rpcAddr)
	client := hrpc.NewClientWithOption(time.Second)
	client.SetSocket(socket)

	// client 仅能注册 仅接收无返回(oneWay) 回调，避免死锁
	client.SetHandlerOneWay(func(pid int32, args []byte) {
		// one way handler
		fmt.Printf("Server.OneWay\n")
	})
	go client.Run()

	// client.Call 请求并接收返回
	reply, err := client.Call(1, []byte{1})
	fmt.Printf("Client.Call, reply:%v, error:%v\n", reply, err) // [1],nil

	// client.Go 请求并返回请求产生的 Call，不阻塞等待返回，可以使用 call.Done() 等待返回
	call := client.Go(1, []byte{1})
	reply, timeout := call.Done()
	fmt.Printf("Client.Go, reply:%v, timeout:%v\n", reply, timeout) // [1],nil

	// client.OneWay 仅发送请求，无返回
	err = client.OneWay(2, []byte{1})
	fmt.Printf("Client.OneWay, error:%v\n", err) // nil

	// server.OneWay 仅发送请求，无返回，再次说明，server 不支持 Call，避免和 Client 互相 Call 导致死锁
	server.OneWay(3, []byte{1})

	time.Sleep(time.Second)
}

func listenConn() {
	hnet.ListenSocket("tcp", rpcAddr, func(socket *hnet.Socket) {
		server = hrpc.NewServer()
		server.SetSocket(socket)

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