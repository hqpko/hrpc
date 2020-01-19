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
	client := hrpc.NewClientWithOption(socket, time.Millisecond*100, time.Second)

	// client 仅能注册 仅接收无返回(oneWay) 协议，避免死锁
	client.RegisterOneWay(3, func(args []byte) {
		fmt.Printf("Server.OneWay\n")
	})
	go client.Run()

	// client.Call 请求并接收返回
	reply, err := client.Call(1, []byte{1})
	fmt.Printf("Client.Call, reply:%v, error:%v\n", reply, err) // [1],nil

	// client.Go 请求并返回请求产生的 Call，不阻塞等待返回，可以使用 call.Done() 等待返回
	call, _ := client.Go(1, []byte{1})
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
		server = hrpc.NewServer(socket)

		// server 端注册应答式协议
		server.Register(1, func(seq uint64, args []byte) {
			server.Reply(seq, args)
		})
		// server 端注册 仅接收无返回(oneWay) 协议
		server.RegisterOneWay(2, func(args []byte) {})
		go server.Run()
	})
}

```