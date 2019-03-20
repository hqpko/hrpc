# hrpc

```go
package main

import (
	"fmt"
	"time"

	"github.com/hqpko/hnet"
	"github.com/hqpko/hrpc"
)

var (
	hrpcAddr = "127.0.0.1:11001"
)

func main() {
	go hnet.ListenSocket("tcp", hrpcAddr, func(socket *hnet.Socket) {
		go listenServer(socket)
	})

	// wait listener
	time.Sleep(100 * time.Millisecond)

	client := hrpc.NewClient()
	client.Run(getSocket(hrpcAddr))

	resp := &Resp{}
	err := client.Call(1, &Req{A: 1}, resp)
	fmt.Println(err) // nil

	err = client.Call(2, &Req{A: 2})
	fmt.Println(err) // nil

	call := client.Go(2, &Req{A: 1})
	call.Done()
	fmt.Println(call.Error()) // nil
}

func listenServer(socket *hnet.Socket) {
	s := hrpc.NewServer()
	// register ptotocols
	s.Register(1, addCount)
	s.Register(2, showCount)
	_ = s.Listen(socket)
}

func addCount(args *Req, reply *Resp) error {
	reply.B = args.A + 1
	return nil
}

func showCount(args *Req) {
	fmt.Println(args.A)
}

func getSocket(addr string) *hnet.Socket {
	socket, _ := hnet.ConnectSocket("tcp", addr)
	return socket
}

```