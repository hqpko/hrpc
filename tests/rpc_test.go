package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/hqpko/hnet"

	"github.com/hqpko/hrpc"
)

func TestRPC(t *testing.T) {
	addr := "127.0.0.1:9093"
	go func() {
		_ = hnet.ListenSocket("tcp", addr, func(socket *hnet.Socket) {
			s := hrpc.NewServer()
			s.Register(1, func(args *Req, reply *Resp) error {
				reply.B = args.A + 1
				return nil
			})
			_ = s.Listen(socket)
		}, hnet.NewOption())
	}()

	time.Sleep(100 * time.Millisecond)
	client, _ := hrpc.Connect("tcp", addr)
	reply := &Resp{}
	e := client.Call(1, &Req{A: 1}, reply)
	fmt.Println(e, reply)
}

func BenchmarkRPC(b *testing.B) {
	addr := "127.0.0.1:9094"
	go func() {
		_ = hnet.ListenSocket("tcp", addr, func(socket *hnet.Socket) {
			s := hrpc.NewServer()
			s.Register(1, func(args *Req, reply *Resp) error {
				reply.B = args.A + 1
				return nil
			})
			_ = s.Listen(socket)
		}, hnet.NewOption())
	}()

	time.Sleep(100 * time.Millisecond)
	client, _ := hrpc.Connect("tcp", addr)
	for i := 0; i < b.N; i++ {
		reply := &Resp{}
		_ = client.Call(1, &Req{A: 1}, reply)
	}
}
