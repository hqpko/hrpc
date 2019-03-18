package tests

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/hqpko/hnet"

	"github.com/hqpko/hrpc"
)

func TestRPC(t *testing.T) {
	addr := testGetAddr()
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

func BenchmarkHRPC_Call(b *testing.B) {
	addr := testGetAddr()
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
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		reply := &Resp{}
		_ = client.Call(1, &Req{A: 1}, reply)
	}
}

func BenchmarkHRPC_Go(b *testing.B) {
	addr := testGetAddr()
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
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		reply := &Resp{}
		_ = client.Go(1, &Req{A: 1}, reply, false)
	}
}

func testGetAddr() string {
	rand.Seed(time.Now().UnixNano())
	addr := fmt.Sprintf("127.0.0.1:%d", 10000+rand.Int31n(3000))
	return addr
}
