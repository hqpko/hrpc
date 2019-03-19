package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/hqpko/hnet"

	"github.com/hqpko/hrpc"
)

var (
	hrpcAddr = "127.0.0.1:12003"
	hrpcOnce = new(sync.Once)
)

func Test_hrpc(t *testing.T) {
	startHRpcServer()

	client, err := hrpc.Connect("tcp", hrpcAddr, hrpc.DefaultOption)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reply := &Resp{}
	e := client.Call(1, &Req{A: 1}, reply)
	if e != nil {
		t.Fatal(e)
	}
	if reply.B != 2 {
		t.Fatal("call error")
	}
}

func Benchmark_hrpc_Call(b *testing.B) {
	startHRpcServer()

	client, err := hrpc.Connect("tcp", hrpcAddr, hrpc.DefaultOption)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	b.StartTimer()
	defer b.StopTimer()
	reply := &Resp{}
	req := &Req{A: 1}
	for i := 0; i < b.N; i++ {
		if err := client.Call(1, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_hrpc_Go(b *testing.B) {
	startHRpcServer()

	client, err := hrpc.Connect("tcp", hrpcAddr, hrpc.DefaultOption)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	b.StartTimer()
	defer b.StopTimer()
	req := &Req{A: 1}
	reply := &Resp{}
	for i := 0; i < b.N; i++ {
		_ = client.Go(1, req, reply, false)
	}
}

func startHRpcServer() {
	hrpcOnce.Do(func() {
		go func() {
			_ = hnet.ListenSocket("tcp", hrpcAddr, func(socket *hnet.Socket) {
				s := hrpc.NewServer(hrpc.DefaultOption)
				s.Register(1, func(args *Req, reply *Resp) {
					reply.B = args.A + 1
				})
				go func() {
					_ = s.Listen(socket)
				}()
			}, hnet.NewOption())
		}()
		time.Sleep(100 * time.Millisecond)
	})
}
