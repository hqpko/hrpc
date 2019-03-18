package tests

import (
	"context"
	"testing"
	"time"

	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/server"
)

func BenchmarkRPCX_Call(b *testing.B) {
	addr := testGetAddr()
	go func() {
		s := server.NewServer()
		//s.RegisterName("Arith", new(example.Arith), "")
		_ = s.Register(new(RPCReq), "")
		_ = s.Serve("tcp", addr)
	}()

	time.Sleep(100 * time.Millisecond)

	c := client.NewClient(client.DefaultOption)
	_ = c.Connect("tcp", addr)
	for i := 0; i < b.N; i++ {
		reply := &Resp{}
		_ = c.Call(context.Background(), "RPCReq", "Mul", &Req{A: 1}, reply)
	}
}

func BenchmarkRPCX_Go(b *testing.B) {
	addr := testGetAddr()
	go func() {
		s := server.NewServer()
		//s.RegisterName("Arith", new(example.Arith), "")
		_ = s.Register(new(RPCReq), "")
		_ = s.Serve("tcp", addr)
	}()

	time.Sleep(100 * time.Millisecond)

	c := client.NewClient(client.DefaultOption)
	_ = c.Connect("tcp", addr)
	for i := 0; i < b.N; i++ {
		reply := &Resp{}
		_ = c.Go(context.Background(), "RPCReq", "Mul", &Req{A: 1}, reply, make(chan *client.Call))
	}
}
