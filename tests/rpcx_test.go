package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/server"
)

var (
	rpcxAddr = "127.0.0.1:12004"
	rpcxOnec = new(sync.Once)
)

type RPCXReq struct {
}

func (r *RPCXReq) Mul(ctx context.Context, args *Req, reply *Resp) error {
	reply.B = args.A + 1
	return nil
}

func Benchmark_rpcx_Call(b *testing.B) {
	startRpcxServer()

	c := client.NewClient(client.DefaultOption)
	if err := c.Connect("tcp", rpcxAddr); err != nil {
		b.Fatal(err)
	}
	req := &Req{A: 1}
	reply := &Resp{}
	for i := 0; i < b.N; i++ {
		if err := c.Call(context.Background(), "RPCXReq", "Mul", req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_rpcx_Go(b *testing.B) {
	startRpcxServer()

	c := client.NewClient(client.DefaultOption)
	if err := c.Connect("tcp", rpcxAddr); err != nil {
		b.Fatal(err)
	}
	req := &Req{A: 1}
	reply := &Resp{}
	for i := 0; i < b.N; i++ {
		_ = c.Go(context.Background(), "RPCXReq", "Mul", req, reply, make(chan *client.Call, 1))
	}
}

func startRpcxServer() {
	rpcxOnec.Do(func() {
		go func() {
			s := server.NewServer()
			_ = s.Register(new(RPCXReq), "")
			_ = s.Serve("tcp", rpcxAddr)
		}()

		time.Sleep(100 * time.Millisecond)
	})
}
