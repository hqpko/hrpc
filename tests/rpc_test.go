package tests

import (
	"net/rpc"
	"testing"
	"time"

	"github.com/hqpko/hnet"
)

type RPCReq struct {
}

func (r *RPCReq) Add(req *Req, resp *Resp) error {
	resp.B = req.A + 1
	return nil
}

func BenchmarkRPC_Call(b *testing.B) {
	addr := testGetAddr()
	go func() {
		_ = hnet.ListenSocket("tcp", addr, func(socket *hnet.Socket) {
			server := rpc.NewServer()
			_ = server.Register(new(RPCReq))
			server.ServeConn(socket)
		}, hnet.NewOption())
	}()

	time.Sleep(100 * time.Millisecond)
	s, _ := hnet.ConnectSocket("tcp", addr, hnet.NewOption())
	client := rpc.NewClient(s)
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		resp := &Resp{}
		_ = client.Call("RPCReq.Add", &Req{A: 1}, resp)
	}
}

func BenchmarkRPC_Go(b *testing.B) {
	addr := testGetAddr()
	go func() {
		_ = hnet.ListenSocket("tcp", addr, func(socket *hnet.Socket) {
			server := rpc.NewServer()
			_ = server.Register(new(RPCReq))
			server.ServeConn(socket)
		}, hnet.NewOption())
	}()

	time.Sleep(100 * time.Millisecond)
	s, _ := hnet.ConnectSocket("tcp", addr, hnet.NewOption())
	client := rpc.NewClient(s)
	b.StartTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		resp := &Resp{}
		_ = client.Go("RPCReq.Add", &Req{A: 1}, resp, make(chan *rpc.Call, 1))
	}
}
