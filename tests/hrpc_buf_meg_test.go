package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/hqpko/hnet"
	"github.com/hqpko/hrpc"
)

var (
	hrpcBufMsgAddr = "127.0.0.1:12008"
	hrpcBufMsgOnce = new(sync.Once)
)

func Test_hrpc_bufMsg(t *testing.T) {
	startHRpcBufMsgServer()

	client, err := hrpc.ConnectBufMsg("tcp", hrpcBufMsgAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reply := &BufResp{}
	e := client.Call(1, &BufReq{A: 1}, reply)
	if e != nil {
		t.Fatal(e)
	}
	if reply.B != 2 {
		t.Fatal("call error")
	}
}

func Benchmark_hrpc_bufMsg_Call(b *testing.B) {
	startHRpcBufMsgServer()

	client, err := hrpc.ConnectBufMsg("tcp", hrpcBufMsgAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	b.StartTimer()
	defer b.StopTimer()
	reply := &BufResp{}
	req := &BufReq{A: 1}
	for i := 0; i < b.N; i++ {
		if err := client.Call(1, req, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_hrpc_bufMsg_Go(b *testing.B) {
	startHRpcBufMsgServer()

	client, err := hrpc.ConnectBufMsg("tcp", hrpcBufMsgAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	b.StartTimer()
	defer b.StopTimer()
	reply := &BufResp{}
	req := &BufReq{A: 1}
	for i := 0; i < b.N; i++ {
		_ = client.Go(1, req, reply, false)
	}
}

func startHRpcBufMsgServer() {
	hrpcBufMsgOnce.Do(func() {
		go func() {
			_ = hnet.ListenSocket("tcp", hrpcBufMsgAddr, func(socket *hnet.Socket) {
				s := hrpc.NewServerBufMsg()
				s.Register(1, func(args *BufReq, reply *BufResp) error {
					reply.B = args.A + 1
					return nil
				})
				go func() {
					_ = s.Listen(socket)
				}()
			}, hnet.NewOption())
		}()
		time.Sleep(100 * time.Millisecond)
	})
}
