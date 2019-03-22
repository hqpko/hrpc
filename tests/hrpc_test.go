package tests

import (
	"log"
	"testing"
	"time"

	"github.com/hqpko/hnet"
	"github.com/hqpko/hrpc"
)

func TestHRpc(t *testing.T) {
	startHRpcServer()

	socket, _ := hnet.ConnectSocket("tcp", hrpcAddr)
	client := hrpc.NewClient().SetTimeoutOption(time.Second, time.Second, 10*time.Second)
	client.Run(socket)
	defer client.Close()

	if err := client.Call(2, &Req{A: 2}); err != nil {
		t.Fatal(err)
	}

	reply := &Resp{}
	e := client.Call(1, &Req{A: 1}, reply)
	if e != nil {
		t.Fatal(e)
	}
	if reply.B != 2 {
		t.Fatal("call error")
	}

	e = client.Call(3, &Req{A: 1}, reply)
	if e != hrpc.ErrCallTimeout {
		t.Error("call timeout fail")
	}
}

var hrpcAddr = "127.0.0.1:12003"

func startHRpcServer() {
	go func() {
		_ = hnet.ListenSocket("tcp", hrpcAddr, func(socket *hnet.Socket) {
			s := hrpc.NewServer()
			s.Register(1, func(args *Req, reply *Resp) error {
				reply.B = args.A + 1
				return nil
			})
			s.Register(2, func(args *Req) {
				log.Println(args.A)
			})
			s.Register(3, func(args *Req, reply *Resp) error {
				time.Sleep(3 * time.Second)
				reply.B = args.A + 1
				return nil
			})
			go func() {
				_ = s.Listen(socket)
			}()
		})
	}()
	time.Sleep(100 * time.Millisecond)
}
