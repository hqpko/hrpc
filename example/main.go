package main

import (
	"fmt"
	"log"
	"time"

	"github.com/hqpko/hrpc"
	"github.com/hqpko/hrpc/example/proto"
)

const rpcAddr = "127.0.0.1:9001"

func main() {
	go serverListen()
	time.Sleep(time.Second)

	client, _ := hrpc.Connect(rpcAddr)
	go client.Run()

	resp := &proto.Resp{}
	err := client.Call(1, &proto.Req{A: 1}, resp)
	fmt.Println(resp.B, err) // 2, nil

	err = client.Call(2, &proto.Req{A: 1}) // server log req.A = 1
	fmt.Println(err)                       // nil
}

func serverListen() {
	_ = hrpc.Listen(rpcAddr, func(s *hrpc.Stream) {
		s.Register(1, func(args *proto.Req, reply *proto.Resp) error {
			reply.B = args.A + 1
			return nil
		})
		s.Register(2, func(args *proto.Req) {
			log.Println(args.A)
		})
		go s.Run()
	})
}
