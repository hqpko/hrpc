package hrpc

import (
	"bytes"
	"testing"

	"github.com/hqpko/hnet"
)

func TestRPC(t *testing.T) {
	addr := "localhost:9919"
	data := []byte{1}
	var server *Server
	go hnet.ListenSocket("tcp", addr, func(socket *hnet.Socket) {
		server = NewServer2(socket)
		server.Register(1, func(seq uint64, args []byte) {
			_ = server.Reply(seq, args)
		})
		server.RegisterOneWay(2, func(args []byte) {
			if !bytes.Equal(data, args) {
				t.Errorf("server receive data error")
			}
		})
		_ = server.Run()
	})

	socket, _ := hnet.ConnectSocket("tcp", addr)
	client := NewClient(socket)
	client.Register(3, func(args []byte) {
		if !bytes.Equal(data, args) {
			t.Errorf("client receive oneWay fail.")
		}
	})
	go client.Run()

	// client.Call
	if reply, err := client.Call(1, data); err != nil {
		t.Errorf("Client.Call error:%s", err.Error())
	} else if !bytes.Equal(data, reply) {
		t.Errorf("Client.Call fail.")
	}

	// client.Go
	if call, err := client.Go(1, data); err != nil {
		t.Errorf("Client.Go error:%s", err.Error())
	} else if reply := call.Done(); !bytes.Equal(data, reply) {
		t.Errorf("Client.Go fail.")
	}

	// client.OneWay
	if err := client.OneWay(2, data); err != nil {
		t.Errorf("Client.OneWay error:%s", err.Error())
	}

	// server.OneWay
	if err := server.OneWay(3, data); err != nil {
		t.Errorf("Server.OneWay error:%s", err.Error())
	}
}
