package hrpc

import (
	"bytes"
	"testing"
	"time"

	"github.com/hqpko/hnet"
)

func TestRPC(t *testing.T) {
	addr := "localhost:9919"
	data := []byte{1}
	var server *Server
	go hnet.ListenSocket("tcp", addr, func(socket *hnet.Socket) {
		server = NewServer(socket)
		server.Register(1, func(seq uint64, args []byte) {
			_ = server.Reply(seq, args)
		})
		server.RegisterOneWay(2, func(args []byte) {
			if !bytes.Equal(data, args) {
				t.Errorf("server receive data error")
			}
		})
		server.Register(3, func(seq uint64, args []byte) {
			// timeout
		})
		_ = server.Run()
	})

	socket, _ := hnet.ConnectSocket("tcp", addr)
	client := NewClientWithOption(socket, time.Millisecond*100, time.Second)
	client.RegisterOneWay(4, func(args []byte) {
		if !bytes.Equal(data, args) {
			t.Errorf("client receive oneWay fail.")
		}
	})
	go client.Run()

	// Client.Call
	if reply, err := client.Call(1, data); err != nil {
		t.Errorf("Client.Call error:%s", err.Error())
	} else if !bytes.Equal(data, reply) {
		t.Errorf("Client.Call fail")
	}

	// Client.Call timeout
	if _, err := client.Call(3, data); err != Timeout {
		t.Errorf("Client.Call timeout fail")
	}

	// Client.Go
	if call, err := client.Go(1, data); err != nil {
		t.Errorf("Client.Go error:%s", err.Error())
	} else if reply, _ := call.Done(); !bytes.Equal(data, reply) {
		t.Errorf("Client.Go fail")
	}

	// Client.OneWay
	if err := client.OneWay(2, data); err != nil {
		t.Errorf("Client.OneWay error:%s", err.Error())
	}

	// Server.OneWay
	if err := server.OneWay(4, data); err != nil {
		t.Errorf("Server.OneWay error:%s", err.Error())
	}
}
