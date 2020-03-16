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
		server = NewServer()
		server.SetSocket(socket)
		server.SetHandlerCall(func(pid int32, seq uint64, args []byte) {
			switch pid {
			case 1:
				_ = server.Reply(seq, args)
			case 2:
				// do nothing
			}
		})
		server.SetHandlerOneWay(func(pid int32, args []byte) {
			if pid != 3 || !bytes.Equal(data, args) {
				t.Errorf("server receive data error")
			}
		})
		_ = server.Run()
	})

	socket, _ := hnet.ConnectSocket("tcp", addr)
	client := NewClientWithOption(time.Second)
	client.SetSocket(socket)
	client.SetHandlerOneWay(func(pid int32, args []byte) {
		if pid != 4 || !bytes.Equal(data, args) {
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
	if _, err := client.Call(2, data); err != Timeout {
		t.Errorf("Client.Call timeout fail")
	}

	// Client.Go
	if call := client.Go(1, data); call.err != nil {
		t.Errorf("Client.Go error:%s", call.err.Error())
	} else if reply, err := call.Done(); err != nil {
		t.Errorf("Client.Go fail %v", err)
	} else if !bytes.Equal(reply, data) {
		t.Errorf("Client.Go fail, reply:%v,shouldBe:%v\n", reply, data)
	}

	// Client.OneWay
	if err := client.OneWay(3, data); err != nil {
		t.Errorf("Client.OneWay error:%s", err.Error())
	}

	// Server.OneWay
	if err := server.OneWay(4, data); err != nil {
		t.Errorf("Server.OneWay error:%s", err.Error())
	}

	_ = client.Close()
	_ = server.Close()
}
