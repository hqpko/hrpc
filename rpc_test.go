package hrpc

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/hqpko/hnet"
)

func TestRPC(t *testing.T) {
	addr := "localhost:9919"
	data := []byte{1}
	var server *Server
	go hnet.ListenSocket(addr, func(socket *hnet.Socket) {
		server = NewServer(socket)
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

	socket, _ := hnet.ConnectSocket(addr)
	client := NewClient(socket).SetCallTimeout(time.Second)
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
	w := &sync.WaitGroup{}
	w.Add(2)
	go testCallTimeout(w, client, data, t)
	go testCallTimeout(w, client, data, t)
	w.Wait()

	if _, err := client.Call(2, data); err != ErrCallTimeout {
		t.Errorf("Client.Call timeout fail")
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

func testCallTimeout(w *sync.WaitGroup, client *Client, data []byte, t *testing.T) {
	if _, err := client.Call(2, data); err != ErrCallTimeout {
		t.Errorf("Client.Call timeout fail")
	}
	w.Done()
}
