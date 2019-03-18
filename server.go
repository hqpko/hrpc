package hrpc

import (
	"fmt"
	"net/rpc"
	"reflect"

	"github.com/hqpko/hbuffer"

	"github.com/hqpko/hnet"
)

type Server struct {
	rpc.Call
	dec       decoder
	socket    *hnet.Socket
	protocols map[int32]reflect.Value
}

func NewServer() *Server {
	return &Server{protocols: map[int32]reflect.Value{}}
}

func (s *Server) Register(protocolID int32, handler func()) {
	if _, ok := s.protocols[protocolID]; ok {
		panic(fmt.Sprintf("register protocol error,id exist:%d", protocolID))
	}
	s.protocols[protocolID] = reflect.ValueOf(handler)
}

func (s *Server) Listen(socket *hnet.Socket) error {
	buffer := hbuffer.NewBuffer()
	return socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		if pid, err := buffer.ReadInt32(); err == nil {
			if f, ok := s.protocols[pid]; ok {
			}
		}

	}, func() *hbuffer.Buffer {
		return buffer
	})
}
