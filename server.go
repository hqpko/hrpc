package hrpc

import (
	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
)

type Server struct {
	*conn
}

func NewServer(socket *hnet.Socket) *Server {
	return &Server{conn: newConn(socket)}
}

func (s *Server) SetHandlerOneWay(handler func(pid int32, args []byte)) *Server {
	s.setHandlerOneWay(handler)
	return s
}

func (s *Server) SetHandlerCall(handler func(pid int32, seq uint64, args []byte)) *Server {
	s.handlerCall = handler
	return s
}

func (s *Server) Run() error {
	return s.run(func(buffer *hbuffer.Buffer) {
		msgType, _ := buffer.ReadByte()
		pid, _ := buffer.ReadInt32()
		if msgType == msgTypeCall {
			seq, _ := buffer.ReadUint64()
			s.handlerCall(pid, seq, buffer.CopyRestOfBytes())
		} else if msgType == msgTypeOneWay {
			s.handlerOneWay(pid, buffer.CopyRestOfBytes())
		}
	})
}

func (s *Server) Reply(seq uint64, reply []byte) error {
	return s.reply(seq, reply)
}
