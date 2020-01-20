package hrpc

import (
	"github.com/hqpko/hbuffer"
)

type Server struct {
	*conn
}

func NewServer() *Server {
	return &Server{conn: newConn()}
}

func (s *Server) SetHandlerCall(handler func(pid int32, seq uint64, args []byte)) {
	s.handlerCall = handler
}

func (s *Server) Run() error {
	return s.run(func(buffer *hbuffer.Buffer) {
		isCall, _ := buffer.ReadBool()
		pid, _ := buffer.ReadInt32()
		if isCall {
			seq, _ := buffer.ReadUint64()
			s.handlerCall(pid, seq, buffer.CopyRestOfBytes())
		} else {
			s.handlerOneWay(pid, buffer.CopyRestOfBytes())
		}
	})
}

func (s *Server) Reply(seq uint64, reply []byte) error {
	return s.reply(seq, reply)
}
