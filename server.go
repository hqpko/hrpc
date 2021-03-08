package hrpc

import (
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
	s.setHandlerCall(handler)
	return s
}

func (s *Server) Run() error {
	return s.run()
}

func (s *Server) OneWay(pid int32, args []byte) error {
	return s.oneWay(pid, args)
}

func (s *Server) Reply(seq uint64, reply []byte) error {
	return s.reply(seq, reply)
}

func (s *Server) Close() error {
	return s.close()
}
