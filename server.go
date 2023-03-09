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

func (s *Server) SetHandlerCall(handler func(pid int32, seq uint32, args []byte)) *Server {
	s.setHandlerCall(handler)
	return s
}

func (s *Server) Reply(seq uint32, reply []byte) error {
	return s.reply(seq, reply)
}

// Call 正常情况下不推荐使用，server 间互相 Call 容易导致死锁，仅在确定不会死锁的情况下使用
func (s *Server) Call(pid int32, args []byte) ([]byte, error) {
	return s.conn.Call(pid, args)
}
