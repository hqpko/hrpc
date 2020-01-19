package hrpc

import (
	"reflect"
	"sync"
	"time"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
	"github.com/hqpko/hutils"
)

type methodInfo struct {
	oneWay        bool
	handler       func(seq uint64, args []byte)
	handlerOneWay func(args []byte)
	method        reflect.Value
	args          reflect.Type
	reply         reflect.Type
}

func (m *methodInfo) isOneWay() bool {
	return m.reply == nil
}

type timeoutTicker struct {
	ticker *time.Ticker
	step   time.Duration
	f      func()
	close  chan bool
}

func newTimeoutTicker(step time.Duration, f func()) *timeoutTicker {
	return &timeoutTicker{step: step, f: f, close: make(chan bool, 1)}
}

func (tt *timeoutTicker) start() {
	tt.ticker = time.NewTicker(tt.step)
	for {
		select {
		case <-tt.ticker.C:
			tt.f()
		case <-tt.close:
			return
		}
	}
}

func (tt *timeoutTicker) stop() {
	if tt.ticker != nil {
		tt.ticker.Stop()
	}
	tt.close <- true
}

type Server struct {
	lock       sync.RWMutex
	bufferPool *hutils.BufferPool
	socket     *hnet.Socket
	protocols  sync.Map
}

func NewServer(socket *hnet.Socket) *Server {
	return &Server{socket: socket, bufferPool: hutils.NewBufferPool()}
}

func (s *Server) Register(pid int32, handler func(seq uint64, args []byte)) {
	s.protocols.Store(pid, &methodInfo{oneWay: false, handler: handler})
}

func (s *Server) RegisterOneWay(pid int32, handler func(args []byte)) {
	s.protocols.Store(pid, &methodInfo{oneWay: true, handlerOneWay: handler})
}

func (s *Server) Run() error {
	return s.socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		pid, _ := buffer.ReadInt32()
		if mi := s.getHandler(pid); mi != nil {
			if mi.oneWay {
				mi.handlerOneWay(buffer.CopyRestOfBytes())
			} else {
				seq, _ := buffer.ReadUint64()
				mi.handler(seq, buffer.CopyRestOfBytes())
			}
		}
		s.bufferPool.Put(buffer)
	}, s.bufferPool.Get)
}

func (s *Server) getHandler(pid int32) *methodInfo {
	if value, ok := s.protocols.Load(pid); ok {
		return value.(*methodInfo)
	}
	return nil
}

func (s *Server) OneWay(pid int32, args []byte) error {
	buf := s.bufferPool.Get()
	defer s.bufferPool.Put(buf)
	s.fillGo(buf, pid, args)
	return s.socket.WriteBuffer(buf)
}

func (s *Server) Reply(seq uint64, reply []byte) error {
	buf := s.bufferPool.Get()
	defer s.bufferPool.Put(buf)
	s.fillReply(buf, seq, reply)
	return s.socket.WriteBuffer(buf)
}

func (s *Server) fillGo(buffer *hbuffer.Buffer, pid int32, args []byte) {
	buffer.WriteEndianUint32(0)
	buffer.WriteBool(false) // not reply
	buffer.WriteInt32(pid)
	buffer.WriteBytes(args)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
}

func (s *Server) fillReply(buffer *hbuffer.Buffer, seq uint64, reply []byte) {
	buffer.WriteEndianUint32(0)
	buffer.WriteBool(true) // is reply
	buffer.WriteUint64(seq)
	buffer.WriteBytes(reply)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
}
