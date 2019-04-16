package hrpc

import (
	"time"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
	"github.com/hqpko/hutils"
)

type Stream struct {
	bufferPool *hutils.BufferPool
	client     *client
	server     *server
}

func NewStream() *Stream {
	bufferPool := hutils.NewBufferPool()
	s := &Stream{bufferPool: bufferPool, client: newClient(bufferPool), server: newServer(bufferPool)}
	trans := NewTranslatorProto()
	s.client.setTranslator(trans)
	s.server.setTranslator(trans)
	return s
}

func (s *Stream) SetTranslator(trans Translator) *Stream {
	s.client.setTranslator(trans)
	s.server.setTranslator(trans)
	return s
}

func (s *Stream) SetTimeoutOption(timeoutCall, stepDuration, maxTimeoutDuration time.Duration) *Stream {
	s.client.setTimeoutOption(timeoutCall, stepDuration, maxTimeoutDuration)
	return s
}

func (s *Stream) Call(pid int32, arg interface{}, reply ...interface{}) error {
	return s.Go(pid, arg, reply...).Done()
}

func (s *Stream) Go(pid int32, arg interface{}, reply ...interface{}) *Call {
	return s.client.call(pid, arg, reply...)
}

func (s *Stream) Register(pid int32, handler interface{}) {
	s.server.register(pid, handler)
}

func (s *Stream) Run(socket *hnet.Socket) error {
	s.client.run(socket)
	s.server.run(socket)
	err := socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		callType, _ := buffer.ReadByte()
		if callType == callTypeRequest {
			s.server.readChannel.MustInput(buffer)
		} else {
			s.client.mainChannel.MustInput(buffer)
		}
	}, s.bufferPool.Get)

	if err != nil {
		s.client.mainChannel.MustInput(err)
	}
	return err
}

func (s *Stream) Close() error {
	s.server.close()
	return s.client.close()
}
