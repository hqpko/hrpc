package hrpc

import (
	"sync"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
)

type conn struct {
	lock          sync.RWMutex
	socket        *hnet.Socket
	readBuffer    *hbuffer.Buffer
	writeBuffer   *hbuffer.Buffer
	handlerCall   func(pid int32, seq uint64, args []byte)
	handlerOneWay func(pid int32, args []byte)
}

func newConn(socket *hnet.Socket) *conn {
	return &conn{socket: socket, readBuffer: hbuffer.NewBuffer(), writeBuffer: hbuffer.NewBuffer()}
}

func (c *conn) setHandlerOneWay(handler func(pid int32, args []byte)) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.handlerOneWay = handler
}

func (c *conn) setHandlerCall(handler func(pid int32, seq uint64, args []byte)) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.handlerCall = handler
}

func (c *conn) run(f func(buffer *hbuffer.Buffer)) error {
	return c.socket.ReadBuffer(f, func() *hbuffer.Buffer {
		c.readBuffer.Reset()
		return c.readBuffer
	})
}

func (c *conn) OneWay(pid int32, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.fillOneWay(c.writeBuffer, pid, args)
	return c.socket.WriteBuffer(c.writeBuffer)
}

func (c *conn) call(pid int32, seq uint64, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.fillCall(c.writeBuffer, pid, seq, args)
	return c.socket.WriteBuffer(c.writeBuffer)
}

func (c *conn) reply(seq uint64, reply []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.fillReply(c.writeBuffer, seq, reply)
	return c.socket.WriteBuffer(c.writeBuffer)
}

func (c *conn) fillReply(buffer *hbuffer.Buffer, seq uint64, reply []byte) {
	buffer.Reset().
		WriteEndianUint32(0).
		WriteBool(true). // is call
		WriteUint64(seq).
		WriteBytes(reply).
		UpdateHead()
}

func (c *conn) fillOneWay(buffer *hbuffer.Buffer, pid int32, args []byte) {
	buffer.Reset().
		WriteEndianUint32(0).
		WriteBool(false). // not call
		WriteInt32(pid).
		WriteBytes(args).
		UpdateHead()
}

func (c *conn) fillCall(buffer *hbuffer.Buffer, pid int32, seq uint64, args []byte) {
	buffer.Reset().
		WriteEndianUint32(0).
		WriteBool(true). // is call
		WriteInt32(pid).
		WriteUint64(seq).
		WriteBytes(args).
		UpdateHead()
}

func (c *conn) Close() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket != nil {
		err = c.socket.Close()
		c.socket = nil
	}

	return
}
