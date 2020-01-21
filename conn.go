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

func newConn() *conn {
	return &conn{readBuffer: hbuffer.NewBuffer(), writeBuffer: hbuffer.NewBuffer()}
}

func (c *conn) SetSocket(socket *hnet.Socket) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.socket = socket
}

func (c *conn) SetHandlerOneWay(handler func(pid int32, args []byte)) {
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
	buffer.Reset()
	buffer.WriteEndianUint32(0)
	buffer.WriteBool(true) // is call
	buffer.WriteUint64(seq)
	buffer.WriteBytes(reply)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
}

func (c *conn) fillOneWay(buffer *hbuffer.Buffer, pid int32, args []byte) {
	buffer.Reset()
	buffer.WriteEndianUint32(0)
	buffer.WriteBool(false) // not call
	buffer.WriteInt32(pid)
	buffer.WriteBytes(args)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
}

func (c *conn) fillCall(buffer *hbuffer.Buffer, pid int32, seq uint64, args []byte) {
	buffer.Reset()
	buffer.WriteEndianUint32(0)
	buffer.WriteBool(true) // is call
	buffer.WriteInt32(pid)
	buffer.WriteUint64(seq)
	buffer.WriteBytes(args)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
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
