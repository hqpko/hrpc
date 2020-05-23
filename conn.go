package hrpc

import (
	"sync"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
)

const (
	msgTypeUnknown = iota
	msgTypeCall
	msgTypeOneWay
	msgTypeReply
)

type conn struct {
	lock          sync.RWMutex
	socket        *hnet.Socket
	pending       *pending
	readBuffer    *hbuffer.Buffer
	writeBuffer   *hbuffer.Buffer
	handlerCall   func(pid int32, seq uint64, args []byte)
	handlerOneWay func(pid int32, args []byte)
}

func newConn(socket *hnet.Socket) *conn {
	return &conn{socket: socket, pending: newPending(), readBuffer: hbuffer.NewBuffer(), writeBuffer: hbuffer.NewBuffer()}
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

func (c *conn) run() error {
	return c.socket.ReadPacket(func(packet []byte) {
		c.readBuffer.Reset().SetBytes(packet)
		msgType, _ := c.readBuffer.ReadByte()
		switch msgType {
		case msgTypeOneWay:
			pid, _ := c.readBuffer.ReadInt32()
			c.handlerOneWay(pid, c.readBuffer.GetRestOfBytes())
		case msgTypeCall:
			pid, _ := c.readBuffer.ReadInt32()
			seq, _ := c.readBuffer.ReadUint64()
			c.handlerCall(pid, seq, c.readBuffer.GetRestOfBytes())
		case msgTypeReply:
			seq, _ := c.readBuffer.ReadUint64()
			c.pending.reply(seq, c.readBuffer.GetRestOfBytes())
		}
	})
}

func (c *conn) oneWay(pid int32, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.socket.WriteBuffer(c.fillOneWay(pid, args))
}

func (c *conn) call(pid int32, args []byte) ([]byte, error) {
	call, seq := c.pending.new()
	if err := c.tryCall(pid, seq, args); err != nil {
		c.pending.error(seq, err)
	}
	return call.Done()
}

func (c *conn) tryCall(pid int32, seq uint64, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.socket.WriteBuffer(c.fillCall(pid, seq, args))
}

func (c *conn) reply(seq uint64, reply []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.socket.WriteBuffer(c.fillReply(seq, reply))
}

func (c *conn) fillReply(seq uint64, reply []byte) *hbuffer.Buffer {
	return c.writeBuffer.Reset().
		WriteEndianUint32(0).
		WriteByte(msgTypeReply).
		WriteUint64(seq).
		WriteBytes(reply).
		UpdateHead()
}

func (c *conn) fillOneWay(pid int32, args []byte) *hbuffer.Buffer {
	return c.writeBuffer.Reset().
		WriteEndianUint32(0).
		WriteByte(msgTypeOneWay).
		WriteInt32(pid).
		WriteBytes(args).
		UpdateHead()
}

func (c *conn) fillCall(pid int32, seq uint64, args []byte) *hbuffer.Buffer {
	return c.writeBuffer.Reset().
		WriteEndianUint32(0).
		WriteByte(msgTypeCall).
		WriteInt32(pid).
		WriteUint64(seq).
		WriteBytes(args).
		UpdateHead()
}

func (c *conn) close() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket != nil {
		err = c.socket.Close()
		c.socket = nil
	}
	return
}
