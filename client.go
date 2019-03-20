package hrpc

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
)

type Call struct {
	seq        uint64
	protocolID int32
	args       interface{}
	reply      interface{}
	error      error
	done       chan *Call
}

func (c *Call) isOneWay() bool {
	return c.reply == nil
}

func (c *Call) doneIfErr(e error) bool {
	if e != nil {
		c.error = e
		if c.done != nil {
			c.done <- c
		}
		return true
	}
	return false
}

func (c *Call) Done() *Call {
	if c.done == nil {
		return c
	}
	return <-c.done
}

func (c *Call) Error() error {
	return c.error
}

type Client struct {
	seq     uint64
	lock    *sync.Mutex
	pending map[uint64]*Call
	enc     HandlerEncode
	dec     HandlerDecode

	readBuffer  *hbuffer.Buffer
	writeBuffer *hbuffer.Buffer
	socket      *hnet.Socket
	sendChannel *hconcurrent.Concurrent
}

func Connect(network, addr string, option Option) (*Client, error) {
	if s, e := hnet.ConnectSocket(network, addr, hnet.DefaultOption); e != nil {
		return nil, e
	} else {
		return NewClient(s, option), nil
	}
}

func NewClient(socket *hnet.Socket, option Option) *Client {
	c := &Client{lock: new(sync.Mutex), pending: map[uint64]*Call{}, socket: socket, readBuffer: hbuffer.NewBuffer(), writeBuffer: hbuffer.NewBuffer(), enc: option.HandlerEncode, dec: option.HandlerDecode}
	c.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, c.handlerSend)
	c.sendChannel.Start()

	go c.read()
	return c
}

func (c *Client) read() {
	_ = c.socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		seq, err := buffer.ReadUint64()
		if err != nil {
			return
		}
		call, ok := c.getAndRemoveCall(seq)
		if !ok {
			return
		}
		errMsg, err := buffer.ReadString()
		if err != nil {
			return
		}
		if errMsg != "" {
			call.doneIfErr(errors.New(errMsg))
			return
		}

		err = c.dec(buffer.GetRestOfBytes(), call.reply)
		if !call.doneIfErr(err) {
			call.done <- call
		}
	}, func() *hbuffer.Buffer {
		return c.readBuffer
	})
}

func (c *Client) handlerSend(i interface{}) interface{} {
	if call, ok := i.(*Call); ok {
		c.writeBuffer.Reset()
		b, err := c.enc(call.args)
		if call.doneIfErr(err) {
			return nil
		}
		c.writeBuffer.WriteInt32(call.protocolID)
		if !call.isOneWay() {
			c.setCall(call)
			c.writeBuffer.WriteUint64(call.seq)
		}
		c.writeBuffer.WriteBytes(b)
		err = c.socket.WritePacket(c.writeBuffer.GetBytes())
		if call.doneIfErr(err) {
			return nil
		}
	}
	return nil
}

func (c *Client) Go(protocolID int32, args interface{}, replies ...interface{}) *Call {
	var reply interface{}
	if len(replies) > 0 {
		reply = replies[0]
	}
	call := c.getCall(protocolID, args, reply)
	c.sendChannel.MustInput(call)
	return call
}

func (c *Client) Call(protocolID int32, args interface{}, replies ...interface{}) error {
	return c.Go(protocolID, args, replies...).Done().error
}

func (c *Client) OneWay(protocolID int32, args interface{}) error {
	return c.Go(protocolID, args, nil).Done().error
}

func (c *Client) Close() error {
	c.sendChannel.Stop()
	return c.socket.Close()
}

func (c *Client) setCall(call *Call) {
	c.lock.Lock()
	c.pending[call.seq] = call
	c.lock.Unlock()
}

func (c *Client) getAndRemoveCall(seq uint64) (call *Call, ok bool) {
	c.lock.Lock()
	call, ok = c.pending[seq]
	if ok {
		delete(c.pending, seq)
	}
	c.lock.Unlock()
	return
}

func (c *Client) getCall(protocolID int32, args, reply interface{}) *Call {
	call := &Call{
		protocolID: protocolID,
		args:       args,
		reply:      reply,
	}
	// if one way,no seq,done chan
	if reply != nil {
		call.seq = atomic.AddUint64(&c.seq, 1)
		call.done = make(chan *Call, 1)
	}
	return call
}
