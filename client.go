package hrpc

import (
	"sync"
	"sync/atomic"

	"github.com/hqpko/hbuffer"

	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
)

const (
	defChannelSize      = 1 << 6
	defReadChannelCount = 1 << 4
)

type Call struct {
	seq        uint64
	protocolID int32
	args       interface{}
	reply      interface{}
	oneWay     bool
	error      error
	done       chan *Call
}

func (c *Call) doneIfErr(e error) bool {
	if e != nil {
		c.error = e
		c.done <- c
		return true
	}
	return false
}

func (c *Call) Done() *Call {
	return <-c.done
}

func (c *Call) Error() error {
	return c.error
}

type Client struct {
	seq     uint64
	pending sync.Map
	enc     encoder // gob,pb
	dec     decoder // gob,pb

	readBuffer  *hbuffer.Buffer
	buffer      *hbuffer.Buffer
	socket      *hnet.Socket
	sendChannel *hconcurrent.Concurrent
}

func Connect(network, addr string) (*Client, error) {
	if s, e := hnet.ConnectSocket(network, addr, hnet.NewOption()); e != nil {
		return nil, e
	} else {
		return NewClient(s), nil
	}
}

func NewClient(socket *hnet.Socket) *Client {
	c := &Client{pending: sync.Map{}, socket: socket, readBuffer: hbuffer.NewBuffer(), buffer: hbuffer.NewBuffer(), enc: newPbEncoder(), dec: newPbDecoder()}
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
		callI, ok := c.pending.Load(seq)
		if !ok {
			return
		}
		call := callI.(*Call)
		c.pending.Delete(seq)

		err = c.dec.decode(buffer.GetRestOfBytes(), call.reply)
		if !call.doneIfErr(err) {
			call.done <- call
		}
	}, func() *hbuffer.Buffer {
		return c.readBuffer
	})
}

//
//func NewClientGob(socket *hnet.Socket) *Client {
//
//}
//
//func newClient(socket *hnet.Socket, enc encoder, dec decoder) *Client {
//
//}

func (c *Client) handlerSend(i interface{}) interface{} {
	if call, ok := i.(*Call); ok {
		c.buffer.Reset()
		b, e := c.enc.encode(call.args)
		if call.doneIfErr(e) {
			return nil
		}
		c.buffer.WriteInt32(call.protocolID)
		c.buffer.WriteUint64(call.seq)
		c.buffer.WriteBytes(b)
		if call.doneIfErr(c.socket.WritePacket(c.buffer.GetBytes())) {
			return nil
		}
		if !call.oneWay {
			c.pending.Store(call.seq, call)
		}
	}
	return nil
}

func (c *Client) Go(protocolID int32, args, reply interface{}, oneWay bool) *Call {
	call := &Call{
		seq:        atomic.AddUint64(&c.seq, 1),
		protocolID: protocolID,
		args:       args,
		reply:      reply,
		oneWay:     oneWay,
		done:       make(chan *Call, 1),
	}
	c.sendChannel.MustInput(call)
	return call
}

func (c *Client) Call(protocolID int32, args interface{}, reply interface{}) error {
	return c.Go(protocolID, args, reply, false).Done().error
}

func (c *Client) OneWay(protocolID int32, args interface{}) error {
	return c.Go(protocolID, args, nil, true).Done().error
}

func (c *Client) Close() {
	c.sendChannel.Stop()
}
