package hrpc

import (
	"sync"
	"sync/atomic"

	"github.com/hqpko/hbuffer"

	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
)

const (
	defChannelSize = 1 << 6
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
	buffer := hbuffer.NewBuffer()
	c := &Client{pending: sync.Map{}, socket: socket, buffer: buffer, enc: newPbEncoder(), dec: newPbDecoder(buffer)}
	c.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, c.handlerSend)
	c.sendChannel.Start()
	return c
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
		b, e := c.enc.encode(call.args)
		if call.doneIfErr(e) {
			return nil
		}
		if call.doneIfErr(c.socket.WritePacket(b)) {
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
