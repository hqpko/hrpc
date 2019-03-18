package hrpc

import (
	"sync"
	"sync/atomic"

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

func (c *Call) Done() *Call {
	return <-c.done
}

func (c *Call) Error() error {
	return c.error
}

type Client struct {
	seq     uint64
	pending sync.Map
	enc     interface{} // gob,pb
	dec     interface{} // gob,pb

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
	c := &Client{pending: sync.Map{}, socket: socket}
	c.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, c.handlerSend)
	c.sendChannel.Start()
	return c
}

func NewClientGob(socket *hnet.Socket)*Client{

}

func (c *Client) handlerSend(i interface{}) interface{} {
	if call, ok := i.(*Call); ok {
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

}
