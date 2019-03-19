package hrpc

import (
	"log"
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
	if s, e := hnet.ConnectSocket(network, addr, hnet.NewOption()); e != nil {
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
			log.Printf("no call with seq:%d", seq)
			return
		}

		err = c.dec(buffer.GetRestOfBytes(), call.reply)
		if !call.doneIfErr(err) {
			call.done <- call
		} else {
			log.Println(err)
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
			log.Println(err)
			return nil
		}
		c.writeBuffer.WriteInt32(call.protocolID)
		c.writeBuffer.WriteBool(call.oneWay)
		if !call.oneWay {
			c.setCall(call)
			c.writeBuffer.WriteUint64(call.seq)
		}
		c.writeBuffer.WriteBytes(b)
		err = c.socket.WritePacket(c.writeBuffer.GetBytes())
		if call.doneIfErr(err) {
			log.Println(err)
			return nil
		}
	}
	return nil
}

func (c *Client) Go(protocolID int32, args, reply interface{}, oneWay bool) *Call {
	call := c.getCall(protocolID, args, reply, oneWay)
	c.sendChannel.MustInput(call)
	return call
}

func (c *Client) Call(protocolID int32, args interface{}, reply interface{}) error {
	return c.Go(protocolID, args, reply, false).Done().error
}

func (c *Client) OneWay(protocolID int32, args interface{}) error {
	return c.Go(protocolID, args, nil, true).Done().error
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

func (c *Client) getCall(protocolID int32, args, reply interface{}, oneWay bool) *Call {
	call := &Call{
		protocolID: protocolID,
		args:       args,
		reply:      reply,
		oneWay:     oneWay,
		done:       make(chan *Call, 1),
	}

	// get *Call from pool
	// call := callPool.Get().(*Call)
	// call.protocolID = protocolID
	// call.error = nil
	// call.args = args
	// call.reply = reply
	// call.oneWay = oneWay

	if !oneWay {
		call.seq = atomic.AddUint64(&c.seq, 1)
	}
	return call
}
