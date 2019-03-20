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
	doneChan   chan *Call
}

func (c *Call) isOneWay() bool {
	return c.reply == nil
}

func (c *Call) doneIfErr(e error) bool {
	if e != nil {
		c.error = e
		c.done()
		return true
	}
	return false
}

func (c *Call) done() {
	select {
	case c.doneChan <- c:
	default:

	}
}

func (c *Call) Done() *Call {
	return <-c.doneChan
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
	started bool

	readBuffer  *hbuffer.Buffer
	writeBuffer *hbuffer.Buffer
	socket      *hnet.Socket
	sendChannel *hconcurrent.Concurrent
}

func NewClient() *Client {
	c := &Client{lock: new(sync.Mutex), pending: map[uint64]*Call{}, readBuffer: hbuffer.NewBuffer(), writeBuffer: hbuffer.NewBuffer(), enc: handlerPbEncode, dec: handlerPbDecode}
	c.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, c.handlerSend)
	c.sendChannel.Start()

	return c
}

func (c *Client) SetEncoder(enc HandlerEncode) {
	c.enc = enc
}

func (c *Client) SetDecoder(dec HandlerDecode) {
	c.dec = dec
}

func (c *Client) SetSocket(socket *hnet.Socket) {
	c.socket = socket
}

func (c *Client) Connect(network, addr string) error {
	if socket, err := hnet.ConnectSocket(network, addr); err != nil {
		return err
	} else {
		c.socket = socket
	}
	return nil
}

func (c *Client) Start() {
	c.lock.Lock()
	if c.started {
		c.lock.Unlock()
		return
	}
	c.started = true
	c.lock.Unlock()
	go c.read()
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
			call.done()
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
		isOneWay := call.isOneWay()
		if !isOneWay {
			c.setCall(call)
			c.writeBuffer.WriteUint64(call.seq)
		}
		c.writeBuffer.WriteBytes(b)
		err = c.socket.WritePacket(c.writeBuffer.GetBytes())
		if call.doneIfErr(err) {
			return nil
		}
		if isOneWay {
			call.done()
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
		doneChan:   make(chan *Call, 1),
	}
	// if one way,no seq,done chan
	if reply != nil {
		call.seq = atomic.AddUint64(&c.seq, 1)
	}
	return call
}
