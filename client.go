package hrpc

import (
	"log"
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
	buffer     *hbuffer.Buffer
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
	enc     encoder // gob,pb
	dec     decoder // gob,pb

	readBuffer  *hbuffer.Buffer
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

func ConnectBufMsg(network, addr string) (*Client, error) {
	if s, e := hnet.ConnectSocket(network, addr, hnet.NewOption()); e != nil {
		return nil, e
	} else {
		return NewClientBufMeg(s), nil
	}
}

func NewClient(socket *hnet.Socket) *Client {
	c := &Client{lock: new(sync.Mutex), pending: map[uint64]*Call{}, socket: socket, readBuffer: hbuffer.NewBuffer(), enc: newPbEncoder(), dec: newPbDecoder()}
	c.sendChannel = hconcurrent.NewConcurrentWithOptions(
		hconcurrent.NewOption(defChannelSize, defReadChannelCount, c.handlerEncode),
		hconcurrent.NewOption(defChannelSize, 1, c.handlerSend),
	)
	c.sendChannel.Start()

	go c.read()
	return c
}

func NewClientBufMeg(socket *hnet.Socket) *Client {
	c := &Client{lock: new(sync.Mutex), pending: map[uint64]*Call{}, socket: socket, readBuffer: hbuffer.NewBuffer(), enc: newBufEncoder(), dec: newBufDecoder()}
	c.sendChannel = hconcurrent.NewConcurrentWithOptions(
		hconcurrent.NewOption(defChannelSize, 1, c.handlerEncode),
		hconcurrent.NewOption(defChannelSize, 1, c.handlerSend),
	)
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

		err = c.dec.decode(buffer.GetRestOfBytes(), call.reply)
		if !call.doneIfErr(err) {
			call.done <- call
		} else {
			log.Println(err)
		}
	}, func() *hbuffer.Buffer {
		return c.readBuffer
	})
}

func (c *Client) handlerEncode(i interface{}) interface{} {
	if call, ok := i.(*Call); ok {
		b, err := c.enc.encode(call.args)
		if call.doneIfErr(err) {
			log.Println(err)
			return nil
		}
		call.buffer.WriteInt32(call.protocolID)
		call.buffer.WriteBool(call.oneWay)
		if !call.oneWay {
			c.setCall(call)
			call.buffer.WriteUint64(call.seq)
		}
		call.buffer.WriteBytes(b)
		return call
	}
	return nil
}

func (c *Client) handlerSend(i interface{}) interface{} {
	if call, ok := i.(*Call); ok {
		err := c.socket.WritePacket(call.buffer.GetBytes())
		if call.doneIfErr(err) {
			log.Println(err)
			return nil
		}
		bufferPool.Put(call.buffer)
	}
	return nil
}

func (c *Client) Go(protocolID int32, args, reply interface{}, oneWay bool) *Call {
	call := &Call{
		protocolID: protocolID,
		args:       args,
		reply:      reply,
		oneWay:     oneWay,
		buffer:     bufferPool.Get(),
		done:       make(chan *Call, 1),
	}
	if !oneWay {
		call.seq = atomic.AddUint64(&c.seq, 1)
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

func (c *Client) Seq() uint64 {
	return atomic.LoadUint64(&c.seq)
}
