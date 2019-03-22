package hrpc

import (
	"errors"
	"sync"
	"time"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
	"github.com/hqpko/hpool"
	"github.com/hqpko/hticker"
)

type Call struct {
	seq          uint64
	protocolID   int32
	args         interface{}
	reply        interface{}
	error        error
	timeoutIndex int
	C            chan *Call
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
	case c.C <- c:
	default:

	}
}

func (c *Call) Done() *Call {
	return <-c.C
}

func (c *Call) Error() error {
	return c.error
}

type Client struct {
	seq        uint64
	lock       *sync.Mutex
	started    bool
	pending    map[uint64]*Call
	translator Translator
	bufferPool *hpool.BufferPool

	writeBuffer *hbuffer.Buffer
	socket      *hnet.Socket
	mainChannel *hconcurrent.Concurrent

	timeoutCall         time.Duration
	timeoutMaxDuration  time.Duration
	timeoutStepDuration time.Duration
	timeoutSlice        []map[uint64]*Call
	timeoutTickerGroup  *hticker.TickerGroup
	timeoutTickID       int
	timeoutIndex        int
	timeoutOffset       int
}

func NewClient() *Client {
	c := &Client{lock: new(sync.Mutex), pending: map[uint64]*Call{}, writeBuffer: hbuffer.NewBuffer(), translator: new(translatorProto), timeoutCall: defTimeoutCall, timeoutMaxDuration: defTimeoutMaxDuration, timeoutStepDuration: defTimeoutStepDuration}
	c.mainChannel = hconcurrent.NewConcurrent(defChannelSize, 1, c.handlerChannel)
	return c
}

func (c *Client) SetTranslator(translator Translator) *Client {
	c.translator = translator
	return c
}

func (c *Client) SetBufferPool(pool *hpool.BufferPool) *Client {
	c.bufferPool = pool
	return c
}

func (c *Client) SetTimeoutOption(timeoutCall, stepDuration, maxTimeoutDuration time.Duration) *Client {
	c.timeoutCall = timeoutCall
	c.timeoutStepDuration = stepDuration
	c.timeoutMaxDuration = maxTimeoutDuration
	return c
}

func (c *Client) Run(socket *hnet.Socket) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return
	}
	c.started = true
	c.socket = socket
	c.mainChannel.Start()

	c.initTimeout()

	go c.read()
}

func (c *Client) initTimeout() {
	stepCount := int(c.timeoutMaxDuration / c.timeoutStepDuration)
	c.timeoutSlice = make([]map[uint64]*Call, stepCount)
	for i := 0; i < stepCount; i++ {
		c.timeoutSlice[i] = map[uint64]*Call{}
	}

	c.timeoutTickerGroup = hticker.NewTickerGroup(c.timeoutStepDuration)
	c.timeoutTickID = c.timeoutTickerGroup.AddTicker(c.timeoutStepDuration, func() {
		c.mainChannel.MustInput(struct{}{})
	})

	c.timeoutOffset = int(c.timeoutCall / c.timeoutStepDuration)
	if c.timeoutOffset <= 0 {
		c.timeoutOffset = 1
	}
}

func (c *Client) read() {
	_ = c.socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		c.mainChannel.MustInput(buffer)
	}, c.getBuffer)
}

func (c *Client) handlerChannel(i interface{}) interface{} {
	if call, ok := i.(*Call); ok {
		c.handlerCall(call)
	} else if buffer, ok := i.(*hbuffer.Buffer); ok {
		c.handlerBuffer(buffer)
	} else {
		c.handlerTimeout()
	}
	return nil
}

func (c *Client) handlerCall(call *Call) {
	c.writeBuffer.Reset()
	b, err := c.translator.Marshal(call.args)
	if call.doneIfErr(err) {
		return
	}
	c.writeBuffer.WriteInt32(call.protocolID)
	isOneWay := call.isOneWay()
	if !isOneWay {
		c.seq++
		call.seq = c.seq
		c.cacheCall(call)
		c.writeBuffer.WriteUint64(call.seq)
	}
	c.writeBuffer.WriteBytes(b)
	err = c.socket.WritePacket(c.writeBuffer.GetBytes())
	if call.doneIfErr(err) {
		return
	}
	if isOneWay {
		call.done()
	}
}

func (c *Client) handlerBuffer(buffer *hbuffer.Buffer) {
	seq, err := buffer.ReadUint64()
	if err != nil {
		return
	}
	call, ok := c.removeCall(seq)
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

	err = c.translator.Unmarshal(buffer.GetRestOfBytes(), call.reply)
	if !call.doneIfErr(err) {
		call.done()
	}
	c.putBuffer(buffer)
}

func (c *Client) handlerTimeout() {
	currentCallMap := c.timeoutSlice[c.timeoutIndex]
	for _, call := range currentCallMap {
		call.doneIfErr(ErrCallTimeout)
		delete(c.pending, call.seq)
		delete(currentCallMap, call.seq)
	}

	c.timeoutIndex++
	c.timeoutIndex = c.timeoutIndex % len(c.timeoutSlice)
}

func (c *Client) Go(protocolID int32, args interface{}, replies ...interface{}) *Call {
	var reply interface{}
	if len(replies) > 0 {
		reply = replies[0]
	}
	call := c.newCall(protocolID, args, reply)
	c.mainChannel.MustInput(call)
	return call
}

func (c *Client) Call(protocolID int32, args interface{}, replies ...interface{}) error {
	return c.Go(protocolID, args, replies...).Done().error
}

func (c *Client) OneWay(protocolID int32, args interface{}) error {
	return c.Go(protocolID, args, nil).Done().error
}

func (c *Client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		c.started = false
		c.mainChannel.Stop()
		c.timeoutTickerGroup.Stop()
		return c.socket.Close()
	}
	return nil
}

func (c *Client) cacheCall(call *Call) {
	c.pending[call.seq] = call
	call.timeoutIndex = (c.timeoutOffset + c.timeoutIndex) % len(c.timeoutSlice)
	c.timeoutSlice[call.timeoutIndex][call.seq] = call
}

func (c *Client) removeCall(seq uint64) (*Call, bool) {
	call, ok := c.pending[seq]
	if ok {
		delete(c.pending, seq)
		delete(c.timeoutSlice[call.timeoutIndex], seq)
	}
	return call, ok
}

func (c *Client) newCall(protocolID int32, args, reply interface{}) *Call {
	call := &Call{
		protocolID: protocolID,
		args:       args,
		reply:      reply,
		C:          make(chan *Call, 1),
	}
	return call
}

func (c *Client) getBuffer() *hbuffer.Buffer {
	if c.bufferPool != nil {
		return c.bufferPool.Get()
	}
	return hbuffer.NewBuffer()
}

func (c *Client) putBuffer(buffer *hbuffer.Buffer) {
	if c.bufferPool != nil {
		c.bufferPool.Put(buffer)
	}
}
