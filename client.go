package hrpc

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
	buf          *hbuffer.Buffer
	client       *Client
	c            chan *Call
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
	case c.c <- c:
	default:
		log.Println("hrpc: call done channel filled")
	}
}

func (c *Call) Done() *Call {
	<-c.c
	if c.buf != nil {
		c.client.unmarshalCall(c)
	}
	return c
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
	c := &Client{lock: new(sync.Mutex), pending: map[uint64]*Call{}, translator: new(translatorProto), timeoutCall: defTimeoutCall, timeoutMaxDuration: defTimeoutMaxDuration, timeoutStepDuration: defTimeoutStepDuration}
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

// SetTimeoutOption
// timeoutCall			: duration of rpc all timeout
// stepDuration			: step duration of checking timeout
// maxTimeoutDuration	: max duration of rpc all timeout
func (c *Client) SetTimeoutOption(timeoutCall, stepDuration, maxTimeoutDuration time.Duration) *Client {
	if maxTimeoutDuration <= timeoutCall || stepDuration >= maxTimeoutDuration {
		panic("hrpc: client setting timeout option error")
	}
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

	go c.readSocket()
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

func (c *Client) readSocket() {
	err := c.socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		c.mainChannel.MustInput(buffer)
	}, c.getBuffer)

	if err != nil {
		c.mainChannel.MustInput(err)
	}
}

func (c *Client) handlerChannel(i interface{}) interface{} {
	switch v := i.(type) {
	case *Call: // send call
		c.handlerCall(v)
	case *hbuffer.Buffer: // read call back
		c.handlerBuffer(v)
	case error: // read socket error
		c.handlerError(v)
	default: // check calls timeout
		c.handlerTimeout()
	}
	return nil
}

func (c *Client) handlerCall(call *Call) {
	defer c.recoveryCallBuffer(call)

	isOneWay := call.isOneWay()
	if !isOneWay {
		c.cacheCall(call)
	}
	err := c.socket.WriteBuffer(call.buf)
	if call.doneIfErr(err) && !isOneWay {
		c.removeCall(call.seq)
		return
	}
	if isOneWay {
		call.done()
	}
}

func (c *Client) handlerBuffer(buffer *hbuffer.Buffer) {
	seq, err := buffer.ReadUint64()
	if err != nil {
		c.putBuffer(buffer)
		return
	}
	call, ok := c.removeCall(seq)
	if !ok {
		c.putBuffer(buffer)
		return
	}
	call.buf = buffer
	call.done()
}

func (c *Client) handlerTimeout() {
	currentCallMap := c.timeoutSlice[c.timeoutIndex]
	for _, call := range currentCallMap {
		delete(c.pending, call.seq)
		delete(currentCallMap, call.seq)
		call.doneIfErr(ErrCallTimeout)
	}

	c.timeoutIndex++
	c.timeoutIndex = c.timeoutIndex % len(c.timeoutSlice)
}

func (c *Client) handlerError(err error) {
	if err == nil {
		return
	}
	for _, call := range c.pending {
		delete(c.pending, call.seq)
		delete(c.timeoutSlice[call.timeoutIndex], call.seq)
		call.doneIfErr(err)
	}
	_ = c.Close()
}

func (c *Client) Go(protocolID int32, args interface{}, replies ...interface{}) *Call {
	var reply interface{}
	if len(replies) > 0 {
		reply = replies[0]
	}
	call := c.newCall(protocolID, args, reply)
	if call.error == nil {
		c.mainChannel.MustInput(call)
	} else {
		c.recoveryCallBuffer(call)
		call.done()
	}
	return call
}

func (c *Client) Call(protocolID int32, args interface{}, replies ...interface{}) error {
	return c.Go(protocolID, args, replies...).Done().error
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
		buf:        c.getBuffer(),
		c:          make(chan *Call, 1),
		client:     c,
	}
	call.buf.WriteEndianUint32(0) // write len
	b, err := c.translator.Marshal(call.args)
	if call.doneIfErr(err) {
		return call
	}
	call.buf.WriteInt32(call.protocolID)
	if !call.isOneWay() {
		call.seq = atomic.AddUint64(&c.seq, 1)
		call.buf.WriteUint64(call.seq)
	}
	call.buf.WriteBytes(b)
	call.buf.SetPosition(0)
	call.buf.WriteEndianUint32(uint32(call.buf.Len() - 4)) // rewrite final len
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

func (c *Client) recoveryCallBuffer(call *Call) {
	c.putBuffer(call.buf)
	call.buf = nil
}

func (c *Client) unmarshalCall(call *Call) {
	defer c.recoveryCallBuffer(call)
	errMsg, err := call.buf.ReadString()
	if err != nil {
		call.error = fmt.Errorf("hrpc: read error msg error:%s", err.Error())
		return
	}
	if errMsg != "" {
		call.error = errors.New(errMsg)
		return
	}

	call.error = c.translator.Unmarshal(call.buf.GetRestOfBytes(), call.reply)
}
