package hrpc

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
	"github.com/hqpko/hticker"
	"github.com/hqpko/hutils"
)

type Call struct {
	seq          uint64
	protocolID   int32
	args         interface{}
	reply        interface{}
	error        error
	timeoutIndex int
	buf          *hbuffer.Buffer
	client       *client
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

func (c *Call) Done() error {
	<-c.c
	if c.buf != nil {
		c.client.unmarshalCall(c)
	}
	return c.error
}

type client struct {
	bufferPool *hutils.BufferPool

	seq         uint64
	pending     map[uint64]*Call
	translator  Translator
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

func newClient(bufferPool *hutils.BufferPool) *client {
	c := &client{
		bufferPool:          bufferPool,
		pending:             map[uint64]*Call{},
		timeoutCall:         defTimeoutCall,
		timeoutMaxDuration:  defTimeoutMaxDuration,
		timeoutStepDuration: defTimeoutStepDuration,
	}
	c.mainChannel = hconcurrent.NewConcurrent(defChannelSize, 1, c.handlerChannel)
	return c
}

func (c *client) setTranslator(translator Translator) *client {
	c.translator = translator
	return c
}

// SetTimeoutOption
// timeoutCall			: duration of rpc all timeout
// stepDuration			: step duration of checking timeout
func (c *client) setTimeoutOption(timeoutCall, stepDuration time.Duration) *client {
	maxTimeoutDuration := timeoutCall * 2
	if stepDuration > timeoutCall {
		stepDuration = timeoutCall
	}
	c.timeoutCall = timeoutCall
	c.timeoutStepDuration = stepDuration
	c.timeoutMaxDuration = maxTimeoutDuration
	return c
}

func (c *client) run(socket *hnet.Socket) {
	c.socket = socket
	c.initTimeout()
	c.mainChannel.Start()
}

func (c *client) initTimeout() {
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

func (c *client) handlerChannel(i interface{}) interface{} {
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

func (c *client) handlerCall(call *Call) {
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

func (c *client) handlerBuffer(buffer *hbuffer.Buffer) {
	seq, err := buffer.ReadUint64()
	if err != nil {
		c.bufferPool.Put(buffer)
		return
	}
	call, ok := c.removeCall(seq)
	if !ok {
		c.bufferPool.Put(buffer)
		return
	}
	call.buf = buffer
	call.done()
}

func (c *client) handlerTimeout() {
	currentCallMap := c.timeoutSlice[c.timeoutIndex]
	for _, call := range currentCallMap {
		delete(c.pending, call.seq)
		delete(currentCallMap, call.seq)
		call.doneIfErr(ErrCallTimeout)
	}

	c.timeoutIndex++
	c.timeoutIndex = c.timeoutIndex % len(c.timeoutSlice)
}

func (c *client) handlerError(err error) {
	if err == nil {
		return
	}
	for _, call := range c.pending {
		delete(c.pending, call.seq)
		delete(c.timeoutSlice[call.timeoutIndex], call.seq)
		call.doneIfErr(err)
	}
}

func (c *client) call(protocolID int32, args interface{}, replies ...interface{}) *Call {
	var reply interface{}
	if len(replies) > 0 {
		reply = replies[0]
	}
	call := c.newCall(protocolID, args, reply)
	if call.error == nil {
		c.mainChannel.MustInput(call)
	} else {
		c.recoveryCallBuffer(call)
	}
	return call
}

func (c *client) close() error {
	c.mainChannel.Stop()
	c.timeoutTickerGroup.Stop()
	return c.socket.Close()
}

func (c *client) cacheCall(call *Call) {
	c.pending[call.seq] = call
	call.timeoutIndex = (c.timeoutOffset + c.timeoutIndex) % len(c.timeoutSlice)
	c.timeoutSlice[call.timeoutIndex][call.seq] = call
}

func (c *client) removeCall(seq uint64) (*Call, bool) {
	call, ok := c.pending[seq]
	if ok {
		delete(c.pending, seq)
		delete(c.timeoutSlice[call.timeoutIndex], seq)
	}
	return call, ok
}

func (c *client) newCall(protocolID int32, args, reply interface{}) *Call {
	call := &Call{
		protocolID: protocolID,
		args:       args,
		reply:      reply,
		buf:        c.bufferPool.Get(),
		c:          make(chan *Call, 1),
		client:     c,
	}
	call.buf.WriteEndianUint32(0) // write len
	call.buf.WriteByte(callTypeRequest)
	call.buf.WriteInt32(call.protocolID)
	if !call.isOneWay() {
		call.seq = atomic.AddUint64(&c.seq, 1)
		call.buf.WriteUint64(call.seq)
	}
	err := c.translator.Marshal(call.args, call.buf)
	if call.doneIfErr(err) {
		return call
	}
	call.buf.SetPosition(0)
	call.buf.WriteEndianUint32(uint32(call.buf.Len() - 4)) // rewrite final len
	return call
}

func (c *client) recoveryCallBuffer(call *Call) {
	c.bufferPool.Put(call.buf)
	call.buf = nil
}

func (c *client) unmarshalCall(call *Call) {
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

	call.error = c.translator.Unmarshal(call.buf, call.reply)
}
