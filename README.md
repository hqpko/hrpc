# hrpc

### example
https://github.com/hqpko/hrpc-example

参考 `go` 自带的 `rpc`，添加了几个额外的功能

### 高性能低精度的超时（其实就是给每个 Call 一个超时时间，每隔一段时间检查下所有 pending 中的 Call）
```go
client, _ := hrpc.Connect(rpcAddr)

// callTimeout, call 的超时时间
// stepTimeout, 每隔 stepTimeout 时间间隔检查一次超时
client.SetTimeoutOption(callTimeout, stepTimeout)
```

### 可以自定义编解码器

##### proto
```go
type translatorProto struct{}

func NewTranslatorProto() Translator {
	return new(translatorProto)
}

func (tp *translatorProto) Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error {
	if pb, ok := value.(proto.Message); ok {
		if bs, err := proto.Marshal(pb); err != nil {
			return err
		} else {
			writeBuffer.WriteBytes(bs)
			return nil
		}
	}
	return ErrNotPbMessage
}

func (tp *translatorProto) Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error {
	if pb, ok := value.(proto.Message); ok {
		return proto.Unmarshal(readBuffer.GetRestOfBytes(), pb)
	}
	return ErrNotPbMessage
}
```
##### fast proto
```go
type translatorFastProto struct{}

func NewTranslatorFastProto() Translator {
	return new(translatorFastProto)
}

func (tp *translatorFastProto) Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error {
	if pb, ok := value.(fast.Message); ok {
		if bs, err := fast.Marshal(pb); err != nil {
			return err
		} else {
			writeBuffer.WriteBytes(bs)
			return nil
		}
	}
	return ErrNotPbMessage
}

func (tp *translatorFastProto) Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error {
	if pb, ok := value.(fast.Message); ok {
		return fast.Unmarshal(readBuffer.GetRestOfBytes(), pb)
	}
	return ErrNotPbMessage
}

```

### 双向通讯
`stream` 支持双向通讯
```go
client, _ := hrpc.Connect(rpcAddr)
client.Register(1, func(req *proto.Req) {
	log.Println(req.A)
})

......
server.Call(1,&proto.Req{A:1})
```
> 注意：为了避免死锁，请在 `Client` 端 **只** 注册 `oneway` 方式的协议，即 `Client` 只注册 `func (req)` 协议，避免注册 `func(req,resp)` 协议（当 `Client` 发起一个请求并等待 `Server` 返回时，进入锁状态，此时如果 `Server` 也发起了一个 `req-resp` 请求，`Client` 无法响应，导致死锁）
