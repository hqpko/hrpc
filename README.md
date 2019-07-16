# hrpc

**练手项目，并未在实际项目中使用，请慎用**


参考 `go` 自带的 `rpc`，添加了几个额外的功能

### get
```
go get -v -u github.com/hqpko/hrpc
```

### example
```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/hqpko/hrpc"
	"github.com/hqpko/hrpc/example/proto"
)

const rpcAddr = "127.0.0.1:9001"

func main() {
	go serverListen()
	time.Sleep(time.Second)

	client, _ := hrpc.Connect(rpcAddr)
	go client.Run()

	resp := &proto.Resp{}
	err := client.Call(1, &proto.Req{A: 1}, resp)
	fmt.Println(resp.B, err) // 2, nil

	err = client.Call(2, &proto.Req{A: 1}) // server log req.A = 1
	fmt.Println(err)                       // nil
}

func serverListen() {
	_ = hrpc.Listen(rpcAddr, func(s *hrpc.Stream) {
		s.Register(1, func(args *proto.Req, reply *proto.Resp) error {
			reply.B = args.A + 1
			return nil
		})
		s.Register(2, func(args *proto.Req) {
			log.Println(args.A)
		})
		go s.Run()
	})
}
```

### 高性能低精度的超时（其实就是给每个 Call 一个超时时间，每隔一段时间检查下所有 pending 中的 Call）
```go
client, _ := hrpc.Connect(rpcAddr)

// callTimeout, call 的超时时间
// stepTimeout, 每隔 stepTimeout 时间间隔检查一次超时
client.SetTimeoutOption(callTimeout, stepTimeout)
```

### 自定义编解码器
自带三种编解码器:`translatorProto`,`translatorFastProto`,`translatorMarshaler`, 默认使用 `translatorProto`

```go
client, _ := hrpc.Connect(rpcAddr)
client.SetTranslator(hrpc.NewTranslatorFashProto())
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
>> 注意：为了避免死锁，请在 `Client` 端 **只** 注册 `oneway` 方式的协议，即 `Client` 只注册 `func (req)` 协议，避免注册 `func(req,resp)` 协议（当 `Client` 发起一个请求并等待 `Server` 返回时，进入锁状态，此时如果 `Server` 也发起了一个 `req-resp` 请求，`Client` 无法响应，导致死锁）

