package hrpc

import (
	"time"

	"github.com/hqpko/hnet"
)

type Client struct {
	*conn
}

func NewClient(socket *hnet.Socket) *Client {
	client := &Client{
		conn: newConn(socket),
	}
	return client
}

func (c *Client) SetCallTimeout(timeout time.Duration) *Client {
	c.pending.setTimeout(timeout)
	return c
}

func (c *Client) SetHandlerOneWay(handler func(pid int32, args []byte)) *Client {
	c.setHandlerOneWay(handler)
	return c
}

func (c *Client) Run() error {
	return c.run()
}

func (c *Client) OneWay(pid int32, args []byte) error {
	return c.oneWay(pid, args)
}

func (c *Client) Call(pid int32, args []byte) ([]byte, error) {
	return c.call(pid, args).Done()
}

func (c *Client) Close() error {
	return c.close()
}
