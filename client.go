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
