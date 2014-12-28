package kestrel

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"time"
)

type Client struct {
	Timeout    time.Duration
	server     string
	tclient    *KestrelClient
	ttransport *thrift.TFramedTransport
}

func NewClient(host string, port int) *Client {
	return &Client{
		server: fmt.Sprintf("%s:%d", host, port),

		// defaults
		Timeout: 3 * time.Second,
	}
}

func (c *Client) Peek(queueName string) (*QueueInfo, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return nil, err
	}

	return c.tclient.Peek(queueName)
}

func (c *Client) Get(queueName string, maxItems int32, timeout time.Duration, autoAbort time.Duration) ([]*QueueItem, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return nil, err
	}

	items, err := c.tclient.Get(queueName, maxItems, int32(timeout/time.Millisecond), int32(autoAbort/time.Millisecond))
	if err != nil {
		c.Close()
	}
	return NewQueueItems(items, queueName, c), err
}

func (c *Client) Put(queueName string, items [][]byte) (int32, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return 0, err
	}

	nitems, err := c.tclient.Put(queueName, items, 0)
	if err != nil {
		c.Close()
	}
	return nitems, err
}

func (c *Client) Confirm(queueName string, items []*QueueItem) (int32, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return 0, err
	}

	ids := make(map[int64]bool, len(items))
	for _, item := range items {
		ids[item.Id] = true
	}

	nitems, err := c.tclient.Confirm(queueName, ids)
	if err != nil {
		c.Close()
	}
	return nitems, err
}

func (c *Client) Abort(queueName string, items []*QueueItem) (int32, error) {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return 0, err
	}

	ids := make(map[int64]bool, len(items))
	for _, item := range items {
		ids[item.Id] = true
	}

	nitems, err := c.tclient.Abort(queueName, ids)
	if err != nil {
		c.Close()
	}
	return nitems, err
}

func (c *Client) FlushAllQueues() error {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return err
	}

	return c.tclient.FlushAllQueues()
}

func (c *Client) DeleteQueue(queueName string) error {
	err := c.ensureConnected()
	if err != nil {
		c.Close()
		return err
	}

	return c.tclient.DeleteQueue(queueName)
}

func (c *Client) Close() error {
	if c.ttransport != nil {
		c.ttransport.Close()
	}
	c.ttransport = nil

	return nil
}

func (c *Client) ensureConnected() error {
	if c.ttransport != nil {
		return nil
	}

	transport, err := thrift.NewTSocketTimeout(c.server, c.Timeout)
	if err == nil {
		err = transport.Open()
		if err == nil {
			c.ttransport = thrift.NewTFramedTransport(transport)
			c.tclient = NewKestrelClientFactory(c.ttransport, thrift.NewTBinaryProtocolFactoryDefault())
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}
