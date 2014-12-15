package beanstalkd

import (
	"time"

	"github.com/kr/beanstalk"
)

type BeanstalkdPeer struct {
	conn     *beanstalk.Conn
	messages chan []byte
}

func NewBeanstalkdPeer(host string) (*BeanstalkdPeer, error) {
	conn, err := beanstalk.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	return &BeanstalkdPeer{
		conn:     conn,
		messages: make(chan []byte, 100000),
	}, nil
}

func (b *BeanstalkdPeer) Subscribe() error {
	go func() {
		for {
			id, message, err := b.conn.Reserve(5 * time.Second)
			if err != nil {
				// Broker shutdown
				return
			}

			b.conn.Delete(id)
			b.messages <- message
		}
	}()
	return nil
}

func (b *BeanstalkdPeer) Recv() []byte {
	return <-b.messages
}

func (b *BeanstalkdPeer) Send(message []byte) {
	b.conn.Put(message, 1, 0, 0)
}

func (b *BeanstalkdPeer) Teardown() {
	b.conn.Close()
}
