package beanstalkd

import (
	"time"

	"github.com/kr/beanstalk"
)

type BeanstalkdPeer struct {
	conn     *beanstalk.Conn
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
}

func NewBeanstalkdPeer(host string) (*BeanstalkdPeer, error) {
	conn, err := beanstalk.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	return &BeanstalkdPeer{
		conn:     conn,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error),
		done:     make(chan bool, 1),
	}, nil
}

func (b *BeanstalkdPeer) Subscribe() error {
	go func() {
		for {
			id, message, err := b.conn.Reserve(5 * time.Second)
			if err != nil {
				// Broker shutdown.
				return
			}

			b.conn.Delete(id)
			b.messages <- message
		}
	}()
	return nil
}

func (b *BeanstalkdPeer) Recv() ([]byte, error) {
	return <-b.messages, nil
}

func (b *BeanstalkdPeer) Send() chan<- []byte {
	return b.send
}

func (b *BeanstalkdPeer) Errors() <-chan error {
	return b.errors
}

func (b *BeanstalkdPeer) Setup() {
	go func() {
		for {
			select {
			case msg := <-b.send:
				if _, err := b.conn.Put(msg, 1, 0, 0); err != nil {
					b.errors <- err
				}
			case <-b.done:
				return
			}
		}
	}()
}

func (b *BeanstalkdPeer) Teardown() {
	b.done <- true
	b.conn.Close()
}
