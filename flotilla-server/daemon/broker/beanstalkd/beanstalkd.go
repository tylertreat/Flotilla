package beanstalkd

import (
	"time"

	"github.com/kr/beanstalk"
)

// Peer implements the peer interface for Beanstalkd.
type Peer struct {
	conn     *beanstalk.Conn
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
}

// NewPeer creates and returns a new Peer for communicating with Beanstalkd.
func NewPeer(host string) (*Peer, error) {
	conn, err := beanstalk.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	return &Peer{
		conn:     conn,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (b *Peer) Subscribe() error {
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

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (b *Peer) Recv() ([]byte, error) {
	return <-b.messages, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (b *Peer) Send() chan<- []byte {
	return b.send
}

// Errors returns the channel on which the peer sends publish errors.
func (b *Peer) Errors() <-chan error {
	return b.errors
}

// Done signals to the peer that message publishing has completed.
func (b *Peer) Done() {
	b.done <- true
}

// Setup prepares the peer for testing.
func (b *Peer) Setup() {
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

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (b *Peer) Teardown() {
	b.conn.Close()
}
