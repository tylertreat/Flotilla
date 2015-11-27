package nats

import (
	"fmt"
	"time"

	"github.com/nats-io/nats"
)

const (
	subject = "test"

	// Maximum bytes we will get behind before we start slowing down publishing.
	maxBytesBehind = 1024 * 1024 // 1MB

	// Maximum msgs we will get behind before we start slowing down publishing.
	maxMsgsBehind = 65536 // 64k

	// Time to delay publishing when we are behind.
	delay = 1 * time.Millisecond
)

// Peer implements the peer interface for NATS.
type Peer struct {
	conn     *nats.Conn
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
}

// NewPeer creates and returns a new Peer for communicating with NATS.
func NewPeer(host string) (*Peer, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s", host))
	if err != nil {
		return nil, err
	}

	// We want to be alerted if we get disconnected, this will be due to Slow
	// Consumer.
	conn.Opts.AllowReconnect = false

	return &Peer{
		conn:     conn,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (n *Peer) Subscribe() error {
	n.conn.Subscribe(subject, func(message *nats.Msg) {
		n.messages <- message.Data
	})
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (n *Peer) Recv() ([]byte, error) {
	return <-n.messages, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (n *Peer) Send() chan<- []byte {
	return n.send
}

// Errors returns the channel on which the peer sends publish errors.
func (n *Peer) Errors() <-chan error {
	return n.errors
}

// Done signals to the peer that message publishing has completed.
func (n *Peer) Done() {
	n.done <- true
}

// Setup prepares the peer for testing.
func (n *Peer) Setup() {
	go func() {
		for {
			select {
			case msg := <-n.send:
				if err := n.sendMessage(msg); err != nil {
					n.errors <- err
				}
			case <-n.done:
				return
			}
		}
	}()
}

func (n *Peer) sendMessage(message []byte) error {
	// Check if we are behind by >= 1MB bytes.
	bytesDeltaOver := n.conn.OutBytes-n.conn.InBytes >= maxBytesBehind

	// Check if we are behind by >= 65k msgs.
	msgsDeltaOver := n.conn.OutMsgs-n.conn.InMsgs >= maxMsgsBehind

	// If we are behind on either condition, sleep a bit to catch up receiver.
	if bytesDeltaOver || msgsDeltaOver {
		time.Sleep(delay)
	}

	return n.conn.Publish(subject, message)
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (n *Peer) Teardown() {
	n.conn.Close()
}
