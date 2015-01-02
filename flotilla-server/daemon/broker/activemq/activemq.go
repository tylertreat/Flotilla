package activemq

import "gopkg.in/stomp.v1"

const queue = "test"

// Peer implements the peer interface for ActiveMQ.
type Peer struct {
	conn   *stomp.Conn
	sub    *stomp.Subscription
	send   chan []byte
	errors chan error
	done   chan bool
}

// NewPeer creates and returns a new Peer for communicating with ActiveMQ.
func NewPeer(host string) (*Peer, error) {
	conn, err := stomp.Dial("tcp", host, stomp.Options{})
	if err != nil {
		return nil, err
	}

	return &Peer{
		conn:   conn,
		send:   make(chan []byte),
		errors: make(chan error, 1),
		done:   make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (a *Peer) Subscribe() error {
	sub, err := a.conn.Subscribe(queue, stomp.AckAuto)
	if err != nil {
		return err
	}

	a.sub = sub
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (a *Peer) Recv() ([]byte, error) {
	message := <-a.sub.C
	return message.Body, message.Err
}

// Send returns a channel on which messages can be sent for publishing.
func (a *Peer) Send() chan<- []byte {
	return a.send
}

// Errors returns the channel on which the peer sends publish errors.
func (a *Peer) Errors() <-chan error {
	return a.errors
}

// Done signals to the peer that message publishing has completed.
func (a *Peer) Done() {
	a.done <- true
}

// Setup prepares the peer for testing.
func (a *Peer) Setup() {
	go func() {
		for {
			select {
			case msg := <-a.send:
				if err := a.conn.Send(queue, "", msg, nil); err != nil {
					a.errors <- err
				}
			case <-a.done:
				return
			}
		}
	}()
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (a *Peer) Teardown() {
	if a.sub != nil {
		a.sub.Unsubscribe()
	}
	a.conn.Disconnect()
}
