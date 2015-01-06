package nsq

import (
	"github.com/bitly/go-nsq"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

const (
	topic = "test"

	// bufferSize is the number of messages we try to publish at a time to
	// increase throughput. TODO: this might need tweaking.
	bufferSize = 50
)

// Peer implements the peer interface for NSQ.
type Peer struct {
	producer *nsq.Producer
	consumer *nsq.Consumer
	host     string
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
	flush    chan bool
}

// NewPeer creates and returns a new Peer for communicating with NSQ.
func NewPeer(host string) (*Peer, error) {
	producer, err := nsq.NewProducer(host, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	return &Peer{
		host:     host,
		producer: producer,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
		flush:    make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (n *Peer) Subscribe() error {
	consumer, err := nsq.NewConsumer(topic, broker.GenerateName(), nsq.NewConfig())
	if err != nil {
		return err
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		n.messages <- message.Body
		return nil
	}))

	if err := consumer.ConnectToNSQD(n.host); err != nil {
		return err
	}

	n.consumer = consumer
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
	<-n.flush
}

// Setup prepares the peer for testing.
func (n *Peer) Setup() {
	buffer := make([][]byte, bufferSize)
	go func() {
		i := 0
		for {
			select {
			case msg := <-n.send:
				buffer[i] = msg
				i++
				if i == bufferSize {
					if err := n.producer.MultiPublishAsync(topic, buffer, nil, nil); err != nil {
						n.errors <- err
					}
					i = 0
				}
			case <-n.done:
				if i > 0 {
					if err := n.producer.MultiPublishAsync(topic, buffer[0:i], nil, nil); err != nil {
						n.errors <- err
					}
				}
				n.flush <- true
				return
			}
		}
	}()

}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (n *Peer) Teardown() {
	n.producer.Stop()
	if n.consumer != nil {
		n.consumer.Stop()
		<-n.consumer.StopChan
	}
}
