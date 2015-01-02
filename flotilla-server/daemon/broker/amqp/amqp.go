package amqp

import (
	"github.com/streadway/amqp"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

const (
	exchange = "test"
)

// Peer implements the peer interface for AMQP brokers.
type Peer struct {
	conn    *amqp.Connection
	queue   amqp.Queue
	channel *amqp.Channel
	inbound <-chan amqp.Delivery
	send    chan []byte
	errors  chan error
	done    chan bool
}

// NewPeer creates and returns a new Peer for communicating with AMQP brokers.
func NewPeer(host string) (*Peer, error) {
	conn, err := amqp.Dial("amqp://" + host)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		broker.GenerateName(), // name
		false, // not durable
		false, // delete when unused
		true,  // exclusive
		false, // no wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	err = channel.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		false,    //  not durable
		false,    // auto-deleted
		false,    // internal
		false,    // no wait
		nil,      // arguments
	)
	if err != nil {
		return nil, err
	}

	return &Peer{
		conn:    conn,
		queue:   queue,
		channel: channel,
		send:    make(chan []byte),
		errors:  make(chan error, 1),
		done:    make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (a *Peer) Subscribe() error {
	err := a.channel.QueueBind(
		a.queue.Name,
		a.queue.Name,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	a.inbound, err = a.channel.Consume(
		a.queue.Name, // queue
		"",           // consumer
		true,         // auto ack
		false,        // exclusive
		true,         // no local
		false,        //  no wait
		nil,          // args
	)
	if err != nil {
		return err
	}

	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (a *Peer) Recv() ([]byte, error) {
	message := <-a.inbound
	return message.Body, nil
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
				if err := a.channel.Publish(
					exchange, // exchange
					"",       // routing key
					false,    // mandatory
					false,    // immediate
					amqp.Publishing{Body: msg},
				); err != nil {
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
	a.channel.Close()
	a.conn.Close()
}
