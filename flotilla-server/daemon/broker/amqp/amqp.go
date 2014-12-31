package amqp

import (
	"github.com/streadway/amqp"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

const (
	exchange = "test"
)

type AMQPPeer struct {
	conn    *amqp.Connection
	queue   amqp.Queue
	channel *amqp.Channel
	inbound <-chan amqp.Delivery
	send    chan []byte
	errors  chan error
	done    chan bool
}

func NewAMQPPeer(host string) (*AMQPPeer, error) {
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

	return &AMQPPeer{
		conn:    conn,
		queue:   queue,
		channel: channel,
		send:    make(chan []byte),
		errors:  make(chan error, 1),
		done:    make(chan bool),
	}, nil
}

func (a *AMQPPeer) Subscribe() error {
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

func (a *AMQPPeer) Recv() ([]byte, error) {
	message := <-a.inbound
	return message.Body, nil
}

func (a *AMQPPeer) Send() chan<- []byte {
	return a.send
}

func (a *AMQPPeer) Errors() <-chan error {
	return a.errors
}

func (a *AMQPPeer) Done() {
	a.done <- true
}

func (a *AMQPPeer) Setup() {
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

func (a *AMQPPeer) Teardown() {
	a.channel.Close()
	a.conn.Close()
}
