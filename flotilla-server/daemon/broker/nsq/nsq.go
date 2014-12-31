package nsq

import (
	"github.com/bitly/go-nsq"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

const (
	topic      = "test"
	bufferSize = 5
)

type NSQPeer struct {
	producer *nsq.Producer
	consumer *nsq.Consumer
	host     string
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
	flush    chan bool
}

func NewNSQPeer(host string) (*NSQPeer, error) {
	producer, err := nsq.NewProducer(host, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	return &NSQPeer{
		host:     host,
		producer: producer,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
		flush:    make(chan bool),
	}, nil
}

func (n *NSQPeer) Subscribe() error {
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

func (n *NSQPeer) Recv() ([]byte, error) {
	return <-n.messages, nil
}

func (n *NSQPeer) Send() chan<- []byte {
	return n.send
}

func (n *NSQPeer) Errors() <-chan error {
	return n.errors
}

func (n *NSQPeer) Done() {
	n.done <- true
	<-n.flush
}

func (n *NSQPeer) Setup() {
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
					if err := n.producer.MultiPublishAsync(topic, buffer, nil, nil); err != nil {
						n.errors <- err
					}
				}
				n.flush <- true
				return
			}
		}
	}()

}

func (n *NSQPeer) Teardown() {
	n.producer.Stop()
	if n.consumer != nil {
		n.consumer.DisconnectFromNSQD(n.host)
		n.consumer.Stop()
		<-n.consumer.StopChan
	}
}
