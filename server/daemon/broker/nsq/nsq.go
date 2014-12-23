package nsq

import (
	"github.com/bitly/go-nsq"
	"github.com/tylertreat/flotilla/server/daemon/broker"
)

const topic = "test"

type NSQPeer struct {
	producer *nsq.Producer
	consumer *nsq.Consumer
	host     string
	messages chan []byte
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

func (n *NSQPeer) Send(message []byte) error {
	return n.producer.PublishAsync(topic, message, nil)
}

func (n *NSQPeer) Teardown() {
	n.producer.Stop()
	if n.consumer != nil {
		n.consumer.DisconnectFromNSQD(n.host)
		n.consumer.Stop()
		<-n.consumer.StopChan
	}
}
