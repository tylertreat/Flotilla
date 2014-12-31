package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
)

const topic = "test"

type KafkaPeer struct {
	client   *sarama.Client
	producer *sarama.Producer
	consumer *sarama.Consumer
	send     chan []byte
	errors   chan error
	done     chan bool
}

func NewKafkaPeer(host string) (*KafkaPeer, error) {
	host = strings.Split(host, ":")[0] + ":9092"
	client, err := sarama.NewClient("producer", []string{host}, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		return nil, err
	}

	return &KafkaPeer{
		client:   client,
		producer: producer,
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

func (k *KafkaPeer) Subscribe() error {
	consumerConfig := sarama.NewConsumerConfig()
	consumerConfig.OffsetMethod = sarama.OffsetMethodNewest
	consumerConfig.DefaultFetchSize = 10 * 1024 * 1024
	consumer, err := sarama.NewConsumer(k.client, topic, 0, topic, consumerConfig)
	if err != nil {
		return err
	}
	k.consumer = consumer
	return nil
}

func (k *KafkaPeer) Recv() ([]byte, error) {
	event := <-k.consumer.Events()
	if event.Err != nil {
		return nil, event.Err
	}
	return event.Value, nil
}

func (k *KafkaPeer) Send() chan<- []byte {
	return k.send
}

func (k *KafkaPeer) Errors() <-chan error {
	return k.errors
}

func (k *KafkaPeer) Done() {
	k.done <- true
}

func (k *KafkaPeer) Setup() {
	go func() {
		for {
			select {
			case msg := <-k.send:
				if err := k.sendMessage(msg); err != nil {
					k.errors <- err
				}
			case <-k.done:
				return
			}
		}
	}()
}

func (k *KafkaPeer) sendMessage(message []byte) error {
	select {
	case k.producer.Input() <- &sarama.MessageToSend{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}:
		return nil
	case err := <-k.producer.Errors():
		return err.Err
	}
}

func (k *KafkaPeer) Teardown() {
	k.producer.Close()
	if k.consumer != nil {
		k.consumer.Close()
	}
	k.client.Close()
}
