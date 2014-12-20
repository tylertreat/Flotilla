package kafka

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const topic = "test"

type KafkaPeer struct {
	host     string
	client   *sarama.Client
	producer *sarama.Producer
	consumer *sarama.Consumer
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
		host:     host,
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

func (k *KafkaPeer) Recv() []byte {
	event := <-k.consumer.Events()
	println("recv")
	if event.Err != nil {
		panic(event.Err)
	}
	return event.Value
}

func (k *KafkaPeer) Send(message []byte) {
	for {
		select {
		case k.producer.Input() <- &sarama.MessageToSend{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}:
			time.Sleep(time.Second)
			return
		case err := <-k.producer.Errors():
			panic(err.Err)
		}
	}
}

func (k *KafkaPeer) Teardown() {
	k.producer.Close()
	if k.consumer != nil {
		k.consumer.Close()
	}
	k.client.Close()
}
