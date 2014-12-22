package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/tcp"
	"github.com/tylertreat/flotilla/daemon/broker/activemq"
	"github.com/tylertreat/flotilla/daemon/broker/amqp"
	"github.com/tylertreat/flotilla/daemon/broker/beanstalkd"
	"github.com/tylertreat/flotilla/daemon/broker/kafka"
	"github.com/tylertreat/flotilla/daemon/broker/kestrel"
	"github.com/tylertreat/flotilla/daemon/broker/nats"
	"github.com/tylertreat/flotilla/daemon/broker/nsq"
)

type daemon string
type operation string
type test string

const (
	start      operation = "start"
	stop       operation = "stop"
	run        operation = "run"
	sub        operation = "subscribers"
	pub        operation = "publishers"
	results    operation = "results"
	teardown   operation = "teardown"
	latency    test      = "latency"
	throughput test      = "throughput"
	NATS                 = "nats"
	Beanstalkd           = "beanstalkd"
	Kafka                = "kafka"
	Kestrel              = "kestrel"
	ActiveMQ             = "activemq"
	RabbitMQ             = "rabbitmq"
	NSQ                  = "nsq"
)

type request struct {
	Operation   operation `json:"operation"`
	Broker      string    `json:"broker"`
	Port        string    `json:"port"`
	NumMessages int       `json:"num_messages"`
	MessageSize int64     `json:"message_size"`
	Test        test      `json:"test"`
	Count       int       `json:"count"`
	Host        string    `json:"host"`
}

type response struct {
	Success    bool        `json:"success"`
	Message    string      `json:"message"`
	Result     interface{} `json:"result"`
	PubResults []*result   `json:"pub_results,omitempty"`
	SubResults []*result   `json:"sub_results,omitempty"`
}

type result struct {
	Duration   float32         `json:"duration,omitempty"`
	Throughput float32         `json:"throughput,omitempty"`
	Latency    *latencyResults `json:"latency,omitempty"`
	Err        string          `json:"error,omitempty"`
}

type broker interface {
	Start(string, string) (interface{}, error)
	Stop() (interface{}, error)
}

type peer interface {
	Subscribe() error
	Recv() ([]byte, error)
	Send([]byte) error
	Teardown()
}

type Daemon struct {
	mangos.Socket
	broker      broker
	publishers  []*publisher
	subscribers []*subscriber
}

func NewDaemon() (*Daemon, error) {
	rep, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	rep.AddTransport(tcp.NewTransport())
	return &Daemon{rep, nil, []*publisher{}, []*subscriber{}}, nil
}

func (d *Daemon) Start(port int) error {
	if err := d.Listen(fmt.Sprintf("tcp://:%d", port)); err != nil {
		return err
	}
	return d.loop()
}

func (d *Daemon) loop() error {
	for {
		msg, err := d.Recv()
		if err != nil {
			log.Println(err)
			continue
		}

		var req request
		if err := json.Unmarshal(msg, &req); err != nil {
			log.Println("Invalid peer request:", err)
			d.sendResponse(response{
				Success: false,
				Message: fmt.Sprintf("Invalid request: %s", err.Error()),
			})
			continue
		}

		resp := d.processRequest(req)
		d.sendResponse(resp)
	}
}

func (d *Daemon) sendResponse(rep response) {
	repJSON, err := json.Marshal(rep)
	if err != nil {
		// This is not recoverable.
		panic(err)
	}

	if err := d.Send(repJSON); err != nil {
		log.Println(err)
	}
}

func (d *Daemon) processRequest(req request) response {
	var (
		response response
		err      error
	)
	switch req.Operation {
	case start:
		response.Result, err = d.processBrokerStart(req.Broker, req.Host, req.Port)
	case stop:
		response.Result, err = d.processBrokerStop()
	case pub:
		err = d.processPub(req.Broker, req.Host, req.Count, req.NumMessages,
			req.MessageSize, req.Test)
	case sub:
		err = d.processSub(req.Broker, req.Host, req.Count, req.NumMessages,
			req.MessageSize, req.Test)
	case run:
		err = d.processPublisherStart()
	case results:
		response.PubResults, response.SubResults, err = d.processResults()
		if err != nil {
			response.Message = err.Error()
			err = nil
		}
	case teardown:
		d.processTeardown()
	default:
		err = fmt.Errorf("Invalid operation %s", req.Operation)
	}

	if err != nil {
		response.Message = err.Error()
	} else {
		response.Success = true
	}

	return response
}
func (d *Daemon) processBrokerStart(broker, host, port string) (interface{}, error) {
	if d.broker != nil {
		return "", errors.New("Broker already running")
	}

	switch broker {
	case NATS:
		d.broker = &nats.NATSBroker{}
	case Beanstalkd:
		d.broker = &beanstalkd.BeanstalkdBroker{}
	case Kafka:
		d.broker = &kafka.KafkaBroker{}
	case Kestrel:
		d.broker = &kestrel.KestrelBroker{}
	case ActiveMQ:
		d.broker = &activemq.ActiveMQBroker{}
	case RabbitMQ:
		d.broker = &amqp.RabbitMQBroker{}
	case NSQ:
		d.broker = &nsq.NSQBroker{}
	default:
		return "", fmt.Errorf("Invalid broker %s", broker)
	}

	result, err := d.broker.Start(host, port)
	if err != nil {
		d.broker = nil
	}
	return result, err
}

func (d *Daemon) processBrokerStop() (interface{}, error) {
	if d.broker == nil {
		return "", errors.New("No broker running")
	}

	result, err := d.broker.Stop()
	if err == nil {
		d.broker = nil
	}
	return result, err
}

func (d *Daemon) processPub(broker, host string, count, numMessages int,
	messageSize int64, test test) error {

	for i := 0; i < count; i++ {
		sender, err := newPeer(broker, host)
		if err != nil {
			return err
		}

		d.publishers = append(d.publishers, &publisher{
			peer:        sender,
			id:          i,
			numMessages: numMessages,
			messageSize: messageSize,
			test:        test,
		})
	}

	return nil
}

func (d *Daemon) processSub(broker, host string, count, numMessages int,
	messageSize int64, test test) error {

	for i := 0; i < count; i++ {
		receiver, err := newPeer(broker, host)
		if err != nil {
			return err
		}

		if err := receiver.Subscribe(); err != nil {
			return err
		}

		subscriber := &subscriber{
			peer:        receiver,
			id:          i,
			numMessages: numMessages,
			messageSize: messageSize,
			test:        test,
		}
		d.subscribers = append(d.subscribers, subscriber)
		go subscriber.start()
	}

	return nil
}

func (d *Daemon) processPublisherStart() error {
	for _, publisher := range d.publishers {
		go publisher.start()
	}

	return nil
}

func (d *Daemon) processResults() ([]*result, []*result, error) {
	subResults := make([]*result, 0, len(d.subscribers))
	for _, subscriber := range d.subscribers {
		result, err := subscriber.getResults()
		if err != nil {
			return nil, nil, err
		}
		subResults = append(subResults, result)
	}

	pubResults := make([]*result, 0, len(d.publishers))
	for _, publisher := range d.publishers {
		result, err := publisher.getResults()
		if err != nil {
			return nil, nil, err
		}
		pubResults = append(pubResults, result)
	}

	log.Println("Benchmark completed")
	return pubResults, subResults, nil
}

func (d *Daemon) processTeardown() {
	for _, subscriber := range d.subscribers {
		subscriber.Teardown()
	}
	d.subscribers = d.subscribers[:0]

	for _, publisher := range d.publishers {
		publisher.Teardown()
	}
	d.publishers = d.publishers[:0]
}

func newPeer(broker, host string) (peer, error) {
	switch broker {
	case NATS:
		return nats.NewNATSPeer(host)
	case Beanstalkd:
		return beanstalkd.NewBeanstalkdPeer(host)
	case Kafka:
		return kafka.NewKafkaPeer(host)
	case Kestrel:
		return kestrel.NewKestrelPeer(host)
	case ActiveMQ:
		return activemq.NewActiveMQPeer(host)
	case RabbitMQ:
		return amqp.NewAMQPPeer(host)
	case NSQ:
		return nsq.NewNSQPeer(host)
	default:
		return nil, fmt.Errorf("Invalid broker: %s", broker)
	}
}
