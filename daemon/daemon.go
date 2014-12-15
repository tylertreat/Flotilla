package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/tcp"
	"github.com/tylertreat/flotilla/daemon/beanstalkd"
	"github.com/tylertreat/flotilla/daemon/nats"
)

type daemon string
type operation string
type test string

const (
	brokerd    daemon    = "broker"
	peerd      daemon    = "peer"
	start      operation = "start"
	stop       operation = "stop"
	sub        operation = "subscribers"
	pub        operation = "publishers"
	results    operation = "results"
	teardown   operation = "teardown"
	latency    test      = "latency"
	throughput test      = "throughput"
	NATS                 = "nats"
	Beanstalkd           = "beanstalkd"
)

type request struct {
	Daemon      daemon    `json:"daemon"`
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
}

type broker interface {
	Start(string) (interface{}, error)
	Stop() (interface{}, error)
}

type peer interface {
	Subscribe() error
	Recv() []byte
	Send([]byte)
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

		var resp response
		switch req.Daemon {
		case brokerd:
			resp = d.processBrokerRequest(req)
		case peerd:
			resp = d.processPeerRequest(req)
		default:
			log.Printf("Invalid daemon: %s", req.Daemon)
			resp = response{
				Success: false,
				Message: fmt.Sprintf("Invalid daemon: %s", req.Daemon),
			}
		}

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

func (d *Daemon) processBrokerRequest(req request) response {
	var (
		msg    string
		result interface{}
		err    error
	)
	switch req.Operation {
	case start:
		result, err = d.processBrokerStart(req.Broker, req.Port)
	case stop:
		result, err = d.processBrokerStop()
	default:
		err = fmt.Errorf("Invalid operation %s", req.Operation)
	}

	if err != nil {
		msg = err.Error()
	}

	return response{Success: err == nil, Message: msg, Result: result}
}

func (d *Daemon) processBrokerStart(broker, port string) (interface{}, error) {
	if d.broker != nil {
		return "", errors.New("Broker already running")
	}

	switch broker {
	case NATS:
		d.broker = &nats.NATSBroker{}
	case Beanstalkd:
		d.broker = &beanstalkd.BeanstalkdBroker{}
	default:
		return "", fmt.Errorf("Invalid broker %s", broker)
	}

	result, err := d.broker.Start(port)
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

func (d *Daemon) processPeerRequest(req request) response {
	var (
		msg        string
		pubResults []*result
		subResults []*result
		err        error
	)
	switch req.Operation {
	case pub:
		err = d.processPub(req.Broker, req.Host, req.Count, req.NumMessages,
			req.MessageSize, req.Test)
	case sub:
		err = d.processSub(req.Broker, req.Host, req.Count, req.NumMessages,
			req.MessageSize, req.Test)
	case start:
		err = d.processPublisherStart()
	case results:
		pubResults, subResults, err = d.processResults()
		if err != nil {
			msg = err.Error()
			err = nil
		}
	case teardown:
		d.processTeardown()
	default:
		err = fmt.Errorf("Invalid operation %s", req.Operation)
	}

	if err != nil {
		msg = err.Error()
	}

	return response{
		Success:    err == nil,
		Message:    msg,
		PubResults: pubResults,
		SubResults: subResults,
	}
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
			results:     make(chan *result, 1),
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
			results:     make(chan *result, 1),
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
		select {
		case result := <-subscriber.results:
			subResults = append(subResults, result)
		default:
			return nil, nil, errors.New("Results not ready")
		}
	}

	pubResults := make([]*result, 0, len(d.publishers))
	for _, publisher := range d.publishers {
		select {
		case result := <-publisher.results:
			pubResults = append(pubResults, result)
		default:
			log.Println("Results not ready")
			return nil, nil, errors.New("Results not ready")
		}
	}

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
	default:
		return nil, fmt.Errorf("Invalid broker: %s", broker)
	}
}
