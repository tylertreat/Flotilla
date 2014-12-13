package peer

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/tcp"
	"github.com/tylertreat/flotillad/daemon/mq"
)

type operation string
type test string

const (
	sub        operation = "subscribers"
	pub        operation = "publishers"
	start      operation = "start"
	latency    test      = "latency"
	throughput test      = "throughput"
)

type peer interface {
	recv() []byte
	send([]byte)
}

type request struct {
	Operation   operation `json:"operation"`
	NumMessages int       `json:"num_messages"`
	MessageSize int64     `json:"message_size"`
	Test        test      `json:"test"`
	Count       int       `json:"count"`
	Broker      string    `json:"broker"`
	Host        string    `json:"host"`
}

type response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// Daemon is an implementation of the Daemon interface which is responsible for
// orchestrating broker publishers and subscribers on the host machine.
type Daemon struct {
	mangos.Socket
	publishers []*publisher
}

// NewDaemon creates and returns a new peer Daemon. An error is returned if
// the Daemon could not be created successfully.
func NewDaemon() (*Daemon, error) {
	rep, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	rep.AddTransport(tcp.NewTransport())
	return &Daemon{rep, []*publisher{}}, nil
}

// Start will start the Daemon on the given port so that it can begin
// processing requests. It returns an error if it could not be successfully
// started.
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
				Message: fmt.Sprintf("Invalid peer request: %s", err.Error()),
			})
			continue
		}

		if err := d.processRequest(req); err != nil {
			d.sendResponse(response{
				Success: false,
				Message: err.Error(),
			})
			continue
		}

		d.sendResponse(response{
			Success: true,
			Message: "Processed request",
		})
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

func (d *Daemon) processRequest(req request) error {
	switch req.Operation {
	case pub:
		return d.processPub(req.Broker, req.Host, req.Count, req.NumMessages,
			req.MessageSize, req.Test)
	case sub:
		return d.processSub(req.Broker, req.Host, req.Count, req.NumMessages,
			req.MessageSize, req.Test)
	case start:
		return d.processStart()
	default:
		return fmt.Errorf("Invalid operation %s", req.Operation)
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

		complete := make(chan bool)
		go func(c chan bool) {
			(&subscriber{
				peer:        receiver,
				id:          i,
				numMessages: numMessages,
				messageSize: messageSize,
				test:        test,
				complete:    complete,
			}).start()
		}(complete)

		// TODO: collect results and send them back.
	}

	return nil
}

func (d *Daemon) processStart() error {
	for _, publisher := range d.publishers {
		go publisher.start()
	}

	// TODO collect results and send them back.
	return nil
}

func newPeer(broker, host string) (peer, error) {
	switch broker {
	case mq.NATS:
		return newNATS(host)
	default:
		return nil, fmt.Errorf("Invalid broker: %s", broker)
	}
}
