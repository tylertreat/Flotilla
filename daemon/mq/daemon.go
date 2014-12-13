package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/tcp"
)

type operation string

const (
	start operation = "start"
	stop  operation = "stop"
)

const (
	NATS = "nats"
)

type request struct {
	Operation operation `json:"operation"`
	Broker    string    `json:"broker"`
	Port      string    `json:"port"`
}

type response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type broker interface {
	start(string) error
	stop() error
}

// Daemon is a implementation of the Daemon interface which is responsible for
// managing message brokers on the host machine.
type Daemon struct {
	mangos.Socket
	broker broker
}

// NewDaemon creates and returns a new broker Daemon. An error is returned if
// the Daemon could not be created successfully.
func NewDaemon() (*Daemon, error) {
	rep, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	rep.AddTransport(tcp.NewTransport())
	return &Daemon{rep, nil}, nil
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
		err = json.Unmarshal(msg, &req)
		if err != nil {
			log.Println("Invalid broker request:", err)
			d.sendResponse(response{
				Success: false,
				Message: fmt.Sprintf("Invalid broker request: %s", err.Error()),
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
	case start:
		return d.processStart(req.Broker, req.Port)
	case stop:
		return d.processStop()
	default:
		return fmt.Errorf("Invalid operation %s", req.Operation)
	}
}

func (d *Daemon) processStart(broker, port string) error {
	if d.broker != nil {
		return errors.New("Broker already running")
	}

	switch broker {
	case NATS:
		d.broker = &natsBroker{}
	default:
		return fmt.Errorf("Invalid broker %s", broker)
	}

	return d.broker.start(port)
}

func (d *Daemon) processStop() error {
	if d.broker == nil {
		return errors.New("No broker running")
	}

	err := d.broker.stop()
	if err == nil {
		d.broker = nil
	}
	return err
}
