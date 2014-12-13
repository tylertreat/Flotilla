package peer

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/tcp"
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

type request struct {
	Operation   operation `json:"operation"`
	NumMessages int       `json:"num_messages"`
	MessageSize int64     `json:"message_size"`
	Test        test      `json:"test"`
	Count       int       `json:"count"`
}

type response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

func (r request) isValid() bool {
	return r.Operation == sub || r.Operation == pub || r.Operation == start
}

// Daemon is an implementation of the Daemon interface which is responsible for
// orchestrating broker publishers and subscribers on the host machine.
type Daemon struct {
	mangos.Socket
}

// NewDaemon creates and returns a new peer Daemon. An error is returned if
// the Daemon could not be created successfully.
func NewDaemon() (*Daemon, error) {
	rep, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	rep.AddTransport(tcp.NewTransport())
	return &Daemon{rep}, nil
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

		if !req.isValid() {
			log.Println("Invalid operation:", req.Operation)
			d.sendResponse(response{
				Success: false,
				Message: fmt.Sprintf("Invalid operation: %s", req.Operation),
			})
			continue
		}

		if err := processRequest(req); err != nil {
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

func processRequest(req request) error {
	fmt.Println(req.Operation)
	return nil
}
