package daemon

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/tcp"
)

type daemon string
type operation string
type test string

const (
	brokerd daemon = "broker"
	peerd   daemon = "peer"
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
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type Daemon struct {
	mangos.Socket
	peerDaemon   *peerDaemon
	brokerDaemon *brokerDaemon
}

func NewDaemon() (*Daemon, error) {
	rep, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	rep.AddTransport(tcp.NewTransport())
	return &Daemon{
		rep,
		&peerDaemon{[]*publisher{}},
		&brokerDaemon{},
	}, nil
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

		switch req.Daemon {
		case brokerd:
			if err := d.brokerDaemon.processRequest(req); err != nil {
				d.sendResponse(response{
					Success: false,
					Message: err.Error(),
				})
				continue
			}
		case peerd:
			if err := d.peerDaemon.processRequest(req); err != nil {
				d.sendResponse(response{
					Success: false,
					Message: err.Error(),
				})
				continue
			}
		default:
			log.Printf("Invalid daemon: %s", req.Daemon)
			d.sendResponse(response{
				Success: false,
				Message: fmt.Sprintf("Invalid daemon: %s", req.Daemon),
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
