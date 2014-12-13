package daemon

import (
	"errors"
	"fmt"

	"github.com/tylertreat/flotillad/daemon/nats"
)

const (
	start operation = "start"
	stop  operation = "stop"
)

const (
	NATS = "nats"
)

type broker interface {
	Start(string) error
	Stop() error
}

type brokerDaemon struct {
	broker broker
}

func (d *brokerDaemon) processRequest(req request) error {
	switch req.Operation {
	case start:
		return d.processStart(req.Broker, req.Port)
	case stop:
		return d.processStop()
	default:
		return fmt.Errorf("Invalid operation %s", req.Operation)
	}
}

func (d *brokerDaemon) processStart(broker, port string) error {
	if d.broker != nil {
		return errors.New("Broker already running")
	}

	switch broker {
	case NATS:
		d.broker = &nats.NATSBroker{}
	default:
		return fmt.Errorf("Invalid broker %s", broker)
	}

	return d.broker.Start(port)
}

func (d *brokerDaemon) processStop() error {
	if d.broker == nil {
		return errors.New("No broker running")
	}

	err := d.broker.Stop()
	if err == nil {
		d.broker = nil
	}
	return err
}
