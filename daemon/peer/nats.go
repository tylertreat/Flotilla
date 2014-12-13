package peer

import (
	"fmt"

	"github.com/apcera/nats"
)

const subject = "test"

type natsPeer struct {
	*nats.Conn
	messages chan []byte
}

func newNATS(host string) (*natsPeer, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s", host))
	if err != nil {
		return nil, err
	}

	messages := make(chan []byte, 100000)

	conn.Subscribe(subject, func(message *nats.Msg) {
		messages <- message.Data
	})

	return &natsPeer{conn, messages}, nil
}

func (n *natsPeer) recv() []byte {
	return <-n.messages
}

func (n *natsPeer) send(message []byte) {
	n.Publish(subject, message)
}
