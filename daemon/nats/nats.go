package nats

import (
	"fmt"

	"github.com/apcera/nats"
)

const subject = "test"

type NATSPeer struct {
	*nats.Conn
	messages chan []byte
}

func NewNATS(host string) (*NATSPeer, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s", host))
	if err != nil {
		return nil, err
	}

	messages := make(chan []byte, 100000)

	conn.Subscribe(subject, func(message *nats.Msg) {
		messages <- message.Data
	})

	return &NATSPeer{conn, messages}, nil
}

func (n *NATSPeer) Recv() []byte {
	return <-n.messages
}

func (n *NATSPeer) Send(message []byte) {
	n.Publish(subject, message)
}
