package nats

import (
	"fmt"
	"time"

	"github.com/apcera/nats"
)

const (
	subject = "test"

	// Maximum bytes we will get behind before we start slowing down publishing.
	maxBytesBehind = 1024 * 1024 // 1MB

	// Maximum msgs we will get behind before we start slowing down publishing.
	maxMsgsBehind = 65536 // 64k

	// Time to delay publishing when we are behind.
	delay = 1 * time.Millisecond
)

type NATSPeer struct {
	*nats.Conn
	messages chan []byte
}

func NewNATS(host string) (*NATSPeer, error) {
	conn, err := nats.Connect(fmt.Sprintf("nats://%s", host))
	if err != nil {
		return nil, err
	}

	// We want to be alerted if we get disconnected, this will be due to Slow
	// Consumer.
	conn.Opts.AllowReconnect = false

	// Report async errors.
	conn.Opts.AsyncErrorCB = func(nc *nats.Conn, sub *nats.Subscription, err error) {
		panic(fmt.Sprintf("NATS: Received an async error! %v\n", err))
	}

	// Report a disconnect scenario.
	conn.Opts.DisconnectedCB = func(nc *nats.Conn) {
		fmt.Printf("Getting behind! %d\n", nc.OutMsgs-nc.InMsgs)
		panic("NATS: Got disconnected!")
	}

	messages := make(chan []byte, 1000000)

	conn.Subscribe(subject, func(message *nats.Msg) {
		messages <- message.Data
	})

	return &NATSPeer{conn, messages}, nil
}

func (n *NATSPeer) Recv() []byte {
	return <-n.messages
}

func (n *NATSPeer) Send(message []byte) {
	// Check if we are behind by >= 1MB bytes.
	bytesDeltaOver := n.Conn.OutBytes-n.Conn.InBytes >= maxBytesBehind

	// Check if we are behind by >= 65k msgs.
	msgsDeltaOver := n.Conn.OutMsgs-n.Conn.InMsgs >= maxMsgsBehind

	// If we are behind on either condition, sleep a bit to catch up receiver.
	if bytesDeltaOver || msgsDeltaOver {
		time.Sleep(delay)
	}

	n.Publish(subject, message)
}
