package daemon

import (
	"fmt"

	"github.com/tylertreat/flotillad/daemon/nats"
)

const (
	sub        operation = "subscribers"
	pub        operation = "publishers"
	latency    test      = "latency"
	throughput test      = "throughput"
)

type peer interface {
	Recv() []byte
	Send([]byte)
}

type peerDaemon struct {
	publishers []*publisher
}

func (d *peerDaemon) processRequest(req request) error {
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

func (d *peerDaemon) processPub(broker, host string, count, numMessages int,
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

func (d *peerDaemon) processSub(broker, host string, count, numMessages int,
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

func (d *peerDaemon) processStart() error {
	for _, publisher := range d.publishers {
		go publisher.start()
	}

	// TODO collect results and send them back.
	return nil
}

func newPeer(broker, host string) (peer, error) {
	switch broker {
	case NATS:
		return nats.NewNATS(host)
	default:
		return nil, fmt.Errorf("Invalid broker: %s", broker)
	}
}
