package activemq

import "gopkg.in/stomp.v1"

const queue = "test"

type ActiveMQPeer struct {
	conn *stomp.Conn
	sub  *stomp.Subscription
}

func NewActiveMQPeer(host string) (*ActiveMQPeer, error) {
	conn, err := stomp.Dial("tcp", host, stomp.Options{})
	if err != nil {
		return nil, err
	}

	return &ActiveMQPeer{conn: conn}, nil
}

func (a *ActiveMQPeer) Subscribe() error {
	sub, err := a.conn.Subscribe(queue, stomp.AckAuto)
	if err != nil {
		return err
	}

	a.sub = sub
	return nil
}

func (a *ActiveMQPeer) Recv() ([]byte, error) {
	message := <-a.sub.C
	return message.Body, message.Err
}

func (a *ActiveMQPeer) Send(message []byte) error {
	return a.conn.Send(queue, "", message, nil)
}

func (a *ActiveMQPeer) Teardown() {
	if a.sub != nil {
		a.sub.Unsubscribe()
	}
	a.conn.Disconnect()
}
