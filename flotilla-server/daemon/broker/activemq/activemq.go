package activemq

import "gopkg.in/stomp.v1"

const queue = "test"

type ActiveMQPeer struct {
	conn   *stomp.Conn
	sub    *stomp.Subscription
	send   chan []byte
	errors chan error
	done   chan bool
}

func NewActiveMQPeer(host string) (*ActiveMQPeer, error) {
	conn, err := stomp.Dial("tcp", host, stomp.Options{})
	if err != nil {
		return nil, err
	}

	return &ActiveMQPeer{
		conn:   conn,
		send:   make(chan []byte),
		errors: make(chan error, 1),
		done:   make(chan bool),
	}, nil
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

func (a *ActiveMQPeer) Send() chan<- []byte {
	return a.send
}

func (a *ActiveMQPeer) Errors() <-chan error {
	return a.errors
}

func (a *ActiveMQPeer) Done() {
	a.done <- true
}

func (a *ActiveMQPeer) Setup() {
	go func() {
		for {
			select {
			case msg := <-a.send:
				if err := a.conn.Send(queue, "", msg, nil); err != nil {
					a.errors <- err
				}
			case <-a.done:
				return
			}
		}
	}()
}

func (a *ActiveMQPeer) Teardown() {
	if a.sub != nil {
		a.sub.Unsubscribe()
	}
	a.conn.Disconnect()
}
