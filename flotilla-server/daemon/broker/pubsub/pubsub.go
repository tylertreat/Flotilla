package pubsub

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"golang.org/x/net/context"
	"google.golang.org/cloud/pubsub"

	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

const (
	stopped = 1

	// bufferSize is the number of messages we try to publish and consume at a
	// time to increase throughput. TODO: this might need tweaking.
	bufferSize = 100
)

// Peer implements the peer interface for Google Cloud Pub/Sub.
type Peer struct {
	context      context.Context
	subscription string
	messages     chan []byte
	stopped      int32
	acks         chan []string
	ackDone      chan bool
	send         chan []byte
	errors       chan error
	done         chan bool
	flush        chan bool
}

// NewPeer creates and returns a new Peer for communicating with Google Cloud
// Pub/Sub.
func NewPeer(projectID, jsonKey string) (*Peer, error) {
	ctx, err := newContext(projectID, jsonKey)
	if err != nil {
		return nil, err
	}

	return &Peer{
		context:  ctx,
		messages: make(chan []byte, 10000),
		acks:     make(chan []string, 100),
		ackDone:  make(chan bool, 1),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
		flush:    make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (c *Peer) Subscribe() error {
	// Subscription names must start with a lowercase letter, end with a
	// lowercase letter or number, and contain only lowercase letters, numbers,
	// dashes, underscores or periods.
	c.subscription = strings.ToLower(fmt.Sprintf("x%sx", broker.GenerateName()))
	exists, err := pubsub.SubExists(c.context, c.subscription)
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf("Subscription %s already exists", c.subscription)
	}

	if err := pubsub.CreateSub(c.context, c.subscription, topic, 0, ""); err != nil {
		return err
	}

	go c.ack()

	go func() {
		// TODO: Can we avoid using atomic flag?
		for atomic.LoadInt32(&c.stopped) != stopped {
			messages, err := pubsub.PullWait(c.context, c.subscription, bufferSize)
			if err != nil {
				// Timed out.
				continue
			}

			ids := make([]string, len(messages))
			for i, message := range messages {
				ids[i] = message.AckID
				c.messages <- message.Data
			}
			c.acks <- ids
		}
	}()
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (c *Peer) Recv() ([]byte, error) {
	return <-c.messages, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (c *Peer) Send() chan<- []byte {
	return c.send
}

// Errors returns the channel on which the peer sends publish errors.
func (c *Peer) Errors() <-chan error {
	return c.errors
}

// Done signals to the peer that message publishing has completed.
func (c *Peer) Done() {
	c.done <- true
	<-c.flush
}

// Setup prepares the peer for testing.
func (c *Peer) Setup() {
	buffer := make([]*pubsub.Message, bufferSize)
	go func() {
		i := 0
		for {
			select {
			case msg := <-c.send:
				buffer[i] = &pubsub.Message{Data: msg}
				i++
				if i == bufferSize {
					if _, err := pubsub.Publish(c.context, topic, buffer...); err != nil {
						c.errors <- err
					}
					i = 0
				}
			case <-c.done:
				if i > 0 {
					if _, err := pubsub.Publish(c.context, topic, buffer[0:i]...); err != nil {
						c.errors <- err
					}
				}
				c.flush <- true
				return
			}
		}
	}()
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (c *Peer) Teardown() {
	atomic.StoreInt32(&c.stopped, stopped)
	c.ackDone <- true
	pubsub.DeleteSub(c.context, c.subscription)
}

func (c *Peer) ack() {
	for {
		select {
		case ids := <-c.acks:
			if len(ids) > 0 {
				if err := pubsub.Ack(c.context, c.subscription, ids...); err != nil {
					log.Println("Failed to ack messages")
				}
			}
		case <-c.ackDone:
			return
		}
	}
}
