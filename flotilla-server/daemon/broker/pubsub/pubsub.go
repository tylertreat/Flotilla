package pubsub

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"golang.org/x/net/context"
	"google.golang.org/cloud/pubsub"

	"github.com/tylertreat/flotilla/flotilla-server/daemon/broker"
)

const stopped = 1

type CloudPubSubPeer struct {
	context      context.Context
	subscription string
	messages     chan []byte
	stopped      int32
	acks         chan []string
	done         chan bool
}

func NewCloudPubSubPeer(projectID, jsonKey string) (*CloudPubSubPeer, error) {
	ctx, err := newContext(projectID, jsonKey)
	if err != nil {
		return nil, err
	}

	return &CloudPubSubPeer{
		context:  ctx,
		messages: make(chan []byte, 10000),
		acks:     make(chan []string, 100),
		done:     make(chan bool, 1),
	}, nil
}

func (c *CloudPubSubPeer) Subscribe() error {
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
			messages, err := pubsub.PullWait(c.context, c.subscription, 100)
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

func (c *CloudPubSubPeer) Recv() ([]byte, error) {
	return <-c.messages, nil
}

func (c *CloudPubSubPeer) Send(message []byte) error {
	_, err := pubsub.Publish(c.context, topic, &pubsub.Message{Data: message})
	if err != nil {
		panic(err)
	}
	return err
}

func (c *CloudPubSubPeer) Teardown() {
	atomic.StoreInt32(&c.stopped, stopped)
	c.done <- true
	pubsub.DeleteSub(c.context, c.subscription)
}

func (c *CloudPubSubPeer) ack() {
	for {
		select {
		case ids := <-c.acks:
			if len(ids) > 0 {
				if err := pubsub.Ack(c.context, c.subscription, ids...); err != nil {
					log.Println("Failed to ack messages")
				}
			}
		case <-c.done:
			return
		}
	}
}
