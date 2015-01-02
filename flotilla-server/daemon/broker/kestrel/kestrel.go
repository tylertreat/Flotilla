package kestrel

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alindeman/go-kestrel"
)

const (
	queue = "test"

	// bufferSize is the number of messages we try to publish and consume at a
	// time to increase throughput. TODO: this might need tweaking.
	bufferSize = 100
)

// Peer implements the peer interface for Kestrel.
type Peer struct {
	client     *kestrel.Client
	messages   chan []byte
	send       chan []byte
	errors     chan error
	done       chan bool
	flush      chan bool
	subscriber bool
}

// NewPeer creates and returns a new Peer for communicating with Kestrel.
func NewPeer(host string) (*Peer, error) {
	addrAndPort := strings.Split(host, ":")
	if len(addrAndPort) < 2 {
		return nil, fmt.Errorf("Invalid host: %s", host)
	}

	port, err := strconv.Atoi(addrAndPort[1])
	if err != nil {
		return nil, err
	}

	client := kestrel.NewClient(addrAndPort[0], port)
	if err := client.FlushAllQueues(); err != nil {
		client.Close()
		return nil, err
	}

	return &Peer{
		client:   client,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
		flush:    make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (k *Peer) Subscribe() error {
	k.subscriber = true
	go func() {
		for {
			items, err := k.client.Get(queue, bufferSize, 0, 0)
			if err != nil {
				// Broker shutdown.
				return
			}
			for _, item := range items {
				k.messages <- item.Data
			}
		}
	}()
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (k *Peer) Recv() ([]byte, error) {
	return <-k.messages, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (k *Peer) Send() chan<- []byte {
	return k.send
}

// Errors returns the channel on which the peer sends publish errors.
func (k *Peer) Errors() <-chan error {
	return k.errors
}

// Done signals to the peer that message publishing has completed.
func (k *Peer) Done() {
	k.done <- true
	<-k.flush
}

// Setup prepares the peer for testing.
func (k *Peer) Setup() {
	buffer := make([][]byte, bufferSize)
	go func() {
		i := 0
		for {
			select {
			case msg := <-k.send:
				buffer[i] = msg
				i++
				if i == bufferSize {
					if _, err := k.client.Put(queue, buffer); err != nil {
						k.errors <- err
					}
					i = 0
				}
			case <-k.done:
				if i > 0 {
					if _, err := k.client.Put(queue, buffer[0:i]); err != nil {
						k.errors <- err
					}
				}
				k.flush <- true
				return
			}
		}
	}()
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (k *Peer) Teardown() {
	k.client.Close()
}
