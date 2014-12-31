package kestrel

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alindeman/go-kestrel"
)

const (
	queue      = "test"
	bufferSize = 10
)

type KestrelPeer struct {
	client     *kestrel.Client
	messages   chan []byte
	send       chan []byte
	errors     chan error
	done       chan bool
	flush      chan bool
	subscriber bool
}

func NewKestrelPeer(host string) (*KestrelPeer, error) {
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

	return &KestrelPeer{
		client:   client,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool, 1),
		flush:    make(chan bool),
	}, nil
}

func (k *KestrelPeer) Subscribe() error {
	k.subscriber = true
	go func() {
		for {
			// TODO: Probably tweak the max items number.
			items, err := k.client.Get(queue, 500, 0, 0)
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

func (k *KestrelPeer) Recv() ([]byte, error) {
	return <-k.messages, nil
}

func (k *KestrelPeer) Send() chan<- []byte {
	return k.send
}

func (k *KestrelPeer) Errors() <-chan error {
	return k.errors
}

func (k *KestrelPeer) Setup() {
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

func (k *KestrelPeer) Teardown() {
	k.done <- true
	if !k.subscriber {
		<-k.flush
	}
	k.client.Close()
}
