package kestrel

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alindeman/go-kestrel"
)

const queue = "test"

type KestrelPeer struct {
	client   *kestrel.Client
	messages chan *kestrel.QueueItem
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
		messages: make(chan *kestrel.QueueItem, 100000),
	}, nil
}

func (k *KestrelPeer) Subscribe() error {
	go func() {
		for {
			// TODO: Probably tweak the max items number.
			items, err := k.client.Get(queue, 500, 0, 0)
			if err != nil {
				// Broker shutdown.
				return
			}
			for _, item := range items {
				k.messages <- item
			}
		}
	}()
	return nil
}

func (k *KestrelPeer) Recv() ([]byte, error) {
	item := <-k.messages
	return item.Data, nil
}

func (k *KestrelPeer) Send(message []byte) error {
	_, err := k.client.Put(queue, [][]byte{message})
	return err
}

func (k *KestrelPeer) Teardown() {
	k.client.Close()
}
