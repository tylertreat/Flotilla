package kestrel

import (
	"sync"
	"time"
)

type ClusterReader struct {
	GetTimeout   time.Duration
	AbortTimeout time.Duration

	clients []*Client

	closed chan struct{}
	wg     sync.WaitGroup
}

func NewClusterReader(clients []*Client) *ClusterReader {
	return &ClusterReader{
		clients: clients,

		// defaults
		GetTimeout:   250 * time.Millisecond,
		AbortTimeout: 1 * time.Minute,
	}
}

func (r *ClusterReader) ReadIntoChannel(queueName string, ch chan<- *QueueItem) {
	r.closed = make(chan struct{})

	for _, client := range r.clients {
		r.wg.Add(1)

		go func(client *Client, queueName string, ch chan<- *QueueItem, closed chan struct{}) {
			defer r.wg.Done()
			defer client.Close()

			for {
				items, err := client.Get(queueName, 1, r.GetTimeout, r.AbortTimeout)
				if err != nil || len(items) == 0 {
					<-time.After(r.GetTimeout)
				} else {
					for _, item := range items {
						ch <- item
					}
				}

				select {
				case <-closed:
					return
				default:
					continue
				}
			}
		}(client, queueName, ch, r.closed)
	}

	r.wg.Wait()
}

func (r *ClusterReader) Close() error {
	if r.closed != nil {
		close(r.closed)
	}

	return nil
}
