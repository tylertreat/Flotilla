package kestrel

import (
	"testing"
)

const kestrelMemcachedPort = 22134

func TestQueueList(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	queueNames, err := QueueNames(kestrelTestHost, kestrelMemcachedPort)
	if err != nil {
		t.Error(err)
	}

	found := false
	for _, queueName := range queueNames {
		if queueName == "queue1" {
			found = true
		}
	}

	if !found {
		t.Fatalf("Did not find queue1 in the list of queues: %v", queueNames)
	}
}
