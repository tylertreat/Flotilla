package kestrel

import (
	"bytes"
	"testing"
)

func TestReadIntoChannel(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	reader := NewClusterReader([]*Client{client})

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	ch := make(chan *QueueItem, 1)
	go reader.ReadIntoChannel("queue1", ch)

	item := <-ch
	if !bytes.Equal(items[0], item.Data) {
		t.Fatalf("Byte sequence differed from expected: %v", items[0])
	}

	reader.Close()
}
