package kestrel

import (
	"bytes"
	"testing"
	"time"
)

const kestrelTestHost = "localhost"
const kestrelThriftPort = 2229

func TestPeek(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	info, err := client.Peek("queue1")
	if err != nil {
		t.Fatalf("Error occured while peeking at a queue: %v", err)
	}
	if info.Items != 1 {
		t.Fatalf("Expected 1 item in the queue, got %d", info.Items)
	}
}

func TestSimplePutAndGetToAndFromServer(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	items := [][]byte{[]byte("Hello World")}
	nitems, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}
	if nitems != 1 {
		t.Fatalf("Did not write 1 item to the queue")
	}

	gitems, err := client.Get("queue1", 1, 0, 0)
	if err != nil {
		t.Fatalf("Error occured getting an item from the queue: %v", err)
	}

	if len(gitems) != 1 {
		t.Fatalf("Did not receive one and only one item from the queue")
	}
	if !bytes.Equal(items[0], gitems[0].Data) {
		t.Fatalf("Byte sequence differed from expected: %v", items[0])
	}
}

func TestConfirm(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	gitems, err := client.Get("queue1", 1, 0, 1*time.Minute)
	if err != nil {
		t.Fatalf("Error occured getting an item from the queue: %v", err)
	}

	_, err = client.Confirm("queue1", gitems)
	if err != nil {
		t.Fatalf("Error occured while confirming an item: %v", err)
	}

	gitems, err = client.Get("queue1", 1, 0, 1*time.Minute)
	if len(gitems) > 0 {
		t.Fatalf("Fetched an item even after confirming it: %v", items[0])
	}
}

func TestAbort(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	gitems, err := client.Get("queue1", 1, 0, 1*time.Minute)
	if err != nil {
		t.Fatalf("Error occured getting an item from the queue: %v", err)
	}

	_, err = client.Abort("queue1", gitems)
	if err != nil {
		t.Fatalf("Error occured while confirming an item: %v", err)
	}

	gitems, err = client.Get("queue1", 1, 0, 1*time.Minute)
	if len(gitems) < 1 {
		t.Fatalf("Was not able to fetch an item even after aborting it")
	}
}

func TestConfirmAfterDisconnect(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	gitems, err := client.Get("queue1", 1, 0, 1*time.Minute)
	if err != nil {
		t.Fatalf("Error occured getting an item from the queue: %v", err)
	}

	client.Close()

	err = gitems[0].Confirm()
	if err != nil {
		t.Fatalf("Error occured while confirming an item: %v", err)
	}

	gitems, err = client.Get("queue1", 1, 0, 1*time.Minute)
	if len(gitems) > 0 {
		t.Fatalf("Fetched an item even after confirming it: %v", items[0])
	}
}

func TestConfirmAfterTimeout(t *testing.T) {
	client := NewClient(kestrelTestHost, kestrelThriftPort)
	client.FlushAllQueues()

	items := [][]byte{[]byte("Hello World")}
	_, err := client.Put("queue1", items)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}

	gitems, err := client.Get("queue1", 1, 0, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("Error occured getting an item from the queue: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	err = gitems[0].Confirm()
	if err == nil {
		t.Fatalf("Expected an error when attempting to confirm an item whose timeout has expired")
	}

	gitems, err = client.Get("queue1", 1, 0, 1*time.Minute)
	if len(gitems) < 1 {
		t.Fatalf("Was not able to fetch an item even after aborting it")
	}
}
