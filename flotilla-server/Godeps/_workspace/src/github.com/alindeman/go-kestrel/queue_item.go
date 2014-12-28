package kestrel

import (
	"fmt"
)

type QueueItem struct {
	*Item

	queueName string
	client    *Client
}

func NewQueueItem(item *Item, queueName string, client *Client) *QueueItem {
	return &QueueItem{item, queueName, client}
}

func NewQueueItems(items []*Item, queueName string, client *Client) []*QueueItem {
	queueItems := make([]*QueueItem, len(items))
	for i, item := range items {
		queueItems[i] = NewQueueItem(item, queueName, client)
	}

	return queueItems
}

func (q *QueueItem) Confirm() error {
	nitems, err := q.client.Confirm(q.queueName, []*QueueItem{q})
	if err != nil {
		return err
	} else if nitems != 1 {
		return fmt.Errorf("Confirmation of item failed; nitems = %d", nitems)
	} else {
		return nil
	}
}

func (q *QueueItem) Abort() error {
	nitems, err := q.client.Abort(q.queueName, []*QueueItem{q})
	if err != nil {
		return err
	} else if nitems != 1 {
		return fmt.Errorf("Aborting item failed; nitems = %d", nitems)
	} else {
		return nil
	}
}
