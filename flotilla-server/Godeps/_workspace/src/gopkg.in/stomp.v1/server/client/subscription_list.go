package client

import (
	"container/list"
)

// Maintains a list of subscriptions. Not thread-safe.
type SubscriptionList struct {
	// TODO: implement linked list locally, adding next and prev
	// pointers to the Subscription struct itself.
	subs *list.List
}

func NewSubscriptionList() *SubscriptionList {
	return &SubscriptionList{list.New()}
}

// Add a subscription to the back of the list. Will panic if
// the subscription destination does not match the subscription
// list destination. Will also panic if the subscription has already
// been added to a subscription list.
func (sl *SubscriptionList) Add(sub *Subscription) {
	if sub.subList != nil {
		panic("subscription is already in a subscription list")
	}
	sl.subs.PushBack(sub)
	sub.subList = sl
}

// Gets the first subscription in the list, or nil if there
// are no subscriptions available. The subscription is removed
// from the list.
func (sl *SubscriptionList) Get() *Subscription {
	if sl.subs.Len() == 0 {
		return nil
	}
	front := sl.subs.Front()
	sub := front.Value.(*Subscription)
	sl.subs.Remove(front)
	sub.subList = nil
	return sub
}

// Removes the subscription from the list.
func (sl *SubscriptionList) Remove(s *Subscription) {
	for e := sl.subs.Front(); e != nil; e = e.Next() {
		if e.Value.(*Subscription) == s {
			sl.subs.Remove(e)
			s.subList = nil
			return
		}
	}
}

// Search for a subscription with the specified id and remove it.
// Returns a pointer to the subscription if found, nil otherwise.
func (sl *SubscriptionList) FindByIdAndRemove(id string) *Subscription {
	for e := sl.subs.Front(); e != nil; e = e.Next() {
		sub := e.Value.(*Subscription)
		if sub.id == id {
			sl.subs.Remove(e)
			return sub
		}
	}
	return nil
}

// Finds all subscriptions in the subscription list that are acked by the
// specified message-id (or ack) header. The subscription is removed from
// the list and the callback function called for that subscription.
func (sl *SubscriptionList) Ack(msgId uint64, callback func(s *Subscription)) {
	for e := sl.subs.Front(); e != nil; {
		next := e.Next()
		sub := e.Value.(*Subscription)
		if sub.IsAckedBy(msgId) {
			sl.subs.Remove(e)
			callback(sub)
		}
		e = next
	}
}

// Finds all subscriptions in the subscription list that are *nacked* by the
// specified message-id (or ack) header. The subscription is removed from
// the list and the callback function called for that subscription. Current
// understanding that all NACKs are individual, but not sure
func (sl *SubscriptionList) Nack(msgId uint64, callback func(s *Subscription)) {
	for e := sl.subs.Front(); e != nil; {
		next := e.Next()
		sub := e.Value.(*Subscription)
		if sub.IsNackedBy(msgId) {
			sl.subs.Remove(e)
			callback(sub)
		}
		e = next
	}
}

// Invoke a callback function for every subscription in the list.
func (sl *SubscriptionList) ForEach(callback func(s *Subscription, isLast bool)) {
	for e := sl.subs.Front(); e != nil; {
		next := e.Next()
		sub := e.Value.(*Subscription)
		callback(sub, next == nil)
		e = next
	}
}
