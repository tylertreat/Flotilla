package client

import (
	. "gopkg.in/check.v1"
)

type SubscriptionListSuite struct{}

var _ = Suite(&SubscriptionListSuite{})

func (s *SubscriptionListSuite) TestAddAndGet(c *C) {
	sub1 := newSubscription(nil, "/dest", "1", "client")
	sub2 := newSubscription(nil, "/dest", "2", "client")
	sub3 := newSubscription(nil, "/dest", "3", "client")

	sl := NewSubscriptionList()
	sl.Add(sub1)
	sl.Add(sub2)
	sl.Add(sub3)

	c.Check(sl.Get(), Equals, sub1)

	// add the subscription again, should go to the back
	sl.Add(sub1)

	c.Check(sl.Get(), Equals, sub2)
	c.Check(sl.Get(), Equals, sub3)
	c.Check(sl.Get(), Equals, sub1)

	c.Check(sl.Get(), IsNil)
}

func (s *SubscriptionListSuite) TestAddAndRemove(c *C) {
	sub1 := newSubscription(nil, "/dest", "1", "client")
	sub2 := newSubscription(nil, "/dest", "2", "client")
	sub3 := newSubscription(nil, "/dest", "3", "client")

	sl := NewSubscriptionList()
	sl.Add(sub1)
	sl.Add(sub2)
	sl.Add(sub3)

	c.Check(sl.subs.Len(), Equals, 3)

	// now remove the second subscription
	sl.Remove(sub2)

	c.Check(sl.Get(), Equals, sub1)
	c.Check(sl.Get(), Equals, sub3)
	c.Check(sl.Get(), IsNil)
}

func (s *SubscriptionListSuite) TestAck(c *C) {
	sub1 := &Subscription{dest: "/dest1", id: "1", ack: "client", msgId: 101}
	sub2 := &Subscription{dest: "/dest3", id: "2", ack: "client-individual", msgId: 102}
	sub3 := &Subscription{dest: "/dest4", id: "3", ack: "client", msgId: 103}
	sub4 := &Subscription{dest: "/dest4", id: "4", ack: "client", msgId: 104}

	sl := NewSubscriptionList()
	sl.Add(sub1)
	sl.Add(sub2)
	sl.Add(sub3)
	sl.Add(sub4)

	c.Check(sl.subs.Len(), Equals, 4)

	var subs []*Subscription
	callback := func(s *Subscription) {
		subs = append(subs, s)
	}

	// now remove the second subscription
	sl.Ack(103, callback)

	c.Assert(len(subs), Equals, 2)
	c.Assert(subs[0], Equals, sub1)
	c.Assert(subs[1], Equals, sub3)

	c.Assert(sl.Get(), Equals, sub2)
	c.Assert(sl.Get(), Equals, sub4)
	c.Assert(sl.Get(), IsNil)
}

func (s *SubscriptionListSuite) TestNack(c *C) {
	sub1 := &Subscription{dest: "/dest1", id: "1", ack: "client", msgId: 101}
	sub2 := &Subscription{dest: "/dest3", id: "2", ack: "client-individual", msgId: 102}
	sub3 := &Subscription{dest: "/dest4", id: "3", ack: "client", msgId: 103}
	sub4 := &Subscription{dest: "/dest4", id: "4", ack: "client", msgId: 104}

	sl := NewSubscriptionList()
	sl.Add(sub1)
	sl.Add(sub2)
	sl.Add(sub3)
	sl.Add(sub4)

	c.Check(sl.subs.Len(), Equals, 4)

	var subs []*Subscription
	callback := func(s *Subscription) {
		subs = append(subs, s)
	}

	// now remove the second subscription
	sl.Nack(103, callback)

	c.Assert(len(subs), Equals, 1)
	c.Assert(subs[0], Equals, sub3)

	c.Assert(sl.Get(), Equals, sub1)
	c.Assert(sl.Get(), Equals, sub2)
	c.Assert(sl.Get(), Equals, sub4)
	c.Assert(sl.Get(), IsNil)
}
