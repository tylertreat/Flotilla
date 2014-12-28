package queue

import (
	. "gopkg.in/check.v1"
)

type ManagerSuite struct{}

var _ = Suite(&ManagerSuite{})

func (s *ManagerSuite) TestManager(c *C) {
	mgr := NewManager(NewMemoryQueueStorage())

	q1 := mgr.Find("/queue/1")
	c.Assert(q1, NotNil)

	q2 := mgr.Find("/queue/2")
	c.Assert(q2, NotNil)

	c.Assert(mgr.Find("/queue/1"), Equals, q1)
}
