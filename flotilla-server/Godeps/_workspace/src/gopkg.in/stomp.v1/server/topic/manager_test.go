package topic

import (
	. "gopkg.in/check.v1"
)

type ManagerSuite struct{}

var _ = Suite(&ManagerSuite{})

func (s *ManagerSuite) TestManager(c *C) {
	mgr := NewManager()

	t1 := mgr.Find("topic1")
	c.Assert(t1, NotNil)

	t2 := mgr.Find("topic2")
	c.Assert(t2, NotNil)

	c.Assert(mgr.Find("topic1"), Equals, t1)
}
