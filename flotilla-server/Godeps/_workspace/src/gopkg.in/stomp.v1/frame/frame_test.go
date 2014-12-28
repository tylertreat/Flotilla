package frame

import (
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

func TestFrame(t *testing.T) {
	TestingT(t)
}

type FrameSuite struct{}

var _ = Suite(&FrameSuite{})

func (s *FrameSuite) TestParseHeartBeat(c *C) {
	testCases := []struct {
		Input             string
		ExpectedDuration1 time.Duration
		ExpectedDuration2 time.Duration
		ExpectError       bool
		ExpectedError     error
	}{
		{
			Input:             "0,0",
			ExpectedDuration1: 0,
			ExpectedDuration2: 0,
		},
		{
			Input:             "20000,60000",
			ExpectedDuration1: 20 * time.Second,
			ExpectedDuration2: time.Minute,
		},
		{
			Input:             "86400000,31536000000",
			ExpectedDuration1: 24 * time.Hour,
			ExpectedDuration2: 365 * 24 * time.Hour,
		},
		{
			Input:             "20r000,60000",
			ExpectedDuration1: 0,
			ExpectedDuration2: 0,
			ExpectedError:     invalidHeartBeat,
		},
		{
			Input:             "99999999999999999999,60000",
			ExpectedDuration1: 0,
			ExpectedDuration2: 0,
			ExpectedError:     invalidHeartBeat,
		},
		{
			Input:             "60000,99999999999999999999",
			ExpectedDuration1: 0,
			ExpectedDuration2: 0,
			ExpectedError:     invalidHeartBeat,
		},
		{
			Input:             "-60000,60000",
			ExpectedDuration1: 0,
			ExpectedDuration2: 0,
			ExpectedError:     invalidHeartBeat,
		},
		{
			Input:             "60000,-60000",
			ExpectedDuration1: 0,
			ExpectedDuration2: 0,
			ExpectedError:     invalidHeartBeat,
		},
	}

	for _, tc := range testCases {
		d1, d2, err := ParseHeartBeat(tc.Input)
		c.Check(d1, Equals, tc.ExpectedDuration1)
		c.Check(d2, Equals, tc.ExpectedDuration2)
		if tc.ExpectError || tc.ExpectedError != nil {
			c.Check(err, NotNil)
			if tc.ExpectedError != nil {
				c.Check(err, Equals, tc.ExpectedError)
			}
		} else {
			c.Check(err, IsNil)
		}
	}
}
