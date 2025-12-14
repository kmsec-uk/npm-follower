package follower

import (
	"context"
	"testing"
)

func TestColdStart(t *testing.T) {
	s := NewFollower()
	err := s.coldStartSequence(context.TODO())
	if err != nil {
		t.Errorf("cold start: %v", err)
	}
}
