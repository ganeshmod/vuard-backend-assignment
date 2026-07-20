package models

import "testing"

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to OrderStatus
		ok       bool
	}{
		{StatusReceived, StatusPreparing, true},
		{StatusReceived, StatusCancelled, true},
		{StatusReceived, StatusComplete, false},
		{StatusPreparing, StatusComplete, true},
		{StatusPreparing, StatusCancelled, true},
		{StatusPreparing, StatusReceived, false},
		{StatusComplete, StatusPreparing, false},
		{StatusCancelled, StatusReceived, false},
		{StatusReceived, StatusReceived, true},
	}
	for _, tc := range cases {
		got := CanTransition(tc.from, tc.to)
		if got != tc.ok {
			t.Fatalf("%s -> %s: got %v want %v", tc.from, tc.to, got, tc.ok)
		}
	}
}
