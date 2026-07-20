package models

import (
	"time"
)

// OrderStatus represents the lifecycle state of an order.
type OrderStatus string

const (
	StatusReceived  OrderStatus = "Received"
	StatusPreparing OrderStatus = "Preparing"
	StatusComplete  OrderStatus = "Complete"
	StatusCancelled OrderStatus = "Cancelled"
)

// ValidStatuses is the set of allowed status values.
var ValidStatuses = map[OrderStatus]bool{
	StatusReceived:  true,
	StatusPreparing: true,
	StatusComplete:  true,
	StatusCancelled: true,
}

// Item is a line item on an order.
type Item struct {
	ItemID string `json:"itemId"`
	Qty    int    `json:"qty"`
}

// Order is the current materialized state of an order.
type Order struct {
	OrderID      string      `json:"orderId"`
	CustomerID   string      `json:"customerId"`
	RestaurantID string      `json:"restaurantId"`
	Items        []Item      `json:"items"`
	Status       OrderStatus `json:"status"`
	CreatedAt    time.Time   `json:"createdAt"`
	UpdatedAt    time.Time   `json:"updatedAt"`
	Version      int64       `json:"-"` // optimistic concurrency control
}

// CanTransition reports whether moving from current to next is allowed.
// Terminal states (Complete, Cancelled) accept no further transitions.
// Cancelled may be reached from Received or Preparing only.
func CanTransition(from, to OrderStatus) bool {
	if from == to {
		return true // idempotent no-op
	}
	switch from {
	case StatusReceived:
		return to == StatusPreparing || to == StatusCancelled
	case StatusPreparing:
		return to == StatusComplete || to == StatusCancelled
	case StatusComplete, StatusCancelled:
		return false
	default:
		return false
	}
}
