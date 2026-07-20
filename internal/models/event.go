package models

import (
	"encoding/json"
	"time"
)

// Event type constants.
const (
	EventTypeOrderCreate       = "order.create"
	EventTypeOrderUpdateStatus = "order.update.status"
	EventTypeOrderUpdateItems  = "order.update.items"
)

// EventEnvelope wraps every inbound event with metadata used for
// idempotency, ordering, and audit.
type EventEnvelope struct {
	EventID   string          `json:"eventId"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// OrderCreatePayload is the body of an order.create event.
type OrderCreatePayload struct {
	CustomerID   string `json:"customerId"`
	RestaurantID string `json:"restaurantId"`
	Items        []Item `json:"items"`
}

// OrderUpdateStatusPayload is the body of an order.update.status event.
type OrderUpdateStatusPayload struct {
	OrderID string      `json:"orderId"`
	Status  OrderStatus `json:"status"`
}

// OrderUpdateItemsPayload is the body of an order.update.items event.
type OrderUpdateItemsPayload struct {
	OrderID string `json:"orderId"`
	Items   []Item `json:"items"`
}
