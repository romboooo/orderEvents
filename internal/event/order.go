package event

import "time"

type orderCreated struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}
