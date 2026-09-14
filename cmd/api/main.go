package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"orderEvents/internal/event"
	"time"
)

type CreateOrderRequest struct {
	UserID string `json:"user_id"`
	Amount int64  `json:"amount"`
}

type CreateOrderResponse struct {
	Status string `json:"status"`
}

func generateOrderID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate random order id: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	var request CreateOrderRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if request.UserID == "" {
		http.Error(w, "user id is required", http.StatusBadRequest)
		return
	}
	if request.Amount <= 0 {
		http.Error(w, "amount must be positive digit", http.StatusBadRequest)
		return
	}

	orderID, err := generateOrderID()
	if err != nil {
		log.Printf("failed to generate order id: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	orderEvent := event.OrderCreated{
		OrderID:   orderID,
		UserID:    request.UserID,
		Amount:    request.Amount,
		CreatedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(orderEvent)

	if err != nil {
		log.Printf("failed to marshal order event: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	//todo дописать

	response := CreateOrderResponse{
		Status: "accepted!",
	}

	w.Header().Set("Content=Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /orders", createOrderHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("http server starter on :8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("http server stopped %v", err)
	}
}
