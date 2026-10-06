package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/worker"
)

// ---------------- 1. order.confirmation_email ----------------

type EmailPayload struct {
	OrderID    string `json:"order_id"`
	Email      string `json:"email"`
	OrderTotal int64  `json:"order_total"`
}

type EmailHandler struct {
	mu       sync.Mutex
	sent     map[string]int // order_id -> count of emails sent
	failRate float64
	rng      *rand.Rand
}

func NewEmailHandler(failRate float64) *EmailHandler {
	return &EmailHandler{
		sent:     make(map[string]int),
		failRate: failRate,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *EmailHandler) Run(ctx context.Context, job core.Job) error {
	var payload EmailPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("%w: invalid email payload: %v", worker.ErrPermanent, err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Handler idempotency: If already sent for this order_id, skip re-sending
	if h.sent[payload.OrderID] > 0 {
		return nil
	}

	// Simulated transient provider failure
	if h.failRate > 0 && h.rng.Float64() < h.failRate {
		return errors.New("smtp: connection reset by peer")
	}

	h.sent[payload.OrderID]++
	return nil
}

func (h *EmailHandler) SentCount(orderID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sent[orderID]
}

// ---------------- 2. inventory.reserve ----------------

type InventoryPayload struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

type InventoryHandler struct {
	mu           sync.Mutex
	stock        map[string]int         // SKU -> available stock
	reservations map[string]int         // orderID -> reserved quantity
	applied      map[string]uint64      // orderID -> highest applied LeaseToken
}

func NewInventoryHandler(initialStock map[string]int) *InventoryHandler {
	st := make(map[string]int)
	for k, v := range initialStock {
		st[k] = v
	}
	return &InventoryHandler{
		stock:        st,
		reservations: make(map[string]int),
		applied:      make(map[string]uint64),
	}
}

func (h *InventoryHandler) Run(ctx context.Context, job core.Job) error {
	var payload InventoryPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("%w: invalid inventory payload: %v", worker.ErrPermanent, err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Strict idempotency using fencing token:
	// If already applied for this order, skip re-decrementing
	if lastToken, exists := h.applied[payload.OrderID]; exists {
		if job.LeaseToken <= lastToken {
			// Already applied by this or earlier attempt
			return nil
		}
	}

	currentStock := h.stock[payload.SKU]
	if currentStock < payload.Qty {
		return fmt.Errorf("%w: insufficient stock for SKU %s (have %d, want %d)", worker.ErrPermanent, payload.SKU, currentStock, payload.Qty)
	}

	h.stock[payload.SKU] -= payload.Qty
	h.reservations[payload.OrderID] += payload.Qty
	h.applied[payload.OrderID] = job.LeaseToken

	return nil
}

func (h *InventoryHandler) Stock(sku string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stock[sku]
}

func (h *InventoryHandler) Reserved(orderID string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reservations[orderID]
}

// ---------------- 3. payment.capture ----------------

type PaymentPayload struct {
	OrderID        string `json:"order_id"`
	AmountCents    int64  `json:"amount_cents"`
	IdempotencyKey string `json:"idempotency_key"`
}

type PaymentHandler struct {
	mu       sync.Mutex
	captured map[string]int64 // idempotencyKey -> amount
	failRate float64
	rng      *rand.Rand
}

func NewPaymentHandler(failRate float64) *PaymentHandler {
	return &PaymentHandler{
		captured: make(map[string]int64),
		failRate: failRate,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *PaymentHandler) Run(ctx context.Context, job core.Job) error {
	var payload PaymentPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("%w: invalid payment payload: %v", worker.ErrPermanent, err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// External payment gateway idempotency
	if _, ok := h.captured[payload.IdempotencyKey]; ok {
		return nil
	}

	if h.failRate > 0 && h.rng.Float64() < h.failRate {
		return errors.New("payment gateway: temporary timeout")
	}

	h.captured[payload.IdempotencyKey] = payload.AmountCents
	return nil
}

func (h *PaymentHandler) Captured(idemKey string) (int64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	amt, ok := h.captured[idemKey]
	return amt, ok
}

// ---------------- 4. invoice.generate ----------------

type InvoicePayload struct {
	OrderID string `json:"order_id"`
}

type InvoiceHandler struct {
	mu        sync.Mutex
	generated map[string]bool
}

func NewInvoiceHandler() *InvoiceHandler {
	return &InvoiceHandler{
		generated: make(map[string]bool),
	}
}

func (h *InvoiceHandler) Run(ctx context.Context, job core.Job) error {
	var payload InvoicePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("%w: invalid invoice payload: %v", worker.ErrPermanent, err)
	}

	// Short simulated work
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}

	h.mu.Lock()
	h.generated[payload.OrderID] = true
	h.mu.Unlock()

	return nil
}

// ---------------- 5. analytics.sync ----------------

type AnalyticsHandler struct {
	mu     sync.Mutex
	synced int
}

func NewAnalyticsHandler() *AnalyticsHandler {
	return &AnalyticsHandler{}
}

func (h *AnalyticsHandler) Run(ctx context.Context, job core.Job) error {
	h.mu.Lock()
	h.synced++
	h.mu.Unlock()
	return nil
}

func (h *AnalyticsHandler) SyncedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.synced
}

// ---------------- 6. search.index ----------------

type SearchIndexHandler struct {
	mu      sync.Mutex
	indexed int
}

func NewSearchIndexHandler() *SearchIndexHandler {
	return &SearchIndexHandler{}
}

func (h *SearchIndexHandler) Run(ctx context.Context, job core.Job) error {
	h.mu.Lock()
	h.indexed++
	h.mu.Unlock()
	return nil
}

func (h *SearchIndexHandler) IndexedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.indexed
}
