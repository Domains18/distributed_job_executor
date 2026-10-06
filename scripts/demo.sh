#!/usr/bin/env bash
set -euo pipefail

ADDR="${1:-http://localhost:8080}"

echo "=== Kombucha Engine Demo ==="
echo "Target coordinator: $ADDR"

echo -n "Checking coordinator health... "
if ! curl -sf "$ADDR/healthz" > /dev/null; then
    echo "FAILED"
    echo "Coordinator is not reachable at $ADDR. Start it first with:"
    echo "  docker compose up -d"
    echo "  or: go run ./cmd/coordinator"
    exit 1
fi
echo "OK"

echo
echo "1. Submitting 'order.confirmation_email'..."
EMAIL_RESP=$(curl -s -X POST "$ADDR/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"order.confirmation_email","queue":"email","payload":{"order_id":"ord-1001","email":"alice@example.com","order_total":4200},"idempotency_key":"email-ord-1001"}')
echo "Response: $EMAIL_RESP"
JOB_ID=$(echo "$EMAIL_RESP" | grep -o '"id":"[^"]*' | cut -d'"' -f4)

echo
echo "2. Submitting 'inventory.reserve'..."
INV_RESP=$(curl -s -X POST "$ADDR/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"inventory.reserve","queue":"critical","priority":10,"payload":{"order_id":"ord-1001","sku":"KOMBUCHA-GINGER","qty":2}}')
echo "Response: $INV_RESP"

echo
echo "3. Submitting 'payment.capture'..."
PAY_RESP=$(curl -s -X POST "$ADDR/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"payment.capture","queue":"critical","payload":{"order_id":"ord-1001","amount_cents":4200,"idempotency_key":"pay-ord-1001"}}')
echo "Response: $PAY_RESP"

echo
echo "Waiting 1 second for worker execution..."
sleep 1

if [ -n "$JOB_ID" ]; then
    echo
    echo "4. Checking status of email job ($JOB_ID)..."
    curl -s "$ADDR/v1/jobs/$JOB_ID" | grep -o '"state":"[^"]*'
fi

echo
echo "5. Current Queue Statistics:"
curl -s "$ADDR/v1/stats"

echo
echo
echo "6. Prometheus Metrics:"
curl -s "$ADDR/metrics"

echo
echo "=== Demo completed successfully ==="
