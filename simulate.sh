#!/bin/bash
# simulate.sh — runs forever, throws random traffic at the load balancer
# Never stops. Use Ctrl+C to end.

LB="http://localhost:8080"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   NetBalancer — Continuous Traffic Generator ║"
echo "╚══════════════════════════════════════════════╝"
echo "  Target : $LB"
echo "  Mode   : Continuous random traffic (Ctrl+C to stop)"
echo ""

# Wait for LB to be ready
echo "  Waiting for load balancer..."
until curl -sf "$LB/" > /dev/null 2>&1; do sleep 1; done
echo "  ✓ Load balancer is up. Sending traffic..."
echo ""

TOTAL=0
BATCH=0

while true; do
  # Random concurrency between 2 and 20
  C=$(( RANDOM % 18 + 2 ))

  # Random burst size between 5 and 40
  N=$(( RANDOM % 35 + 5 ))

  # Random endpoint to simulate different request types
  ENDPOINTS=("/" "/api/users" "/api/data" "/health" "/api/search" "/api/metrics")
  EP=${ENDPOINTS[$((RANDOM % ${#ENDPOINTS[@]}))]}

  # Fire N requests with C concurrency
  for i in $(seq 1 $N); do
    curl -sf "$LB$EP?id=$RANDOM" > /dev/null &
    if (( i % C == 0 )); then wait; fi
  done
  wait

  TOTAL=$((TOTAL + N))
  BATCH=$((BATCH + 1))

  # Print a status line every 10 batches
  if (( BATCH % 10 == 0 )); then
    echo "  [batch $BATCH] total sent: $TOTAL requests"
  fi

  # Random jitter between bursts: 0.1s to 1.5s
  SLEEP_MS=$(( RANDOM % 1400 + 100 ))
  sleep "0.${SLEEP_MS}"

done