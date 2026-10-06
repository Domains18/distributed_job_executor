# syntax=docker/dockerfile:1

# ---------------- Build Stage ----------------
FROM golang:alpine AS builder
WORKDIR /app

# Copy module files and source
COPY go.mod ./
COPY . .

# Build statically-linked coordinator and worker binaries
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/coordinator ./cmd/coordinator
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/worker ./cmd/worker

# ---------------- Coordinator Runtime Stage ----------------
FROM alpine:3.20 AS coordinator
RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /bin/coordinator /usr/local/bin/coordinator

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --retries=5 \
  CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/coordinator"]
CMD ["-data-dir=/data", "-addr=:8080"]

# ---------------- Worker Runtime Stage ----------------
FROM alpine:3.20 AS worker
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /bin/worker /usr/local/bin/worker

ENTRYPOINT ["/usr/local/bin/worker"]
CMD ["-coordinator=http://coordinator:8080", "-capacity=10", "-queues=critical,email,default,bulk"]
