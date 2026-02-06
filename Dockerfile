# syntax=docker/dockerfile:1.7

# ---- Build Stage ----
FROM golang:1.23-alpine AS builder
WORKDIR /app

# Install git for go mod download (some deps may need it)
RUN apk add --no-cache git

# Copy dependency manifests first for caching
COPY go.mod go.sum ./

# Download dependencies with cache
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY . .

# Build the binary with cache
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o /app/monitor-ingest .

# ---- Runtime Stage ----
FROM alpine:3.19 AS runner
WORKDIR /app

# Install ca-certificates for HTTPS and curl for healthcheck
RUN apk add --no-cache ca-certificates curl && rm -rf /var/cache/apk/*

# Non-root user
RUN addgroup -g 1001 -S appgroup && adduser -S appuser -u 1001 -G appgroup

# Copy binary from builder
COPY --from=builder /app/monitor-ingest /app/monitor-ingest

# Copy migrations (optional, for running schema setup)
COPY --from=builder /app/migrations /app/migrations

RUN chown -R appuser:appgroup /app
USER appuser

ENV HTTP_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["/app/monitor-ingest"]
