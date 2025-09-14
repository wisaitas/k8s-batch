# Multi-stage build for efficient images
FROM golang:1.24.4-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY src/ ./src/

# Build scheduler
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o scheduler src/scheduler/scheduler.go

# Build worker  
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o worker src/worker/worker.go

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/

# Copy binaries from builder
COPY --from=builder /app/scheduler /app/scheduler
COPY --from=builder /app/worker /app/worker

# Create symlinks for easier access
RUN ln -s /app/scheduler /usr/local/bin/scheduler
RUN ln -s /app/worker /usr/local/bin/worker

# Default to worker (can be overridden)
CMD ["/app/worker"]

