FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
WORKDIR /app/batch/worker
RUN CGO_ENABLED=0 GOOS=linux go build -o batch-worker main.go

# Publisher build
WORKDIR /app/batch/publisher
RUN CGO_ENABLED=0 GOOS=linux go build -o job-publisher main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/batch/worker/batch-worker .
COPY --from=builder /app/batch/publisher/job-publisher .

# Default เป็น worker, แต่สามารถ override ได้
CMD ["./batch-worker"]
