# syntax=docker/dockerfile:1.6

############################
# Builder (Go 1.24.4 + cache)
############################
FROM --platform=$BUILDPLATFORM golang:1.24.4-alpine AS builder
WORKDIR /src

# เครื่องมือที่พอใช้ตอน build (ไม่ไปอยู่ใน final image)
RUN apk add --no-cache git ca-certificates tzdata

# ดึงโมดูลล่วงหน้าให้ cache แตกบ่อยน้อยลง
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# ใส่โค้ดที่เหลือ
COPY . .

# Build args สำหรับ embed เวอร์ชัน/คอมมิต/เวลา
ARG TARGETOS TARGETARCH
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

# สร้างไบนารีแบบ static (CGO=0) + ตัดสัญลักษณ์ (-s -w) + netgo resolver
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -tags "netgo,osusergo,timetzdata" \
      -ldflags "-s -w \
        -X 'main.Version=${VERSION}' \
        -X 'main.Commit=${GIT_COMMIT}' \
        -X 'main.BuildTime=${BUILD_TIME}'" \
      -o /out/app ./src/worker

############################
# Runner (Alpine บาง ๆ)
############################
FROM alpine:3.20

# ติดตั้ง CA/TZ และสร้าง user ไม่ใช่ root
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app && adduser -S -G app app

WORKDIR /app
COPY --from=builder /out/app /usr/local/bin/app

# แนะนำให้ K8s inject POD_NAME → ใช้ log/mark worker_id ได้
USER app
ENV GIN_MODE=release

ENTRYPOINT ["app"]
