# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -o /bin/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -o /bin/generator ./cmd/generator

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata curl
WORKDIR /app
COPY --from=builder /bin/server /app/server
COPY --from=builder /bin/generator /app/generator
EXPOSE 8080
ENTRYPOINT ["/app/server"]
