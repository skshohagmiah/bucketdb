# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
# CGO_ENABLED=0 for static binary
RUN CGO_ENABLED=0 GOOS=linux go build -o bucketdb ./cmd/bucketdb

# Final Stage
FROM alpine:latest

WORKDIR /app

# Install basic connection tools for debugging (optional)
RUN apk add --no-cache curl

# Copy binary from builder
COPY --from=builder /app/bucketdb .

# Create directories for data
RUN mkdir -p /data/chunks /data/metadata

# Expose ports
# 8080: Cluster Coordination (ClusterKit)
# 9080: Public HTTP API
EXPOSE 8080 9080

# Environment variables with defaults
ENV BUCKETDB_ID=node-1 \
    BUCKETDB_HTTP=:8080 \
    BUCKETDB_API=:9080 \
    BUCKETDB_STORAGE=/data/chunks \
    BUCKETDB_METADATA=/data/metadata

# Entrypoint script to handle flags or just run direct
ENTRYPOINT ["./bucketdb"]
