FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go test ./...
RUN CGO_ENABLED=0 go build .
FROM alpine
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/supervisord_exporter /usr/bin/supervisord_exporter
ENTRYPOINT ["/usr/bin/supervisord_exporter"]
