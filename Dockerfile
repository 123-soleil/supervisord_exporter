FROM golang:alpine as builder
WORKDIR /app
ADD go.mod /app
ADD go.sum /app
RUN go mod download
ADD supervisord_exporter.go /app
RUN CGO_ENABLED=0 go build .
FROM alpine
COPY --from=builder /app/supervisord_exporter /usr/bin/supervisord_exporter
ENTRYPOINT /usr/bin/supervisord_exporter
