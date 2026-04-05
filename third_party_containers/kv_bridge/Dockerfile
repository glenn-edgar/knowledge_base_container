FROM golang:1.23-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -ldflags="-s -w" -o /kv-bridge .

FROM alpine:3.20
COPY --from=build /kv-bridge /kv-bridge

ENV MQTT_HOST=mosquitto
ENV MQTT_PORT=1883
ENV MQTT_TOPIC=kv_bridge/write
ENV MQTT_CLIENT_ID=kv-bridge
ENV NATS_URL=nats://nats:4222
ENV LOG_LEVEL=info

CMD ["/kv-bridge"]
