# docker-host-broker — Phase 1 scaffold
#
# Multi-stage Go build. No host-side Go required; the builder image
# pulls dependencies and compiles, then a tiny alpine final image
# contains only the static binary.
#
# Final image is ~25-35 MB. Image size is intentionally small because
# the broker runs continuously on every host and the supervisor needs
# fast cold-start when respawning it.

# -------------------------------------------------------------------
# Build stage
# -------------------------------------------------------------------
FROM golang:1.25-alpine AS build

WORKDIR /src

# Cache dep download layer
COPY go.mod ./
# go.sum copied if present; harmless when absent
COPY go.su[m] ./
RUN go mod download || true

COPY cmd ./cmd
COPY internal ./internal

# Static build — no glibc dependency in final image.
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN go build -trimpath -ldflags="-s -w" -o /out/broker ./cmd/broker

# -------------------------------------------------------------------
# Final stage
# -------------------------------------------------------------------
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /out/broker /usr/local/bin/broker

# /var/run/docker.sock comes in via volume mount at runtime; do not
# bake an expectation about its location into the image.
#
# Runs as root because /var/run/docker.sock is mode 660 owned by
# root:docker on the host and the container needs read access. A
# tighter setup (run as a non-root user with the host's docker GID
# passed via --group-add at runtime) is left for a future hardening
# pass; see continue.md.

EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/broker"]
