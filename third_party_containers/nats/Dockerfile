FROM nats:2.10-alpine
# Copy config
COPY nats.conf /etc/nats/nats.conf
# Create data dir (will be tmpfs at runtime)
RUN mkdir -p /data
EXPOSE 4222 8222 9222
CMD ["-c", "/etc/nats/nats.conf"]

