FROM nats:latest
COPY nats.conf /etc/nats/nats.conf
EXPOSE 4222 8222
ENTRYPOINT ["nats-server","-c","/etc/nats/nats.conf"]
