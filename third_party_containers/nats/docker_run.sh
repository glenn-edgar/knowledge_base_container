docker run --name nats-js-ram -d -p 4222:4222 -p 8222:8222 --tmpfs /data:rw,size=50m nanodatacenter/nats-js-ram:latest

