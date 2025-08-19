docker run -d --name nats-ram -p 4222:4222 -p 8222:8222 --tmpfs /js:rw,size=256m nanodatacenter/nats-ram

