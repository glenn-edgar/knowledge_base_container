docker run -d   --network bridge  --name mosquitto --restart unless-stopped -p 1883:1883 -p 9001:9001 -v /home/gedgar/mount_startup:/mosquitto/config  eclipse-mosquitto:latest

