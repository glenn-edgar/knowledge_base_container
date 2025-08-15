# Stop/remove any old one
docker rm -f mosquitto 2>/dev/null || true

# Run fresh
docker run -d \
  --name mosquitto \
  --restart unless-stopped \
  -p 1883:1883 -p 9001:9001 \
  -v /home/gedgar/mount_startup/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro \
  eclipse-mosquitto:latest
