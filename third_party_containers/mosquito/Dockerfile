FROM eclipse-mosquitto:latest
COPY mosquitto.conf /mosquitto/config/mosquitto.conf
# Optional test page(s) if you enabled http_dir
# COPY www/ /mosquitto/www/
RUN chown -R mosquitto:mosquitto /mosquitto
EXPOSE 1883 9001
CMD ["/usr/sbin/mosquitto","-c","/mosquitto/config/mosquitto.conf"]
