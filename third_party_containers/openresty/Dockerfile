FROM openresty/openresty:alpine-fat

# pgmoon + lua-resty-openssl for SCRAM-SHA-256 Postgres auth
RUN luarocks install pgmoon \
    && luarocks install lua-resty-openssl

# Copy config
COPY nginx.conf /usr/local/openresty/nginx/conf/nginx.conf

EXPOSE 8080
