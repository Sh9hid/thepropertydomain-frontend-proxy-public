FROM caddy:2.10.2-alpine

# Render can reject execution of the stock Caddy binary when Linux file
# capabilities are preserved. Copying it to a new inode strips them.
RUN cp /usr/bin/caddy /usr/bin/caddy-render \
    && mv /usr/bin/caddy-render /usr/bin/caddy \
    && chmod +x /usr/bin/caddy

COPY Caddyfile /etc/caddy/Caddyfile
COPY dist /srv
EXPOSE 10000
