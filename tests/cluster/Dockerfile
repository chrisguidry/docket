ARG BASE_IMAGE=redis:7.4
FROM ${BASE_IMAGE}

# Create config and data directories
RUN mkdir -p /etc/redis /data

# Startup scripts - configs generated at runtime based on PORT env vars
COPY entrypoint.sh /entrypoint.sh
COPY start-cluster.sh /start-cluster.sh
RUN chmod +x /entrypoint.sh /start-cluster.sh

CMD ["/entrypoint.sh"]
