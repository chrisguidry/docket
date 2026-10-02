#!/bin/bash
set -e

# Ports from environment (set by entrypoint.sh)
PORT0=${CLUSTER_PORT_0:?CLUSTER_PORT_0 required}
PORT1=${CLUSTER_PORT_1:?CLUSTER_PORT_1 required}
PORT2=${CLUSTER_PORT_2:?CLUSTER_PORT_2 required}

# Create the cluster - nodes use the same ports internally and externally
# so no address translation is needed by clients
redis-cli --cluster create \
    127.0.0.1:$PORT0 \
    127.0.0.1:$PORT1 \
    127.0.0.1:$PORT2 \
    --cluster-replicas 0 --cluster-yes

# Wait for cluster to be healthy
until redis-cli -p $PORT0 cluster info | grep -q "cluster_state:ok"; do
    sleep 0.2
done

echo "Cluster is ready on ports $PORT0, $PORT1, $PORT2"

# Keep container running
tail -f /dev/null
